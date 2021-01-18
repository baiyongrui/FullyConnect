import collections
import struct
import asyncio
from asyncio import StreamReader, StreamWriter
from asyncio.streams import FlowControlMixin
from asyncio import ensure_future, Queue
import logging

from fullyconnect import cryptor, common
from fullyconnect.adapters import StreamReaderAdapter
from fullyconnect.mqtt import packet_class
from fullyconnect.errors import MQTTException, NoDataException
from fullyconnect.mqtt.packet import (
    RESERVED_0, CONNECT, PUBLISH,
    SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    RESERVED_15, MQTTFixedHeader)
from fullyconnect.mqtt.publish import PublishPacket
from fullyconnect.mqtt.pingreq import PingReqPacket
from fullyconnect.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from fullyconnect.mqtt.connack import ConnackPacket, ConnackVariableHeader
from fullyconnect.mqtt.pingresp import PingRespPacket
from fullyconnect.ConnectionGroup import ConnectionGroup

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


connections = ConnectionGroup()
topic_to_target = {}


class TCPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

    def add_to_loop(self, loop):
        self._loop = loop
        coro = loop.create_server(lambda: MQTTServerProtocol(self._loop, self._config),
                                  '0.0.0.0', self._config['port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTServerProtocol(FlowControlMixin, asyncio.Protocol):

    def __init__(self, loop, config):
        super().__init__(loop=loop)
        self._loop = loop
        self._transport = None
        self._encryptor = cryptor.Cryptor(config['password'], config['method'])
        # self._auth_ip = config['auth_ip']

        self._peername = None

        self._reader_task = None
        self._data_task = None
        self._keepalive_task = None
        self._keepalive_timeout = config['timeout']
        self._reader_ready = None
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._stream_reader = StreamReader(loop=self._loop)
        self._stream_writer = None
        self._reader = None
        self._approved = False

        self._queue = Queue(loop=loop)

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        logging.info("Mqtt client connected from: {}.".format(self._peername))

        self._stream_reader.set_transport(transport)
        self._reader = StreamReaderAdapter(self._stream_reader)
        self._stream_writer = StreamWriter(transport, self,
                                           self._stream_reader,
                                           self._loop)
        self._loop.create_task(self.start())

    def connection_lost(self, exc):
        connections.remove_connection(self, self._peername)
        logging.info("Mqtt client connection{} lost.".format(self._peername))
        super().connection_lost(exc)

        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)

        self.stop()

    def data_received(self, data):
        self._stream_reader.feed_data(data)

    def eof_received(self):
        self._stream_reader.feed_eof()

    @asyncio.coroutine
    def consume(self):
        while self._transport is not None:
            packet = yield from self._queue.get()
            if self._transport is None or packet is None:
                break
            yield from self._send_packet(packet)

    @asyncio.coroutine
    def start(self):
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._reader_task = asyncio.Task(self._reader_loop(), loop=self._loop)
        yield from self._reader_ready.wait()
        if self._keepalive_timeout:
            self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

        self._data_task = self._loop.create_task(self.consume())

    @asyncio.coroutine
    def stop(self):
        if self._keepalive_task:
            self._keepalive_task.cancel()
        self._data_task.cancel()
        logger.debug("waiting for tasks to be stopped")
        if not self._reader_task.done():
            if not self._reader_stopped.is_set():
                self._reader_task.cancel()  # this will cause the reader_loop handle CancelledError
                # yield from asyncio.wait(
                #     [self._reader_stopped.wait()], loop=self._loop)
            else:   # caused by reader_loop break statement
                if self._transport:
                    self._transport.close()
                    self._transport = None

                    # TODO close remote in topic_to_target
                    # for topic, remote in self._topic_to_target.items():
                    #     remote.close()

    @asyncio.coroutine
    def _reader_loop(self):
        running_tasks = collections.deque()
        while True:
            try:
                self._reader_ready.set()
                while running_tasks and running_tasks[0].done():
                    running_tasks.popleft()
                if len(running_tasks) > 1:
                    logging.debug("{} Handler running tasks: {}".format(self._peername, len(running_tasks)))

                fixed_header = yield from asyncio.wait_for(
                    MQTTFixedHeader.from_stream(self._reader),
                    self._keepalive_timeout + 10, loop=self._loop)
                if fixed_header:
                    if fixed_header.packet_type == RESERVED_0 or fixed_header.packet_type == RESERVED_15:
                        logging.warning("{} Received reserved packet, which is forbidden: closing connection".format(self._peername))
                        break
                    else:
                        cls = packet_class(fixed_header)
                        packet = yield from cls.from_stream(self._reader, fixed_header=fixed_header)
                        task = None
                        if packet.fixed_header.packet_type == CONNECT:
                            task = ensure_future(self.handle_connect(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            task = ensure_future(self.handle_pingreq(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            task = ensure_future(self.handle_pingresp(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            task = ensure_future(self.handle_publish(packet), loop=self._loop)
                            # self.handle_publish(packet)
                        # elif packet.fixed_header.packet_type == SUBSCRIBE:
                        #     task = ensure_future(self.handle_subscribe(packet), loop=self._loop)
                        # elif packet.fixed_header.packet_type == UNSUBSCRIBE:
                        #     task = ensure_future(self.handle_unsubscribe(packet), loop=self._loop)
                        # elif packet.fixed_header.packet_type == SUBACK:
                        #     task = ensure_future(self.handle_suback(packet), loop=self._loop)
                        # elif packet.fixed_header.packet_type == UNSUBACK:
                        #     task = ensure_future(self.handle_unsuback(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == DISCONNECT:
                            task = ensure_future(self.handle_disconnect(packet), loop=self._loop)
                        else:
                            # TODO: handle unknow packet type
                            logging.warning("{} Unhandled packet type: {}".format(self._peername, packet.fixed_header.packet_type))
                        if task:
                            running_tasks.append(task)
                else:
                    logging.debug("{} No more data (EOF received), stopping reader coro".format(self._peername))
                    break
            except MQTTException:
                logging.debug("{} Message discarded".format(self._peername))
            except asyncio.CancelledError:
                # logger.debug("Task cancelled, reader loop ending")
                break
            except asyncio.TimeoutError:
                logging.debug("{} Input stream read timeout".format(self._peername))
                break
            except NoDataException:
                logging.debug("{} No data available".format(self._peername))
            except BaseException as e:
                logging.warning("{}:{} Unhandled exception in reader coro: {}".format(type(self).__name__, self._peername, e))
                break
        while running_tasks:
            running_tasks.popleft().cancel()
        self._reader_stopped.set()
        logging.debug("{} Reader coro stopped".format(self._peername))
        yield from self.stop()

    # for target read
    def write(self, data: bytes, client_topic: str, src_addr):
        if self._transport is None or self._transport.is_closing():
            return
        data = common.pack_addr(src_addr[0]) + struct.pack('>H', src_addr[1]) + data
        data = self._encryptor.encrypt(data)
        packet = PublishPacket.build(client_topic, data, None, dup_flag=0, qos=0, retain=0)
        ensure_future(self._do_write(packet), loop=self._loop)

    @asyncio.coroutine
    def _do_write(self, packet):
        yield from self._queue.put(packet)

    @asyncio.coroutine
    def _send_packet(self, packet):
        yield from packet.to_stream(self._stream_writer)
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_write_timeout(self):
        packet = PingReqPacket()
        self._transport.write(packet.to_bytes())
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_read_timeout(self):
        self._loop.create_task(self.stop())

    @asyncio.coroutine
    def handle_connect(self, connect: ConnectPacket):
        return_code = 0

        # if self._peername[0] != self._auth_ip:
        #     return_code = 5
        #     logging.warning("Not authorized connection: {}!".format(self._peername))

        connack_vh = ConnackVariableHeader(return_code=return_code)
        connack = ConnackPacket(variable_header=connack_vh)
        yield from self._do_write(connack)

        if return_code != 0:
            self._loop.create_task(self.stop())

    @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        if not self._approved:
            if publish_packet.topic_name == "auth":
                password = self._encryptor.decrypt(bytes(publish_packet.data)).decode('utf-8')
                if password == self._encryptor.password:
                    self._approved = True
                    packet = PublishPacket.build("auth",
                                                 self._encryptor.encrypt(self._encryptor.password.encode('utf-8')),
                                                 None, dup_flag=0, qos=0, retain=0)
                    yield from self._do_write(packet)
                    connections.add_connection(self, self._peername)
                else:
                    self._loop.create_task(self.stop())
            else:
                self._loop.create_task(self.stop())
            return

        data = self._encryptor.decrypt(bytes(publish_packet.data))
        if not data:
            logger.debug('UDP handle_server: data is empty after decrypt')
            return
        header_result = common.parse_header(data)
        if header_result is None:
            logger.error("can not parse header when handling connection from {}".format(self._peername))
            return
        addrtype, remote_addr, remote_port, header_length = header_result
        logger.info("udp data to {}:{} from {}".format(remote_addr, remote_port, self._peername))

        target = topic_to_target.get(publish_packet.topic_name)
        if not target:
            conn_pool = connections.get(self._peername)
            target = RelayTargetProtocol(self._loop, publish_packet.topic_name, conn_pool)
            topic_to_target[publish_packet.topic_name] = target
            self._loop.create_task(self.create_endpoint(target, common.to_str(remote_addr), remote_port))

        data = data[header_length:]
        target.write(data)

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        logging.info("Received PingRespPacket from mqtt client.")

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        logging.info("Received PingRepPacket from mqtt client, replying PingRespPacket.")
        ping_resp = PingRespPacket()
        yield from self._do_write(ping_resp)

    async def create_endpoint(self, remote, host, port):
        try:
            await self._loop.create_datagram_endpoint(lambda: remote, remote_addr=(host, port))
        except OSError as e:
            logger.error("error in create_enpoint: {}".format(e))


class RelayTargetProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, client_topic, conn_pool):
        self._loop = loop
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self.client_topic = client_topic
        self._conn_pool = conn_pool
        self._last_activity = 0     # for read timeout
        # TODO: from config
        self._timeout = 60
        self._timeout_handle = None

    def connection_made(self, transport):
        self._transport = transport
        self._connected = True

        if len(connections) == 0:
            self._transport.close()
            return

        if len(self._write_pending_data) > 0:
            data = b''.join(self._write_pending_data)
            self._write_pending_data = []
            self._transport.sendto(data)

        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

    def connection_lost(self, exc):
        self._transport = None
        topic_to_target.pop(self.client_topic, None)

        self._timeout_handle.cancel()
        self._timeout_handle = None

    def datagram_received(self, data, addr):
        conn = self._conn_pool.fetch()
        if conn is None:
            logging.warning("No available client connections, closing relay target")
            self.close()
            return
        conn.write(data, self.client_topic, addr)
        self._last_activity = self._loop.time()

    def error_received(self, exc):
        self.close()

    def write(self, data):
        if not self._connected:
            self._write_pending_data.append(data)
        else:
            self._transport.sendto(data)
            self._last_activity = self._loop.time()

    def close(self):
        self._connected = False
        if self._transport is not None:
            self._transport.close()
        else:
            topic_to_target.pop(self.client_topic, None)

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logger.info("udp session of topic id {} timeout".format(self.client_topic))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":
    server = TCPRelayServer({"password": "123456", "method": "aes-128-cfb", "timeout": 60, "port": 1884})
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
