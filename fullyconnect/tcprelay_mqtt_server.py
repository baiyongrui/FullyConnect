import collections
import asyncio
from asyncio import StreamReader
from asyncio import ensure_future
import logging

from fullyconnect import cryptor, common
from fullyconnect.adapters import StreamReaderAdapter
from fullyconnect.mqtt import packet_class
from fullyconnect.errors import fullyconnectException, MQTTException, NoDataException
from fullyconnect.mqtt.packet import (
    RESERVED_0, CONNECT, PUBLISH,
    SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    RESERVED_15, MQTTFixedHeader)
from fullyconnect.mqtt.publish import PublishPacket
from fullyconnect.mqtt.pingreq import PingReqPacket
from fullyconnect.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from fullyconnect.mqtt.connack import ConnackPacket, ConnackVariableHeader
from fullyconnect.mqtt.pingresp import PingRespPacket

logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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


class MQTTServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._encryptor = cryptor.Cryptor(config['password'], config['method'])
        self._topic_to_remote = {}

        self._peername = None

        self._reader_task = None
        self._keepalive_task = None
        self.keepalive_timeout = config['timeout']
        self._reader_ready = None
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._stream_reader = StreamReader(loop=self._loop)
        self.reader = None

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        self._stream_reader.set_transport(transport)
        self.reader = StreamReaderAdapter(self._stream_reader)
        self._loop.create_task(self.start())

    def connection_lost(self, exc):
        # TODO: reestablish connection
        print("client {0} connection lost.".format(self._peername))
        self.stop()
        # TODO: close all remotes

        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)

    def data_received(self, data):
        self._stream_reader.feed_data(data)

    def eof_received(self):
        self._stream_reader.feed_eof()

    @asyncio.coroutine
    def start(self):
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._reader_task = asyncio.Task(self._reader_loop(), loop=self._loop)
        yield from self._reader_ready.wait()
        if self.keepalive_timeout:
            self._keepalive_task = self._loop.call_later(self.keepalive_timeout, self.handle_write_timeout)

    @asyncio.coroutine
    def stop(self):
        if self._keepalive_task:
            self._keepalive_task.cancel()
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
                print("MQTTServer closed for peer:{0}".format(self._peername))

    @asyncio.coroutine
    def _reader_loop(self):
        running_tasks = collections.deque()
        while True:
            try:
                self._reader_ready.set()
                while running_tasks and running_tasks[0].done():
                    running_tasks.popleft()
                if len(running_tasks) > 1:
                    logger.debug("handler running tasks: %d" % len(running_tasks))

                fixed_header = yield from asyncio.wait_for(
                    MQTTFixedHeader.from_stream(self.reader),
                    self.keepalive_timeout+10, loop=self._loop)
                if fixed_header:
                    if fixed_header.packet_type == RESERVED_0 or fixed_header.packet_type == RESERVED_15:
                        logger.warning("%s Received reserved packet, which is forbidden: closing connection")
                        yield from self.handle_connection_closed()
                    else:
                        cls = packet_class(fixed_header)
                        packet = yield from cls.from_stream(self.reader, fixed_header=fixed_header)
                        task = None
                        if packet.fixed_header.packet_type == CONNECT:
                            task = ensure_future(self.handle_connect(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            task = ensure_future(self.handle_pingreq(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            task = ensure_future(self.handle_pingresp(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            task = ensure_future(self.handle_publish(packet), loop=self._loop)
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
                            logger.warning("%s Unhandled packet type: %s" %
                                                (self._peername, packet.fixed_header.packet_type))
                        if task:
                            running_tasks.append(task)
                else:
                    logger.debug("%s No more data (EOF received), stopping reader coro" % self._peername)
                    break
            except MQTTException:
                logger.debug("Message discarded")
            except asyncio.CancelledError:
                # logger.debug("Task cancelled, reader loop ending")
                break
            except asyncio.TimeoutError:
                logger.debug("%s Input stream read timeout", self._peername)
                break;
            except NoDataException:
                logger.debug("%s No data available" % self._peername)
            except BaseException as e:
                logger.warning("%s Unhandled exception in reader coro: %r" % (type(self).__name__, e))
                break
        while running_tasks:
            running_tasks.popleft().cancel()
        # yield from self.handle_connection_closed()
        self._reader_stopped.set()
        logger.debug("%s Reader coro stopped" % self._peername)
        yield from self.stop()

    # for remote read
    def write(self, data, client_topic):
        data = self._encryptor.encrypt(data)
        packet = PublishPacket.build(client_topic, data, None, dup_flag=0, qos=0, retain=0)
        self._send_packet(packet)

    def _write_eof(self, client_topic):
        packet = PublishPacket.build(client_topic, b'', None, dup_flag=0, qos=0, retain=1)
        self._send_packet(packet)
        
    def _send_packet(self, packet):
        self._transport.write(packet.to_bytes())
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self.keepalive_timeout, self.handle_write_timeout)

    def handle_write_timeout(self):
        packet = PingReqPacket()
        self._transport.write(packet.to_bytes())
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self.keepalive_timeout, self.handle_write_timeout)

    def handle_read_timeout(self):
        self._loop.create_task(self.stop())

    @asyncio.coroutine
    def handle_connect(self, connect: ConnectPacket):
        # TODO: check password and save
        # if connect.password == "":
        connack_vh = ConnackVariableHeader(return_code=0)
        connack = ConnackPacket(variable_header=connack_vh)
        self._send_packet(connack)
        # else:
        #     self._loop.create_task(self.stop())

    @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        data = bytes(publish_packet.data)
        remote = self._topic_to_remote.get(publish_packet.topic_name, None)
        if not publish_packet.retain_flag:
            data = self._encryptor.decrypt(data)
            if remote is None:    # we are in STAGE_ADDR
                if not data:
                    self._write_eof(publish_packet.topic_name)
                    return

                header_result = common.parse_header(data)
                if header_result is None:
                    logger.error("can not parse header when handling connection from {0}:{1}"
                                 .format(self._peername[0], self._peername[1]))
                    self._write_eof(publish_packet.topic_name)
                    return

                addrtype, remote_addr, remote_port, header_length = header_result
                logger.info('connecting to %s:%d from %s:%d' %
                            (common.to_str(remote_addr), remote_port,
                             self._peername[0], self._peername[1]))

                remote = RelayRemoteProtocol(self._loop, self, publish_packet.topic_name)
                self._topic_to_remote[publish_packet.topic_name] = remote
                self._loop.create_task(self.create_connection(remote, common.to_str(remote_addr), remote_port))

                if len(data) > header_length:
                    remote.write(data[header_length:])
            else:   # now in STAGE_STREAM
                remote.write(data)
        else:
            if remote is not None:
                print("closing remote.")
                remote.close()

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        print("PINGRESP")

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        print("send PINGREQ")
        pingresp = PingRespPacket()
        self._send_packet(pingresp)

    async def create_connection(self, remote, host, port):
        try:
            #TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: remote, host, port)
        except OSError as e:
            logger.error("{0} when connecting to {1}:{2} from {3}:{4}".format(e, host, port, self._peername[0], self._peername[1]))
            self.remove_topic(remote.client_topic)

    def remove_topic(self, topic):
        self._write_eof(topic)
        self._topic_to_remote.pop(topic, None)


class RelayRemoteProtocol(asyncio.Protocol):

    def __init__(self, loop, server: MQTTServerProtocol, client_topic):
        self._loop = loop
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self._server = server
        self.client_topic = client_topic

        self._peername = None

        # for read timeout
        self._last_activity = 0
        # TODO: from config
        self._timeout = 60
        self._timeout_handle = None

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._connected = True

        if self._server is None:
            self._transport.close()
            return

        if len(self._write_pending_data) > 0:
            data = b''.join(self._write_pending_data)
            self._write_pending_data = []
            self._transport.write(data)

        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

    def connection_lost(self, exc):
        print("remote {0} connection lost.".format(self._peername))
        self._transport = None
        if self._server is not None:
            self._server.remove_topic(self.client_topic)
            self._server = None

        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        self._server.write(data, self.client_topic)

        self._last_activity = self._loop.time()

    def write(self, data):
        if not self._connected:
            self._write_pending_data.append(data)
        else:
            # if len(self._write_pending_data) > 0:
            #     self._write_pending_data.append(data)
            #     data = b''.join(self._write_pending_data)
            #     self._write_pending_data = []
            self._transport.write(data)

    def close(self):    # closed by local
        self._server = None
        if self._transport:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logger.warning("connection from {0}:{1} timeout".format(self._peername[0], self._peername[1]))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":
    server = TCPRelayServer({"password": "", "method": "chacha20", "timeout": 60, "port": 1883})
    import uvloop
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    server.add_to_loop(loop)
    loop.run_forever()
