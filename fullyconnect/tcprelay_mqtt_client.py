import collections
import asyncio
from asyncio import StreamReader, StreamWriter
from asyncio import ensure_future, Queue
import logging

from fullyconnect import cryptor, common
from fullyconnect.adapters import StreamReaderAdapter, FlowControlMixin
from fullyconnect.mqtt import packet_class
from fullyconnect.errors import MQTTException, NoDataException
from fullyconnect.mqtt.packet import (
    RESERVED_0, CONNACK, PUBLISH,
    SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    RESERVED_15, MQTTFixedHeader)
from fullyconnect.mqtt.publish import PublishPacket
from fullyconnect.mqtt.pingreq import PingReqPacket
from fullyconnect.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from fullyconnect.mqtt.connack import ConnackPacket, ConnackVariableHeader
from fullyconnect.mqtt.pingresp import PingRespPacket
from fullyconnect.ConnectionGroup import ConnectionGroup
from fullyconnect.DataChunk import DataChunk

import traceback

logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

STAGE_ADDR = 1
STAGE_STREAM = 2


def topic_generator():
    seq = 0
    while True:
        yield "$SYS/{}".format(seq)
        seq += 1


f_topic_generator = topic_generator()
topic_to_clients = {}


class TCPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

        # TODO remove
        self._mqtt_client = None
        self._mqtt_client_connections = ConnectionGroup()

    def add_to_loop(self, loop):
        self._loop = loop

        for client_config in self._config['mqtt_client']:
            mqtt_client = MQTTClientProtocol(loop, client_config)
            self._mqtt_client_connections.add_connection(mqtt_client)
            self._loop.create_task(mqtt_client.create_connection())

        coro = loop.create_server(lambda: RelayServerProtocol(self._loop, self._config['server'], self._mqtt_client_connections),
                                  '0.0.0.0', self._config['server']['port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._loop.run_until_complete(self._mqtt_client.stop())
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTClientProtocol(FlowControlMixin, asyncio.Protocol):

    def __init__(self, loop, config):
        super().__init__(loop=loop)
        self._loop = loop
        self._config = config

        self._transport = None
        self._write_pending_data_topic = []     # tuple (data, topic)
        self._connected = False

        self._encryptor = cryptor.Cryptor(self._config['password'], self._config['method'])

        self._peername = None

        self._reader_task = None
        self._data_task = None
        self._keepalive_task = None
        self._keepalive_timeout = self._config['timeout']
        self._reader_ready = None
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._stream_reader = StreamReader(loop=self._loop)
        self._stream_writer = None
        self._reader = None

        self._queue = Queue(loop=loop)

    async def create_connection(self):
        try:
            # TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self, self._config['address'], self._config['port'])
        except OSError as e:
            logging.error("{0} when connecting to mqtt server({1}:{2})".format(e, self._config['address'], self._config['port']))
            logging.error("Reconnection will be performed after 5s...")
            await asyncio.sleep(5)     # TODO:retry interval
            self._loop.create_task(self.create_connection())

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        self._stream_reader.set_transport(transport)
        self._reader = StreamReaderAdapter(self._stream_reader)
        self._stream_writer = StreamWriter(transport, self,
                                           self._stream_reader,
                                           self._loop)
        self._loop.create_task(self.start())

    def connection_lost(self, exc):
        logging.info("Lost connection with mqtt server{0}".format(self._peername))
        super().connection_lost(exc)

        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)

        self.stop()

        self.reestablish_connection()

    def reestablish_connection(self):
        self._stream_reader = StreamReader(loop=self._loop)
        self._encryptor = cryptor.Cryptor(self._config['password'], self._config['method'])
        self._loop.call_later(5, lambda: self._loop.create_task(self.create_connection()))

    def data_received(self, data):
        self._stream_reader.feed_data(data)

    def eof_received(self):
        self._stream_reader.feed_eof()

    @asyncio.coroutine
    def consume(self):
        while self._transport is not None:
            packet = yield from self._queue.get()
            if packet is None:
                break

            if self._transport is None:
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

        # send connect packet
        connect_vh = ConnectVariableHeader(keep_alive=self._keepalive_timeout)
        connect_payload = ConnectPayload(client_id=ConnectPayload.gen_client_id())
        connect_packet = ConnectPacket(vh=connect_vh, payload=connect_payload)
        yield from self._do_write(connect_packet)

        logging.info("Creating connection to mqtt server.")

    @asyncio.coroutine
    def stop(self):
        self._connected = False
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
                        if packet.fixed_header.packet_type == CONNACK:
                            task = ensure_future(self.handle_connack(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGREQ:
                            task = ensure_future(self.handle_pingreq(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PINGRESP:
                            task = ensure_future(self.handle_pingresp(packet), loop=self._loop)
                        elif packet.fixed_header.packet_type == PUBLISH:
                            # task = ensure_future(self.handle_publish(packet), loop=self._loop)
                            self.handle_publish(packet)
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
                logging.warning(
                    "{}:{} Unhandled exception in reader coro: {}".format(type(self).__name__, self._peername, e))
                print(traceback.format_exc())
                break
        while running_tasks:
            running_tasks.popleft().cancel()
        self._reader_stopped.set()
        logging.debug("{} Reader coro stopped".format(self._peername))
        yield from self.stop()

    def write(self, data: bytes, topic):
        if not self._connected:
            self._write_pending_data_topic.append((data, topic))
        else:
            data = self._encryptor.encrypt(data)
            packet = PublishPacket.build(topic, data, None, dup_flag=0, qos=0, retain=0)
            ensure_future(self._do_write(packet), loop=self._loop)

    def write_eof(self, topic):
        packet = PublishPacket.build(topic, b'', None, dup_flag=0, qos=0, retain=1)
        ensure_future(self._do_write(packet), loop=self._loop)

    @asyncio.coroutine
    def _do_write(self, packet):
        yield from self._queue.put(packet)

    @asyncio.coroutine
    def _send_packet(self, packet):
        try:
            yield from packet.to_stream(self._stream_writer)
        except ConnectionResetError:
            return

        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_write_timeout(self):
        packet = PingReqPacket()
        # TODO: check transport
        self._transport.write(packet.to_bytes())
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_read_timeout(self):
        self._loop.create_task(self.stop())

    @asyncio.coroutine
    def handle_connack(self, connack: ConnackPacket):
        if connack.variable_header.return_code == 0:
            packet = PublishPacket.build("$SYS/auth",
                                         self._encryptor.encrypt(self._encryptor.password.encode('utf-8')),
                                         None, dup_flag=0, qos=0, retain=0)
            yield from self._do_write(packet)
        else:
            logging.info("Unable to create connection to mqtt server! Shutting down...")
            self._loop.create_task(self.stop())

    # @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        data = bytes(publish_packet.data)
        if not self._connected:
            if publish_packet.topic_name == "$SYS/auth":
                password = self._encryptor.decrypt(data).decode('utf-8')
                if password == self._encryptor.password:
                    self._connected = True
                    logging.info("Connection to mqtt server established!")

                    if len(self._write_pending_data_topic) > 0:
                        self._keepalive_task.cancel()
                        for data, topic in self._write_pending_data_topic:
                            data = self._encryptor.encrypt(data)
                            packet = PublishPacket.build(topic, data, None, dup_flag=0, qos=0, retain=0)
                            yield from self._do_write(packet)
                        self._write_pending_data_topic = []
                        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)
                else:
                    logging.info("Connection authorization failed! Shuting down...")
                    self._loop.create_task(self.stop())
            else:
                self._loop.create_task(self.stop())
            return

        server = topic_to_clients.get(publish_packet.topic_name, None)
        if server is None:
            logging.info("Received unregistered publish topic({0}) from mqtt server, packet will be ignored.".format(
                publish_packet.topic_name))
        if not publish_packet.retain_flag:    # retain=1 indicate we should close the client connection
            data = self._encryptor.decrypt(data)
            if server is not None:
                chunk = DataChunk(publish_packet.packet_id, data)
                server.deliver(chunk)
        else:
            if server is not None:
                server.close()

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        logging.info("Received PingRespPacket from mqtt server.")

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        logging.info("Received PingReqPacket from mqtt server, Replying PingResqPacket.")
        ping_resp = PingRespPacket()
        yield from self._do_write(ping_resp)


class RelayServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config, mqtt_client_groups: ConnectionGroup):
        self._loop = loop
        self._transport = None

        self._stage = STAGE_ADDR
        self._mqtt_client = mqtt_client_groups.pick_connection()

        self._peername = None
        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handle = None

        self._manual_close = False

        self._topic = next(f_topic_generator)
        topic_to_clients[self._topic] = self

        self._encryptor = cryptor.Cryptor(config['password'], config['method'])

        self._cur_chunk_index = 0
        self._chunks = [None for i in range(1000)]

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

        logging.info("Client({}) connected from: {}.".format(self._topic, self._peername))

    def connection_lost(self, exc):
        logging.info("Client({}) connection{} lost.".format(self._topic, self._peername))
        if not self._manual_close:
            self._mqtt_client.write_eof(self._topic)
        self._transport = None
        topic_to_clients.pop(self._topic, None)
        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        # self._last_activity = self._loop.time()

        # verify packet
        data = self._encryptor.decrypt(data)
        if self._stage == STAGE_ADDR:
            header_result = common.parse_header(data)
            if header_result is None:
                logging.error("Can not parse header when handling client connection{0}".format(self._peername))
                self.close()
                return

            addrtype, remote_addr, remote_port, header_length = header_result
            self._stage = STAGE_STREAM

        self._mqtt_client.write(data, self._topic)

    # handle remote read
    def deliver(self, chunk: DataChunk):
        index = chunk.chunk_index % len(self._chunks)
        # if self._chunks[index] is not None:
        #     self.close()
        self._chunks[index] = chunk

        self.try_write()

    def try_write(self):
        buf = bytearray()
        while True:
            chunk = self._chunks[self._cur_chunk_index]
            if chunk is not None:
                buf.extend(chunk.data)
                self._chunks[self._cur_chunk_index] = None
                self._cur_chunk_index += 1
                if self._cur_chunk_index >= len(self._chunks):
                    self._cur_chunk_index = 0
            else:
                break

        if len(buf) > 0:
            data = self._encryptor.encrypt(bytes(buf))
            self._transport.write(data)

            self._last_activity = self._loop.time()

    def close(self):
        self._manual_close = True
        if self._transport is not None:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logging.info("Client({0}) connection{1} timeout.".format(self._topic, self._peername))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":

    config = {
        "mqtt_client": [
            {"password": "", "method": "aes-128-cfb", "timeout": 60, "address": "127.0.0.1", "port": 1883}
            # ,
            # {"password": "", "method": "aes-128-cfb", "timeout": 60, "address": "127.0.0.1", "port": 1883}
            # ,
            # {"password": "", "method": "aes-128-cfb", "timeout": 60, "address": "127.0.0.1", "port": 1883}
            ],
        "server": {"password": "", "method": "rc4-md5", "timeout": 60, "port": 1370}}

    server = TCPRelayServer(config)
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
