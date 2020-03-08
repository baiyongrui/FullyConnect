import collections
import asyncio
from asyncio import StreamReader, StreamWriter, ensure_future
from asyncio.streams import FlowControlMixin
import logging

from fullyconnect import cryptor, common
from fullyconnect.adapters import StreamReaderAdapter
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
from fullyconnect.DataChunk import DataChunk, ChunkType, ChunkProcessor

logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def topic_generator():
    seq = 0
    while True:
        # yield "$SYS/{}".format(seq)
        yield seq
        seq += 1
        if seq >= 65535:
            seq = 0


f_topic_generator = topic_generator()
mqtt_connections = ConnectionGroup()
topic_to_clients = {}


class TCPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

    def add_to_loop(self, loop):
        self._loop = loop

        for client_config in self._config['mqtt_client']:
            mqtt_client = MQTTClientProtocol(loop, client_config)
            self._loop.create_task(mqtt_client.create_connection())

        coro = loop.create_server(lambda: RelayServerProtocol(self._loop, self._config['server']),
                                  '0.0.0.0', self._config['server']['port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        for conn in mqtt_connections:
            self._loop.run_until_complete(conn.stop())
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTClientProtocol(FlowControlMixin, asyncio.Protocol):

    def __init__(self, loop, config):
        super().__init__(loop=loop)
        self._loop = loop
        self._config = config

        self._transport = None
        self._write_pending_data = []
        self._connected = False

        self._encryptor = cryptor.Cryptor(self._config['password'], self._config['method'])

        self._peername = None

        self._reader_task = None
        self._keepalive_task = None
        self._keepalive_timeout = self._config['timeout']
        self._reader_ready = None
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._stream_reader = StreamReader(loop=self._loop)
        self._stream_writer = None
        self._reader = None

    async def create_connection(self):
        try:
            # TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self, self._config['address'], self._config['port'],
                                                                     local_addr=(self._config['source_ip'], 0))
        except OSError as e:
            logging.error("{0} when connecting to mqtt server({1}:{2})".format(e, self._config['address'], self._config['port']))
            logging.error("Reconnection will be performed after 5s...")
            await asyncio.sleep(5)     # TODO:retry interval
            self._loop.create_task(self.create_connection())

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        mqtt_connections.add_connection(self)
        self._stream_reader.set_transport(transport)
        self._reader = StreamReaderAdapter(self._stream_reader)
        self._stream_writer = StreamWriter(transport, self,
                                           self._stream_reader,
                                           self._loop)
        self._loop.create_task(self.start())

    def connection_lost(self, exc):
        mqtt_connections.remove_connection(self)
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

    async def start(self):
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._reader_task = asyncio.Task(self._reader_loop(), loop=self._loop)
        await self._reader_ready.wait()
        if self._keepalive_timeout:
            self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

        # send connect packet
        connect_vh = ConnectVariableHeader(keep_alive=self._keepalive_timeout)
        connect_payload = ConnectPayload(client_id=ConnectPayload.gen_client_id())
        connect_packet = ConnectPacket(vh=connect_vh, payload=connect_payload)
        await connect_packet.to_stream(self._stream_writer)

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
                break
        while running_tasks:
            running_tasks.popleft().cancel()
        self._reader_stopped.set()
        logging.debug("{} Reader coro stopped".format(self._peername))
        yield from self.stop()

    # For relay_data, relay_disconnect
    async def write(self, chunk: DataChunk):
        if self._transport is None or self._transport.is_closing():
            return
        if not self._connected:
            self._write_pending_data.append(chunk)
        else:
            if chunk.type == ChunkType.Data or chunk.type == ChunkType.Connect:
                chunk.data = self._encryptor.encrypt(chunk.data)
            data = chunk.to_bytes()
            packet = PublishPacket.build("XCH", data, packet_id=None, dup_flag=0, qos=0, retain=0)

            await packet.to_stream(self._stream_writer)
            self._keepalive_task.cancel()
            self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_write_timeout(self):
        packet = PingReqPacket()
        # TODO: Use async semantic
        self._transport.write(packet.to_bytes())
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_read_timeout(self):
        self._loop.create_task(self.stop())

    async def handle_connack(self, connack: ConnackPacket):
        if connack.variable_header.return_code == 0:
            packet = PublishPacket.build("auth",
                                         self._encryptor.encrypt(self._encryptor.password.encode('utf-8')),
                                         None, dup_flag=0, qos=0, retain=0)
            await packet.to_stream(self._stream_writer)
        else:
            logging.info("Unable to create connection to mqtt server! Shutting down...")
            self._loop.create_task(self.stop())

    async def handle_publish(self, publish_packet: PublishPacket):
        data = bytes(publish_packet.data)
        if not self._connected:
            if publish_packet.topic_name == "auth":
                password = self._encryptor.decrypt(data).decode('utf-8')
                if password == self._encryptor.password:
                    self._connected = True
                    logging.info("Connection to mqtt server established!")

                    # TODO polish here
                    if len(self._write_pending_data) > 0:
                        self._keepalive_task.cancel()
                        for chunk in self._write_pending_data:
                            await self.write(chunk)
                        self._write_pending_data = []
                        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)
                else:
                    logging.info("Connection authorization failed! Shuting down...")
                    self._loop.create_task(self.stop())
            else:
                self._loop.create_task(self.stop())
            return

        chunk = DataChunk.from_bytes(publish_packet.data)
        if chunk is not None:
            if chunk.type == ChunkType.Data:
                chunk.data = self._encryptor.decrypt(chunk.data)
            client = topic_to_clients.get(chunk.connection_id, None)
            if client is not None:
                client.deliver(chunk)
            else:
                logging.info(
                    "Received unregistered publish topic({0}) from mqtt server, packet will be ignored.".format(
                        chunk.connection_id))
        else:
            logging.warning("Invalid chunk, packet will be ignored.")

    async def handle_pingresp(self, pingresp: PingRespPacket):
        logging.info("Received PingRespPacket from mqtt server.")

    async def handle_pingreq(self, pingreq: PingReqPacket):
        logging.info("Received PingReqPacket from mqtt server, Replying PingResqPacket.")
        ping_resp = PingRespPacket()
        await ping_resp.to_stream(self._stream_writer)


class RelayServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None

        self._address_parsed = False

        self._peername = None
        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handle = None

        self._manual_close = False

        self._connection_id = next(f_topic_generator)   # Topic as connection id
        topic_to_clients[self._connection_id] = self

        self._encryptor = cryptor.Cryptor(config['password'], config['method'])

        self._chunk_processor = ChunkProcessor()

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

        if len(mqtt_connections) == 0:
            logging.warning("No mqtt carrier available, closing relay server")
            self.close()
            return

        logging.info("Client({}) connected from: {}.".format(self._connection_id, self._peername))

    def connection_lost(self, exc):
        logging.info("Client({}) connection{} lost.".format(self._connection_id, self._peername))
        if not self._manual_close:
            # Tell the mqtt server close relay target connection ASAP
            ensure_future(self.relay_disconnect(), loop=self._loop)
        self._transport = None
        topic_to_clients.pop(self._connection_id, None)
        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        # self._last_activity = self._loop.time()

        # verify packet
        data = self._encryptor.decrypt(data)
        # FIXME ???
        if not data:
            return
        if not self._address_parsed:    # Make connect
            header_result = common.parse_header(data)
            if header_result is None:
                logging.error("Can not parse header when handling client connection{0}".format(self._peername))
                self.close()
                return

            addrtype, remote_addr, remote_port, _ = header_result
            ensure_future(self.relay_connect(data), loop=self._loop)
            self._address_parsed = True
        else:
            ensure_future(self.relay_data(data), loop=self._loop)

    async def relay_data(self, data: bytes):
        chunks = self._chunk_processor.pack_data(self._connection_id, data)
        for chunk in chunks:
            mqtt_carrier = mqtt_connections.pick_connection()
            if mqtt_carrier is None:
                logging.warning("No mqtt carrier available, closing relay server")
                self.close()
                return
            await mqtt_carrier.write(chunk)

    async def relay_connect(self, data: bytes):
        chunk = self._chunk_processor.pack_connect(self._connection_id, data)
        mqtt_carrier = mqtt_connections.pick_connection()
        if mqtt_carrier is not None:
            await mqtt_carrier.write(chunk)

    async def relay_disconnect(self):
        chunk = self._chunk_processor.pack_disconnect(self._connection_id)
        mqtt_carrier = mqtt_connections.pick_connection()
        if mqtt_carrier is not None:
            await mqtt_carrier.write(chunk)

    def write(self, data: bytes):
        data = self._encryptor.encrypt(data)
        self._transport.write(data)
        self._last_activity = self._loop.time()

    # Delivered from mqtt client
    def deliver(self, chunk: DataChunk):
        self._chunk_processor.store(chunk)
        ordered_chunks = self._chunk_processor.dump_ordered()
        for ordered_chunk in ordered_chunks:
            if ordered_chunk.type == ChunkType.Data:
                self.write(ordered_chunk.data)
            elif ordered_chunk.type == ChunkType.Disconnect:
                self.close()

    def close(self):
        self._manual_close = True
        if self._transport is not None:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logging.info("Client({0}) connection{1} timeout.".format(self._connection_id, self._peername))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":

    config = {
        "mqtt_client": [
            {"password": "123456", "method": "aes-128-cfb", "timeout": 60, "address": "10.0.0.236", "port": 1883,
             "source_ip": "10.0.0.236"}
            ,
        {"password": "123456", "method": "aes-128-cfb", "timeout": 60, "address": "10.0.0.236", "port": 1883,
             "source_ip": "10.0.0.236"}
            ],
        "server": {"password": "123456", "method": "rc4-md5", "timeout": 60, "port": 8700}}

    server = TCPRelayServer(config)
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
