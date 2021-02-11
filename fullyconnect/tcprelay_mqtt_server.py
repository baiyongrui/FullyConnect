import collections
import asyncio
from asyncio import StreamReader, StreamWriter, ensure_future, Queue
from asyncio.streams import FlowControlMixin
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
from fullyconnect.DataChunk import DataChunk, ChunkType, ChunkProcessor

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


connection_group = ConnectionGroup()
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
        self._keepalive_task = None
        self._keepalive_timeout = config['timeout']
        self._reader_ready = None
        self._reader_stopped = asyncio.Event(loop=self._loop)
        self._stream_reader = StreamReader(loop=self._loop)
        self._stream_writer = None
        self._reader = None
        self._approved = False

        self._write_task = None
        self._queue = Queue(maxsize=1024, loop=self._loop)

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        logging.info("Mqtt client connected from: {}.".format(self._peername))

        self._stream_reader.set_transport(transport)
        self._reader = StreamReaderAdapter(self._stream_reader)
        self._stream_writer = StreamWriter(transport, self,
                                           self._stream_reader,
                                           self._loop)
        ensure_future(self.start(), loop=self._loop)

    def connection_lost(self, exc):
        super().connection_lost(exc)

        self._transport = None
        connection_group.remove_connection(self, self._peername)
        logging.info("Mqtt client connection{} lost.".format(self._peername))

        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)

        ensure_future(self.stop(), loop=self._loop)

    def data_received(self, data):
        self._stream_reader.feed_data(data)

    def eof_received(self):
        self._stream_reader.feed_eof()

    @asyncio.coroutine
    def start(self):
        self._reader_ready = asyncio.Event(loop=self._loop)
        self._reader_task = asyncio.Task(self._reader_loop(), loop=self._loop)
        yield from self._reader_ready.wait()
        if self._keepalive_timeout:
            self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)
        
        self._write_task = self._loop.create_task(self._consume_write())

    async def stop(self):
        if self._transport:
            self._transport.close()

        if self._keepalive_task:
            self._keepalive_task.cancel()
        self._write_task.cancel()
        if not self._reader_task.done():
            if not self._reader_stopped.is_set():
                self._reader_task.cancel()  # this will cause the reader_loop handle CancelledError
                await asyncio.wait(
                    [self._reader_stopped.wait()], loop=self._loop)

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
                        drop_variable_header = False
                        if self._approved and cls == PublishPacket:
                            drop_variable_header = True
                        packet = yield from cls.from_stream(self._reader, fixed_header=fixed_header, drop_variable_header=drop_variable_header)
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
        ensure_future(self.stop(), loop=self._loop)

    async def _consume_write(self):
        while self._transport is not None:
            packet = await self._queue.get()
            if self._transport is None or packet is None:
                break
            if not await self._send_packet(packet):
                break
            
    async def _send_packet(self, packet):
        try:
            await packet.to_stream(self._stream_writer)
            self._keepalive_task.cancel()
            self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

            return True
        except ConnectionResetError:
            return False

    # For relay_data, relay_disconnect
    async def write(self, chunk: DataChunk):
        if self._transport is None or self._transport.is_closing():
            return
        if chunk.type == ChunkType.DATA:
            chunk.data = self._encryptor.encrypt(chunk.data)
        data = chunk.to_bytes()
        # packet = PublishPacket.build("XCH", data, packet_id=None, dup_flag=0, qos=0, retain=0)
        packet = PublishPacket.build(None, data, packet_id=None, dup_flag=0, qos=0, retain=0)

        await self._queue.put(packet)

    def handle_write_timeout(self):
        packet = PingReqPacket()

        ensure_future(self._queue.put(packet), loop=self._loop)
        self._keepalive_task.cancel()
        self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)

    def handle_read_timeout(self):
        ensure_future(self.stop(), loop=self._loop)

    async def handle_connect(self, connect: ConnectPacket):
        return_code = 0

        # if self._peername[0] != self._auth_ip:
        #     return_code = 5
        #     logging.warning("Not authorized connection: {}!".format(self._peername))

        connack_vh = ConnackVariableHeader(return_code=return_code)
        connack = ConnackPacket(variable_header=connack_vh)
        await self._queue.put(connack)

        if return_code != 0:
            await self.stop()
        else:
            connection_group.add_connection(self, self._peername)

    async def handle_publish(self, publish_packet: PublishPacket):
        if not self._approved:
            if publish_packet.topic_name == "auth":
                password = self._encryptor.decrypt(bytes(publish_packet.data)).decode('utf-8')
                if password == self._encryptor.password:
                    self._approved = True
                    packet = PublishPacket.build("auth",
                                                 self._encryptor.encrypt(self._encryptor.password.encode('utf-8')),
                                                 None, dup_flag=0, qos=0, retain=0)
                    await self._queue.put(packet)
                else:
                    await self.stop()
            else:
                await self.stop()
            return

        chunk = DataChunk.from_bytes(publish_packet.data)
        if chunk is not None:
            target = topic_to_target.get(chunk.connection_id, None)
            if target is None:
                conn_pool = connection_group.get(self._peername)
                target = RelayTargetProtocol(self._loop, chunk.connection_id, conn_pool)
                topic_to_target[chunk.connection_id] = target
            # TODO chunk.data is not None?
            if chunk.type == ChunkType.DATA or chunk.type == ChunkType.CONNECT:
                chunk.data = self._encryptor.decrypt(chunk.data)
                # FIXME ??
                if not chunk.data:
                    return

            target.deliver(chunk)
        else:
            logging.warning("Invalid chunk, packet will be ignored.")

    async def handle_pingresp(self, pingresp: PingRespPacket):
        logging.info("Received PingRespPacket from mqtt client.")

    async def handle_pingreq(self, pingreq: PingReqPacket):
        logging.info("Received PingRepPacket from mqtt client, replying PingRespPacket.")

        packet = PingRespPacket()
        await self._queue.put(packet)


class RelayTargetProtocol(asyncio.Protocol):

    def __init__(self, loop, connection_id, connection_pool):
        self._loop = loop
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self._connection_id = connection_id
        self._connection_pool = connection_pool

        self._peername = None

        self._last_activity = 0     # for read timeout
        # TODO: from config
        self._timeout = 60
        self._timeout_handle = None

        self._chunk_processor = ChunkProcessor()

        self._idle_task = self._loop.call_later(20, self.remove)

        self._force_pause_reading = False

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        self._idle_task.cancel()
        self._idle_task = None

        if len(self._connection_pool) == 0:
            self._transport.close()
            return

        if len(self._write_pending_data) > 0:
            data = b''.join(self._write_pending_data)
            self._write_pending_data = []
            self._transport.write(data)
        self._connected = True

        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

    def connection_lost(self, exc):
        logging.info("Target connection {}{} lost.".format(self._connection_id, self._peername))
        self._transport = None

        ensure_future(self.relay_disconnect(), loop=self._loop)
        self._idle_task = self._loop.call_later(5, self.remove)

        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        if not self._connected:
            return
        self._transport.pause_reading()
        task = ensure_future(self.relay_data(data), loop=self._loop)

        def maybe_resume_reading(_):
            if self._transport is not None:
                self._transport.resume_reading()
        
        if not self._force_pause_reading:
            task.add_done_callback(maybe_resume_reading)

        self._last_activity = self._loop.time()

    async def relay_data(self, data: bytes):
        chunks = self._chunk_processor.pack_data(self._connection_id, data)
        for chunk in chunks:
            mqtt_carrier = self._connection_pool.fetch()
            if mqtt_carrier is None:
                logging.warning("No mqtt carrier available, closing relay target")
                self.close()
                return
            await mqtt_carrier.write(chunk)

    async def relay_disconnect(self):
        chunk = self._chunk_processor.pack_disconnect(self._connection_id)
        mqtt_carrier = self._connection_pool.fetch()
        if mqtt_carrier is not None:
            await mqtt_carrier.write(chunk)

    # Delivered from mqtt server
    def deliver(self, chunk: DataChunk):
        self._chunk_processor.store(chunk)
        ordered_chunks = self._chunk_processor.dump_ordered()
        for ordered_chunk in ordered_chunks:
            if ordered_chunk.type == ChunkType.DATA:
                self.write(ordered_chunk.data)
            elif ordered_chunk.type == ChunkType.CONNECT:
                data = ordered_chunk.data
                header_result = common.parse_header(data)
                if header_result is None:
                    logging.warning("Target connection {} can not parse header".format(self._connection_id))
                    return
                addrtype, remote_addr, remote_port, header_length = header_result
                logging.info("Target connection {} creating connection to {}:{}.".format(self._connection_id, common.to_str(remote_addr), remote_port))
                ensure_future(self.create_connection(common.to_str(remote_addr), remote_port), loop=self._loop)

                if len(data) > header_length:
                    self.write(data[header_length:])
            elif ordered_chunk.type == ChunkType.DISCONNECT:
                self.close(force=True)
            elif ordered_chunk.type == ChunkType.HIGH_WATER_MARK:
                self._toggle_transport_reading(pause_reading=True)
            elif ordered_chunk.type == ChunkType.LOW_WATER_MARK:
                self._toggle_transport_reading(pause_reading=False)

    def _toggle_transport_reading(self, pause_reading: bool):
        self._force_pause_reading = pause_reading
        if self._transport is not None:
            if pause_reading:
                self._transport.pause_reading()
            else:
                self._transport.resume_reading()

    def write(self, data: bytes):
        if not self._connected:
            self._write_pending_data.append(data)
        else:
            self._transport.write(data)

    def close(self, force=False):
        self._connected = False
        if self._transport:
            if force:
                self._transport.abort()
            else:
                self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logging.info("Target connection {}{} timeout".format(self._connection_id, self._peername))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)

    async def create_connection(self, host, port):
        try:
            #TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self, host, port)
        except OSError as e:
            logging.error("{} when creating target connection {} to {}:{}.".format(e, self._connection_id, host, port))
            ensure_future(self.relay_disconnect(), loop=self._loop)

    def remove(self):
        topic_to_target.pop(self._connection_id, None)


if __name__ == "__main__":
    server = TCPRelayServer({"password": "123456", "method": "aes-128-cfb", "timeout": 60, "port": 1883})
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
