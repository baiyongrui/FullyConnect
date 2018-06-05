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
    RESERVED_0, CONNACK, PUBLISH,
    SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    RESERVED_15, MQTTFixedHeader)
from fullyconnect.mqtt.publish import PublishPacket
from fullyconnect.mqtt.pingreq import PingReqPacket
from fullyconnect.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from fullyconnect.mqtt.connack import ConnackPacket, ConnackVariableHeader
from fullyconnect.mqtt.pingresp import PingRespPacket


logger = logging.getLogger(__name__)

STAGE_INIT = 0
STAGE_ADDR = 1
STAGE_STREAM = 2


def topic_generator():
    seq = 0
    while True:
        yield "$SYS/{}".format(seq)
        seq += 1


f_topic_generator = topic_generator()


class TCPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

        self._mqtt_client = None

    def add_to_loop(self, loop):
        self._loop = loop

        self._mqtt_client = MQTTClientProtocol(loop, self._config['mqtt_client'])
        self._loop.create_task(self._mqtt_client.create_connection())

        coro = loop.create_server(lambda: RelayServerProtocol(self._loop, self._config['server'], self._mqtt_client),
                                  '0.0.0.0', self._config['server']['port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._loop.run_until_complete(self._mqtt_client.stop())
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTClientProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
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
        self._reader = None

        self._topic_to_clients = {}

    async def create_connection(self):
        try:
            # TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self, self._config['address'], self._config['port'])
        except OSError as e:
            logger.error("{0} when connecting to mqtt server({1}:{2})".format(e, self._config['address'], self._config['port']))
            logger.error("retrying")
            await asyncio.sleep(5)     # TODO:retry interval
            self._loop.create_task(self.create_connection())

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        self._stream_reader.set_transport(transport)
        self._reader = StreamReaderAdapter(self._stream_reader)
        self._loop.create_task(self.start())

    def connection_lost(self, exc):
        # TODO: reestablish connection
        print("remote {0} connection lost.".format(self._peername))
        self.stop()

        # TODO: clean all self._topic_to_clients

        if self._stream_reader is not None:
            if exc is None:
                self._stream_reader.feed_eof()
            else:
                self._stream_reader.set_exception(exc)

        self.reestablish_connection()

    def reestablish_connection(self):
        self._stream_reader = StreamReader(loop=self._loop)
        self._encryptor = cryptor.Cryptor(self._encryptor.password, self._encryptor.method)
        self._loop.call_later(5, self._loop.create_task(self.create_connection()))

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

        # send connect packet
        # TODO: need password
        connect_vh = ConnectVariableHeader(keep_alive=self._keepalive_timeout)
        connect_payload = ConnectPayload(client_id=ConnectPayload.gen_client_id())
        connect_packet = ConnectPacket(vh=connect_vh, payload=connect_payload)
        self._send_packet(connect_packet)

        print("connect packet sent")

    @asyncio.coroutine
    def stop(self):
        self._connected = False
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
                    MQTTFixedHeader.from_stream(self._reader),
                    self._keepalive_timeout + 10, loop=self._loop)
                if fixed_header:
                    if fixed_header.packet_type == RESERVED_0 or fixed_header.packet_type == RESERVED_15:
                        logger.warning("%s Received reserved packet, which is forbidden: closing connection")
                        yield from self.handle_connection_closed()
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
                            logger.warning("%s Unhandled packet type: %s" % packet.fixed_header.packet_type)
                        if task:
                            running_tasks.append(task)
                else:
                    logger.debug("%s No more data (EOF received), stopping reader coro" )
                    break
            except MQTTException:
                logger.debug("Message discarded")
            except asyncio.CancelledError:
                # logger.debug("Task cancelled, reader loop ending")
                break
            except asyncio.TimeoutError:
                logger.debug("%s Input stream read timeout")
                break;
            except NoDataException:
                logger.debug("%s No data available")
            except BaseException as e:
                logger.warning("%s Unhandled exception in reader coro: %r" % (type(self).__name__, e))
                break
        while running_tasks:
            running_tasks.popleft().cancel()
        # yield from self.handle_connection_closed()
        self._reader_stopped.set()
        logger.debug("%s Reader coro stopped")
        yield from self.stop()

    def write(self, data: bytes, topic):
        data = self._encryptor.encrypt(data)
        packet = PublishPacket.build(topic, data, None, dup_flag=0, qos=0, retain=0)
        if not self._connected:
            self._write_pending_data.append(packet.to_bytes())
        else:
            self._send_packet(packet)

    def write_eof(self, topic):
        packet = PublishPacket.build(topic, b'', None, dup_flag=0, qos=0, retain=1)
        self._send_packet(packet)

    def _send_packet(self, packet):
        self._transport.write(packet.to_bytes())
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
    def handle_connack(self, connack: ConnackPacket):
        if connack.variable_header.return_code == 0:
            self._connected = True
            print("mqtt connected!")

            if len(self._write_pending_data) > 0:
                data = b''.join(self._write_pending_data)
                self._write_pending_data = []
                self._transport.write(data)
                self._keepalive_task.cancel()
                self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)
        else:
            print("mqtt connect fail! System will stop!")
            self._loop.create_task(self.stop())

    # @asyncio.coroutine
    def handle_publish(self, publish_packet: PublishPacket):
        data = bytes(publish_packet.data)
        try:
            server = self._topic_to_clients[publish_packet.topic_name]
        except KeyError:
            print("received unregistered topic({0}), packet will be ignored.".format(publish_packet.topic_name))
            server = None
        if not publish_packet.retain_flag:    # retain=1 indicate we should close the client connection
            data = self._encryptor.decrypt(data)
            if server is not None:
                server.write(data)
        else:
            if server is not None:
                server.close()

    @asyncio.coroutine
    def handle_pingresp(self, pingresp: PingRespPacket):
        print("PINGRESP")

    @asyncio.coroutine
    def handle_pingreq(self, pingreq: PingReqPacket):
        print("send PINGREQ")
        pingresp = PingRespPacket()
        self._send_packet(pingresp)

    def register_client_topic(self, topic, server):
        self._topic_to_clients[topic] = server

    def unregister_client_topic(self, topic):
        self._topic_to_clients.pop(topic, None)


class RelayServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config, mqtt_client: MQTTClientProtocol):
        self._loop = loop
        self._transport = None

        self._stage = STAGE_ADDR
        self._mqtt_client = mqtt_client

        self._peername = None
        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handle = None

        self._manual_close = False

        self._topic = next(f_topic_generator)
        self._mqtt_client.register_client_topic(self._topic, self)

        # test
        self._encryptor = cryptor.Cryptor(config['password'], config['method'])

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

        print("connection from: {}({})".format(self._peername, self._topic))

    def connection_lost(self, exc):
        print("client({0}) {1} connection lost.".format(self._topic, self._peername))
        if not self._manual_close:
            self._mqtt_client.write_eof(self._topic)
        self._transport = None
        self._mqtt_client.unregister_client_topic(self._topic)
        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        # self._last_activity = self._loop.time()
        #
        # elif self._stage == STAGE_ADDR:
        #     self.handle_stage_addr(data)
        data = self._encryptor.decrypt(data)
        self._mqtt_client.write(data, self._topic)

    # handle remote read
    def write(self, data):
        # TODO: test
        data = self._encryptor.encrypt(data)
        self._transport.write(data)

        self._last_activity = self._loop.time()

    def handle_stage_addr(self, data):
        header_result = common.parse_header(data)
        if header_result is None:
            logger.error("can not parse header when handling connection from {0}:{1}"
                         .format(self._peername[0], self._peername[1]))
            self.close()
            return

        addrtype, remote_addr, remote_port, header_length = header_result

        self._stage = STAGE_STREAM
        if len(data) > header_length:
            self._mqtt_client.write(data[header_length:])

    def close(self):
        self._manual_close = True
        if self._transport is not None:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logger.warning("connection from {0}:{1} timeout".format(self._peername[0], self._peername[1]))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":

    config = {"mqtt_client": {"password": "", "method": "chacha20", "timeout": 60, "address":"127.0.0.1", "port": 1883},
              "server": {"password": "", "method": "rc4-md5", "timeout": 60, "port": 1370}}

    server = TCPRelayServer(config)
    import uvloop
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    server.add_to_loop(loop)
    loop.run_forever()
