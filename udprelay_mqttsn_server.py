import struct
import asyncio
import logging

from fullyconnect import cryptor, common
from fullyconnect.mqtt import packet_class
from fullyconnect.errors import FullyConnectException, MQTTException, NoDataException
from fullyconnect.mqtt_sn.publish import PublishPacket

logging.basicConfig(level=logging.DEBUG,
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
        coro = loop.create_datagram_endpoint(lambda: MQTTServerProtocol(self._loop, self._config),
                                  local_addr=('0.0.0.0', self._config['port']))
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTServerProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._data_task = None
        self._password = common.to_bytes(config['password'])
        self._method = config['method']
        self._topic_to_remote = {}

        self._peername = None

        self._approved = False

    @property
    def loop(self):
        return self._loop

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, data, addr):
        # addr = ip, port
        packet = PublishPacket.decode(data)
        if packet is not None:
            try:
                data, key, iv = cryptor.decrypt_all(self._password,
                                                    self._method,
                                                    packet.data)
            except Exception:
                logger.debug('UDP handle_server: decrypt data failed')
                return
            if not data:
                logger.debug('UDP handle_server: data is empty after decrypt')
                return
            header_result = common.parse_header(data)
            if header_result is None:
                logger.error("can not parse header when handling connection from {}".format(addr))
                return
            addrtype, remote_addr, remote_port, header_length = header_result
            logger.info("udp data to {}:{} from {}:{}".format(remote_addr, remote_port, addr[0], addr[1]))

            remote = self._topic_to_remote.get(packet.topic_id)
            if not remote:
                remote = RelayRemoteProtocol(self, packet.topic_id, addr)
                self._topic_to_remote[packet.topic_id] = remote
                self._loop.create_task(self.create_endpoint(remote, common.to_str(remote_addr), remote_port))

            data = data[header_length:]
            remote.write(data)

    async def create_endpoint(self, remote, host, port):
        try:
            await self._loop.create_datagram_endpoint(lambda: remote, remote_addr=(host, port))
        except OSError as e:
            logger.error("error in create_enpoint: {}".format(e))

    @asyncio.coroutine
    def stop(self):
        self._data_task.cancel()
        if self._transport:
            self._transport.close()
            self._transport = None

    # for remote read
    def write(self, data, client_topic, src_addr, dst_addr):
        data = common.pack_addr(src_addr[0]) + struct.pack('>H', src_addr[1]) + data
        data = cryptor.encrypt_all(self._password, self._method, data)
        packet = PublishPacket(client_topic, client_topic, data, 0)

        self._transport.sendto(packet.to_bytes(), dst_addr)

    # @asyncio.coroutine
    # def handle_connect(self, connect: ConnectPacket):
    #     return_code = 0
    #     self._approved = True
    #
    #     password = self._encryptor.decrypt(connect.password)
    #     password = password.decode('utf-8')
    #     if password != self._encryptor.password:
    #         return_code = 4
    #         self._approved = False
    #         logging.warning("Invalid ConnectPacket password from mqtt client connection{}!".format(self._peername))
    #
    #     connack_vh = ConnackVariableHeader(return_code=return_code)
    #     connack = ConnackPacket(variable_header=connack_vh)
    #     yield from self._do_write(connack)
    #
    #     if return_code != 0:
    #         self._loop.create_task(self.stop())

    def remove_topic(self, topic):
        self._topic_to_remote.pop(topic, None)


class RelayRemoteProtocol(asyncio.DatagramProtocol):
    def __init__(self, server, topic_id, src_addr):
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self._server = server
        self._topic_id = topic_id
        self._last_activity = 0
        self._timeout = 60
        self._timeout_handle = None
        self._src_addr = src_addr

    def connection_made(self, transport):
        self._transport = transport
        self._connected = True
        self._last_activity = self._server.loop.time()
        self._timeout_handle = self._server.loop.call_later(self._timeout, self.timeout_handler)

        if len(self._write_pending_data) > 0:
            data = b''.join(self._write_pending_data)
            self._write_pending_data = []
            self._transport.sendto(data)

    def datagram_received(self, data, addr):
        self._server.write(data, self._topic_id, addr, self._src_addr)
        self._last_activity = self._server.loop.time()

    def error_received(self, exc):
        if self._transport is not None:
            self._transport.close()
        else:
            self._server.remove_topic(self._topic_id)

    def connection_lost(self, exc):
        self._server.remove_topic(self._topic_id)

    def write(self, data):
        if not self._connected:
            self._write_pending_data.append(data)
        else:
            self._transport.sendto(data)
            self._last_activity = self._server.loop.time()

    def timeout_handler(self):
        after = self._last_activity - self._server.loop.time() + self._timeout
        if after < 0:
            logger.info("udp session of topic id {} timeout".format(self._topic_id))
            self._transport.close()
        else:
            self._timeout_handle = self._server.loop.call_later(after, self.timeout_handler)


if __name__ == "__main__":
    server = TCPRelayServer({"password": "123456", "method": "aes-128-cfb", "timeout": 60, "port": 1884})
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
