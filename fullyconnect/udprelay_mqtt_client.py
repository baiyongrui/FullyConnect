import asyncio
import logging

from fullyconnect import cryptor, common
from fullyconnect.mqtt import packet_class
from fullyconnect.errors import FullyConnectException, MQTTException, NoDataException
from fullyconnect.mqtt_sn.publish import PublishPacket
from fullyconnect.udpsession_lru import UDPSessionLRU

logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def topic_generator():
    seq = 0
    while True:
        yield seq
        seq += 1


class UDPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

        self._mqtt_client = None

    def add_to_loop(self, loop):
        self._loop = loop

        self._mqtt_client = MQTTClientProtocol(loop, self._config['mqtt_client'])
        self._loop.create_task(self._mqtt_client.create_endpoint(self._config['mqtt_client']['address'], self._config['mqtt_client']['port']))

        coro = loop.create_datagram_endpoint(lambda: RelayServerProtocol(self._loop, self._config['server'], self._mqtt_client),
                                      local_addr=('0.0.0.0', self._config['server']['port']))
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._loop.run_until_complete(self._mqtt_client.stop())
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class MQTTClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._data_task = None
        self._write_pending_data_topic = []     # tuple (data, topic)
        self._connected = False

        self._password = common.to_bytes(config['password'])
        self._method = config['method']

        self._udpsession_lru = UDPSessionLRU(128, topic_generator())

        self._server = None

        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handler = None

    def connection_made(self, transport):
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handler = self._loop.call_later(self._timeout, self.timeout_handler)

    def datagram_received(self, data, addr):
        packet = PublishPacket.decode(data)
        if packet is not None:
            try:
                data, key, iv = cryptor.decrypt_all(self._password,
                                                    self._method,
                                                    packet.data)
            except Exception:
                logger.debug('UDP handle_server: decrypt data failed')
                return
            header_result = common.parse_header(data)
            if header_result is None:
                logger.error("can not parse header when handling publish packet  from server: {}".format(addr))
                return
            addr = self._udpsession_lru.topic_to_addr(packet.topic_id)
            if addr is not None and self._server is not None:
                self._server.write(data, addr)
                self._last_activity = self._loop.time()

    async def create_endpoint(self, host, port):
        try:
            await self._loop.create_datagram_endpoint(lambda: self, remote_addr=(host, port))
        except OSError as e:
            logger.error("{}".format(e))

    @asyncio.coroutine
    def stop(self):
        self._connected = False
        self._data_task.cancel()
        if self._transport:
            self._transport.close()
            self._transport = None
        self._server = None

    def write(self, data: bytes, addr):
        topic = self._udpsession_lru.addr_to_topic(addr)
        data = cryptor.encrypt_all(self._password, self._method, data)
        packet = PublishPacket(topic, topic, data, 0)

        self._transport.sendto(packet.to_bytes())
        self._last_activity = self._loop.time()

    # @asyncio.coroutine
    # def handle_connack(self, connack: ConnackPacket):
    #     if connack.variable_header.return_code == 0:
    #         self._connected = True
    #         logging.info("Connection to mqtt server established!")
    #
    #         if len(self._write_pending_data_topic) > 0:
    #             self._keepalive_task.cancel()
    #             for data, topic in self._write_pending_data_topic:
    #                 data = self._encryptor.encrypt(data)
    #                 packet = PublishPacket.build(topic, data, None, dup_flag=0, qos=0, retain=0)
    #                 yield from self._do_write(packet)
    #             self._write_pending_data_topic = []
    #             self._keepalive_task = self._loop.call_later(self._keepalive_timeout, self.handle_write_timeout)
    #     else:
    #         logging.info("Unable to create connection to mqtt server! Shuting down...")
    #         self._loop.create_task(self.stop())

    def timeout_handler(self):
        # after = self._last_activity - self._loop.time() + self._timeout
        if self._loop.time() - self._last_activity >= self._timeout:
            logging.info("clear all expired udp sessions.")
            self._udpsession_lru.clear()
        self._timeout_handler = self._loop.call_later(self._timeout, self.timeout_handler)

    def regsiter_server(self, server):
        self._server = server


class RelayServerProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, config, mqtt_client: MQTTClientProtocol):
        self._loop = loop
        self._transport = None

        self._mqtt_client = mqtt_client
        mqtt_client.regsiter_server(self)

        self._password = common.to_bytes(config['password'])
        self._method = config['method']

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, data, addr):
        # addr = ip, port
        try:
            data, key, iv = cryptor.decrypt_all(self._password, self._method, data)
        except Exception:
            logger.debug('UDP handle_server: decrypt data failed')
            return

        header_result = common.parse_header(data)
        if header_result is None:
            logger.error("can not parse header when handling connection from {}".format(addr))
            return
        addrtype, remote_addr, remote_port, header_length = header_result
        self._mqtt_client.write(data, addr)

    # handle remote read
    def write(self, data, addr):
        data = cryptor.encrypt_all(self._password, self._method, data)
        self._transport.sendto(data, addr)


if __name__ == "__main__":

    config = {"mqtt_client": {"password": "", "method": "aes-128-cfb", "timeout": 300, "address": "127.0.0.1", "port": 1883},
              "server": {"password": "", "method": "rc4-md5", "timeout": 60, "port": 1370}}

    server = UDPRelayServer(config)
    import uvloop
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    server.add_to_loop(loop)
    loop.run_forever()
