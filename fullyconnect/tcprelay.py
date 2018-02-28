import asyncio
import logging

from fullyconnect import cryptor, common

logger = logging.getLogger(__name__)

STAGE_INIT = 0
STAGE_ADDR = 1
STAGE_STREAM = 2


class TCPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

    def add_to_loop(self, loop):
        self._loop = loop
        coro = loop.create_server(lambda: RelayServerProtocol(self._loop, self._config),
                                  self._config['server'], self._config['server_port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class RelayServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._encryptor = cryptor.Cryptor(config['password'], config['method'])
        self._stage = STAGE_ADDR
        self._remote = None

        self._peername = None
        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handle = None

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

    def connection_lost(self, exc):
        print("client {0} connection lost.".format(self._peername))
        self._transport = None
        if self._stage == STAGE_STREAM:
            self._remote.close()
            self._remote = None
        self._timeout_handle.cancel()
        self._timeout_handle = None

    def data_received(self, data):
        data = self._encryptor.decrypt(data)
        if not data:
            self.close()
            return

        self._last_activity = self._loop.time()

        if self._stage == STAGE_STREAM:
            self.handle_stage_stream(data)
        elif self._stage == STAGE_ADDR:
            self.handle_stage_addr(data)

    # handle remote read
    def write(self, data):
        data = self._encryptor.encrypt(data)
        self._transport.write(data)

        self._last_activity = self._loop.time()

    def handle_stage_addr(self, data):
        header_result = common.parse_header(data)
        if header_result is None:
            logger.error("can not parse header when handling connection from {0}:{1}"
                          .format(self._peername[0], self._peername[1]))
            self._transport.close()
            return

        addrtype, remote_addr, remote_port, header_length = header_result
        logger.info('connecting to %s:%d from %s:%d' %
                    (common.to_str(remote_addr), remote_port,
                    self._peername[0], self._peername[1]))

        self._remote = RelayRemoteProtocol(self)
        self._loop.create_task(self.create_connection(common.to_str(remote_addr), remote_port))

        self._stage = STAGE_STREAM
        if len(data) > header_length:
            self._remote.write(data[header_length:])

    async def create_connection(self, host, port):
        try:
            #TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self._remote, host, port)
        except OSError as e:
            logger.error("{0} when connecting to {1}:{2} from {3}:{4}".format(e, host, port, self._peername[0], self._peername[1]))
            self.close()

    def handle_stage_stream(self, data):
        self._remote.write(data)

    def close(self):
        if self._transport is not None:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logger.warning("connection from {0}:{1} timeout".format(self._peername[0], self._peername[1]))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


class RelayRemoteProtocol(asyncio.Protocol):

    def __init__(self, server):
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self._server = server

        self._peername = None

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

    def connection_lost(self, exc):
        print("remote {0} connection lost.".format(self._peername))
        self._transport = None
        if self._server is not None:
            self._server.close()
            self._server = None

    def data_received(self, data):
        self._server.write(data)

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
