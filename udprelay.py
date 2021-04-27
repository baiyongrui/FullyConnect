import asyncio
import struct
import logging

import cryptor, common

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class UDPRelayServer:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

    def add_to_loop(self, loop):
        self._loop = loop
        coro = loop.create_datagram_endpoint(lambda: RelayServerProtocol(self._loop, self._config),
                                             local_addr=(self._config['server'], self._config['server_port']))
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class RelayServerProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._password = common.to_bytes(config['password'])
        self._method = config['method']

        self._sessions = {}

    @property
    def loop(self):
        return self._loop

    def connection_made(self, transport):
        self._transport = transport

    def datagram_received(self, data, addr):
        try:
            data, key, iv = cryptor.decrypt_all(self._password,
                                                self._method,
                                                data)
        except Exception:
            logging.error('decrypt data failed')
            return
        if not data:
            return
        header_result = common.parse_header(data)
        if header_result is None:
            logging.error(f"can not parse header when handling connection from {addr}")
            return
        addrtype, remote_addr, remote_port, header_length = header_result
        logging.debug(f"udp data to {remote_addr}:{remote_port} from {addr[0]}:{addr[1]}")

        client_key = f"{addr[0]}:{addr[1]}"
        remote = self._sessions.get(client_key, None)
        if not remote:
            remote = RelayRemoteProtocol(self, addr)
            self._sessions[client_key] = remote
            self._loop.create_task(self.create_endpoint(remote, common.to_str(remote_addr), remote_port))

        data = data[header_length:]
        remote.write(data)

    async def create_endpoint(self, remote, host, port):
        try:
            await self._loop.create_datagram_endpoint(lambda: remote, remote_addr=(host, port))
        except OSError as e:
            logging.error(f"{e} when creating endpoint to {host}:{port}")

    def write(self, data, addr, r_addr):
        # try:
        data = common.pack_addr(r_addr[0]) + struct.pack('>H', r_addr[1]) + data
        data = cryptor.encrypt_all(self._password, self._method, data)

        self._transport.sendto(data, addr)

    def clear_session(self, client_addr):
        client_key = f"{client_addr[0]}:{client_addr[1]}"
        del self._sessions[client_key]


class RelayRemoteProtocol(asyncio.DatagramProtocol):
    def __init__(self, server, addr):
        self._transport = None
        self._write_pending_data = []
        self._connected = False
        self._server = server
        self._client_addr = addr
        self._last_activity = 0
        self._timeout = 60
        self._timeout_handle = None

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
        self._server.write(data, self._client_addr, addr)
        self._last_activity = self._server.loop.time()

    def error_received(self, exc):
        if self._transport is not None:
            self._transport.close()
        else:
            self._server.clear_session(self._client_addr)

    def connection_lost(self, exc):
        self._server.clear_session(self._client_addr)

    def write(self, data):
        if not self._connected:
            self._write_pending_data.append(data)
        else:
            self._transport.sendto(data)
            self._last_activity = self._server.loop.time()

    def timeout_handler(self):
        after = self._last_activity - self._server.loop.time() + self._timeout
        if after < 0:
            logging.info(f"session {self._client_addr[0]}:{self._client_addr[1]} closed")
            self._transport.close()
        else:
            self._timeout_handle = self._server.loop.call_later(after, self.timeout_handler)
