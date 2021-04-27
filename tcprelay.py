import asyncio
from asyncio import ensure_future, Queue
import logging
import cryptor, common

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class TCPRelay:

    def __init__(self, config):
        self._loop = None
        self._server = None
        self._config = config

    def add_to_loop(self, loop):
        self._loop = loop
        coro = loop.create_server(lambda: RelayServerProtocol(self._loop, self._config),
                                  '0.0.0.0', self._config['port'])
        self._server = loop.run_until_complete(coro)

    def close(self):
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())


class RelayServerProtocol(asyncio.Protocol):

    def __init__(self, loop, config):
        self._loop = loop
        self._transport = None
        self._encryptor = cryptor.Cryptor(config['password'], config['method'])
        self._peername = None
        self._remote = None
        self._last_activity = 0
        self._timeout = config['timeout']
        self._timeout_handle = None

        self._write_task = None
        self._queue = Queue(maxsize=1024, loop=self._loop)

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport
        self._last_activity = self._loop.time()
        self._timeout_handle = self._loop.call_later(self._timeout, self.timeout_handler)

        self._write_task = self._loop.create_task(self._consume_to_write())

    def connection_lost(self, exc):
        logging.info(f"client {self._peername} connection lost.")
        self._transport = None
        if self._remote:
            self._remote.close()
        self._timeout_handle.cancel()
        self._timeout_handle = None
        if self._write_task:
            self._write_task.cancel()

    def data_received(self, data):
        if not self._transport or self._transport.is_closing():
            return

        data = self._encryptor.decrypt(data)
        if not data:
            self.close()
            return

        self._last_activity = self._loop.time()

        if self._remote:
            self._remote.write(data)
        else:
            header_result = common.parse_header(data)
            if header_result is None:
                logging.error(
                    f"can not parse header when handling connection from {self._peername[0]}:{self._peername[1]}")
                self._transport.close()
                return

            addrtype, remote_addr, remote_port, header_length = header_result
            logging.info(
                f"connecting to {common.to_str(remote_addr)}:{remote_port} from {self._peername[0]}:{self._peername[1]}")

            self._remote = RelayRemoteProtocol(self)
            self._loop.create_task(self.create_connection(common.to_str(remote_addr), remote_port))

            if len(data) > header_length:
                self._remote.write(data[header_length:])

    async def _consume_to_write(self):
        while self._transport is not None:
            data = await self._queue.get()
            if self._transport is None:
                break
            self._transport.write(data)
            self._last_activity = self._loop.time()

    async def enqueue_to_write(self, data):
        if self._transport is None or self._transport.is_closing():
            return
        data = self._encryptor.encrypt(data)
        await self._queue.put(data)

    async def create_connection(self, host, port):
        try:
            # TODO handle pending task
            transport, protocol = await self._loop.create_connection(lambda: self._remote, host, port)
        except OSError as e:
            logging.error(f"{e} when connecting to {host}:{port} from {self._peername[0]}:{self._peername[1]}")
            self.close()

    def close(self):
        if self._transport:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logging.info("connection from {0}:{1} timeout".format(self._peername[0], self._peername[1]))
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


class RelayRemoteProtocol(asyncio.Protocol):

    def __init__(self, server: RelayServerProtocol):
        self._transport = None
        self._write_pending_data = []
        self._server = server
        self._loop = asyncio.get_event_loop()

        self._peername = None

        self._last_activity = 0     # for read timeout
        self._timeout = 60      # TODO: from config
        self._timeout_handler = None

    def connection_made(self, transport):
        self._peername = transport.get_extra_info('peername')
        self._transport = transport

        if self._server is None:
            self._transport.close()
            return

        if len(self._write_pending_data) > 0:
            data = b''.join(self._write_pending_data)
            self._write_pending_data = []
            self._transport.write(data)

        self._last_activity = self._loop.time()
        self._timeout_handler = self._loop.call_later(self._timeout, self.timeout_handler)

    def connection_lost(self, exc):
        logging.info("remote {0} connection lost.".format(self._peername))
        self._transport = None
        if self._server:
            self._server.close()
            self._server = None

        if self._timeout_handler is not None:
            self._timeout_handler.cancel()
            self._timeout_handler = None

    def data_received(self, data):
        if self._transport is None:
            return
        self._transport.pause_reading()
        task = ensure_future(self.relay_data(data), loop=self._loop)

        def maybe_resume_reading(_):
            if self._transport is not None:
                self._transport.resume_reading()
        
        task.add_done_callback(maybe_resume_reading)

        self._last_activity = self._loop.time()

    async def relay_data(self, data: bytes):
        await self._server.enqueue_to_write(data)

    def write(self, data):
        if self._transport:
            self._transport.write(data)
        else:
            self._write_pending_data.append(data)

    def close(self):    # closed by local
        self._server = None
        if self._transport:
            self._transport.close()

    def timeout_handler(self):
        after = self._last_activity - self._loop.time() + self._timeout
        if after < 0:
            logging.info(f"remote {self._peername} connection timeout")
            self.close()
        else:
            self._timeout_handle = self._loop.call_later(after, self.timeout_handler)


if __name__ == '__main__':
    server = TCPRelay({"password": "123456", "method": "aes-128-cfb", "timeout": 60, "port": 8700})
    # import uvloop
    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()
    server.add_to_loop(loop)
    loop.run_forever()
