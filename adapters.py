# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
import asyncio
import io
from asyncio import StreamReader, StreamWriter, events
import logging


class ReaderAdapter:
    """
    Base class for all network protocol reader adapter.

    Reader adapters are used to adapt read operations on the network depending on the protocol used
    """

    @asyncio.coroutine
    def read(self, n=-1) -> bytes:
        """
        Read up to n bytes. If n is not provided, or set to -1, read until EOF and return all read bytes.
        If the EOF was received and the internal buffer is empty, return an empty bytes object.
        :return: packet read as bytes data
        """

    def feed_eof(self):
        """
        Acknowleddge EOF
        """


class WriterAdapter:
    """
    Base class for all network protocol writer adapter.

    Writer adapters are used to adapt write operations on the network depending on the protocol used
    """

    def write(self, data):
        """
        write some data to the protocol layer
        """

    @asyncio.coroutine
    def drain(self):
        """
        Let the write buffer of the underlying transport a chance to be flushed.
        """

    def get_peer_info(self):
        """
        Return peer socket info (remote address and remote port as tuple
        """

    @asyncio.coroutine
    def close(self):
        """
        Close the protocol connection
        """


class StreamReaderAdapter(ReaderAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on StreamReader to read from a TCP socket.
    Because API is very close, this class is trivial
    """
    def __init__(self, reader: StreamReader):
        self._reader = reader

    @asyncio.coroutine
    def read(self, n=-1) -> bytes:
        return (yield from self._reader.read(n))

    def feed_eof(self):
        return self._reader.feed_eof()


class StreamWriterAdapter(WriterAdapter):
    """
    Asyncio Streams API protocol adapter
    This adapter relies on StreamWriter to write to a TCP socket.
    Because API is very close, this class is trivial
    """
    def __init__(self, writer: StreamWriter):
        self.logger = logging.getLogger(__name__)
        self._writer = writer

    def write(self, data):
        self._writer.write(data)

    @asyncio.coroutine
    def drain(self):
        yield from self._writer.drain()

    def get_peer_info(self):
        extra_info = self._writer.get_extra_info('peername')
        return extra_info[0], extra_info[1]

    @asyncio.coroutine
    def close(self):
        yield from self._writer.drain()
        if self._writer.can_write_eof():
            self._writer.write_eof()
        self._writer.close()


class BufferReader(ReaderAdapter):
    """
    Byte Buffer reader adapter
    This adapter simply adapt reading a byte buffer.
    """
    def __init__(self, buffer: bytes):
        self._stream = io.BytesIO(buffer)

    @asyncio.coroutine
    def read(self, n=-1) -> bytes:
        return self._stream.read(n)


class BufferWriter(WriterAdapter):
    """
    ByteBuffer writer adapter
    This adapter simply adapt writing to a byte buffer
    """
    def __init__(self, buffer=b''):
        self._stream = io.BytesIO(buffer)

    def write(self, data):
        """
        write some data to the protocol layer
        """
        self._stream.write(data)

    @asyncio.coroutine
    def drain(self):
        pass

    def get_buffer(self):
        return self._stream.getvalue()

    def get_peer_info(self):
        return "BufferWriter", 0

    @asyncio.coroutine
    def close(self):
        self._stream.close()
