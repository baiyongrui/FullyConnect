from enum import IntEnum
from struct import pack, unpack

MAX_CHUNK_ID = 65535
CHUNK_HEADER_LENGTH = 5


class ChunkType(IntEnum):
    DATA = 0,
    CONNECT = 1
    DISCONNECT = 2
    LOW_WATER_MARK = 3
    HIGH_WATER_MARK = 4


class DataChunk:
    def __init__(self, type: ChunkType, chunk_id: int, connection_id: int, data: bytes = None):
        self._type = type                       # 1 byte
        self._id = chunk_id                     # 4 byte
        self._connection_id = connection_id     # 4 byte
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def id(self):
        return self._id

    @property
    def connection_id(self):
        return self._connection_id

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data: bytes):
        self._data = data

    def to_bytes(self):
        out = bytearray()

        # type
        out.extend(pack("!B", self._type.value))
        # id
        out.extend(pack("!H", self._id))
        # connection id
        out.extend(pack("!H", self._connection_id))
        # data
        if self._data is not None:
            out.extend(self._data)

        return out

    @staticmethod
    def build_data_chunk(chunk_id: int, connection_id: int, data: bytes):
        return DataChunk(ChunkType.DATA, chunk_id, connection_id, data)

    @staticmethod
    def build_connect_chunk(chunk_id: int, connection_id: int, data: bytes):
        return DataChunk(ChunkType.CONNECT, chunk_id, connection_id, data)

    @staticmethod
    def build_disconnect_chunk(chunk_id: int, connection_id: int):
        return DataChunk(ChunkType.DISCONNECT, chunk_id, connection_id, None)

    @staticmethod
    def build_lwm_chunk(chunk_id: int, connection_id: int):
        return DataChunk(ChunkType.LOW_WATER_MARK, chunk_id, connection_id, None)

    @staticmethod
    def build_hwm_chunk(chunk_id: int, connection_id: int):
        return DataChunk(ChunkType.HIGH_WATER_MARK, chunk_id, connection_id, None)

    @staticmethod
    def from_bytes(buf: bytearray):
        chunk = None
        if len(buf) >= CHUNK_HEADER_LENGTH:
            type, id, connection_id = unpack("!BHH", buf[:CHUNK_HEADER_LENGTH])
            if type == ChunkType.DATA:
                data = bytes(buf[CHUNK_HEADER_LENGTH:])
                chunk = DataChunk.build_data_chunk(id, connection_id, data)
            elif type == ChunkType.CONNECT:
                data = bytes(buf[CHUNK_HEADER_LENGTH:])
                chunk = DataChunk.build_connect_chunk(id, connection_id, data)
            elif type == ChunkType.DISCONNECT:
                chunk = DataChunk.build_disconnect_chunk(id, connection_id)
            elif type == ChunkType.LOW_WATER_MARK:
                chunk = DataChunk.build_lwm_chunk(id, connection_id)
            elif type == ChunkType.HIGH_WATER_MARK:
                chunk = DataChunk.build_hwm_chunk(id, connection_id)

        return chunk


class ChunkProcessor:
    def __init__(self):
        self._chunks = {}
        self._cur_recv_id = 0
        self._cur_send_id = 0

    def store(self, chunk: DataChunk):
        self._chunks[chunk.id] = chunk

    def dump_ordered(self):
        out = []
        while True:
            chunk = self._chunks.get(self._cur_recv_id, None)
            if chunk is not None:
                out.append(chunk)
                self._chunks.pop(chunk.id)
                self._cur_recv_id += 1
                if self._cur_recv_id >= MAX_CHUNK_ID:
                    self._cur_recv_id = 0
            else:
                break
        return out

    def pack_data(self, connection_id: int, data: bytes, parts=1):
        data_chunks = []
        chunk_size = len(data)
        if chunk_size > parts:
            chunk_size = len(data) // parts

        while len(data) > 0:
            chunk = DataChunk.build_data_chunk(self._cur_send_id, connection_id, data[:chunk_size])
            data = data[chunk_size:]
            data_chunks.append(chunk)

            self._cur_send_id += 1
            if self._cur_send_id >= MAX_CHUNK_ID:
                self._cur_send_id = 0

        return data_chunks

    def pack_connect(self, connection_id: int, data: bytes):
        connect_chunk = DataChunk.build_connect_chunk(self._cur_send_id, connection_id, data)
        self._cur_send_id += 1
        if self._cur_send_id >= MAX_CHUNK_ID:
            self._cur_send_id = 0

        return connect_chunk

    def pack_disconnect(self, connection_id: int):
        disconnect_chunk = DataChunk.build_disconnect_chunk(self._cur_send_id, connection_id)
        self._cur_send_id += 1
        if self._cur_send_id >= MAX_CHUNK_ID:
            self._cur_send_id = 0

        return disconnect_chunk

    def pack_lwm(self, connection_id: int):
        lwm_chunk = DataChunk.build_lwm_chunk(self._cur_send_id, connection_id)
        self._cur_send_id += 1
        if self._cur_send_id >= MAX_CHUNK_ID:
            self._cur_send_id = 0

        return lwm_chunk

    def pack_hwm(self, connection_id: int):
        hwm_chunk = DataChunk.build_hwm_chunk(self._cur_send_id, connection_id)
        self._cur_send_id += 1
        if self._cur_send_id >= MAX_CHUNK_ID:
            self._cur_send_id = 0

        return hwm_chunk

    def reset(self):
        self._chunks.clear()
        self._cur_recv_id = 0
