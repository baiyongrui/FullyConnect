from enum import Enum

MAX_CHUNK_ID = 65535


class ChunkType(Enum):
    Data = 0,
    Disconnect = 1


class DataChunk:
    def __init__(self, type: ChunkType, chunk_id: int, connection_id: str, data: bytes):
        self._type = type
        self._id = chunk_id
        self._connection_id = connection_id
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

    @staticmethod
    def build_data_chunk(chunk_id: int, connection_id: str, data: bytes):
        return DataChunk(ChunkType.Data, chunk_id, connection_id, data)

    @staticmethod
    def build_disconnect_chunk(chunk_id: int, connection_id: str):
        return DataChunk(ChunkType.Disconnect, chunk_id, connection_id, None)


class ChunkGenerator:
    def __init__(self):
        self._chunk_id = 0

    def _inc(self):
        self._chunk_id += 1
        if self._chunk_id >= MAX_CHUNK_ID:
            self._chunk_id = 0

    def fragment(self, connection_id: str, data: bytes, parts=2):
        chunks = []
        chunk_size = len(data)
        if chunk_size > parts:
            chunk_size = len(data) // parts

        while len(data) > 0:
            chunk = DataChunk.build_data_chunk(self._chunk_id, connection_id, data[:chunk_size])
            data = data[chunk_size:]
            chunks.append(chunk)

            self._inc()

        return chunks

    def get_chunk_id(self):
        ret = self._chunk_id
        self._inc()
        return ret


class ChunkProcessor:
    def __init__(self):
        self._chunks = {}
        self._cur_chunk_id = 0

    def store(self, chunk: DataChunk):
        self._chunks[chunk.id] = chunk

    def dump_ordered(self):
        out = []
        while True:
            chunk = self._chunks.get(self._cur_chunk_id, None)
            if chunk is not None:
                out.append(chunk)
                self._chunks.pop(chunk.id)
                self._cur_chunk_id += 1
                if self._cur_chunk_id >= MAX_CHUNK_ID:
                    self._cur_chunk_id = 0
            else:
                break
        return out

    def reset(self):
        self._chunks.clear()
        self._cur_chunk_id = 0
