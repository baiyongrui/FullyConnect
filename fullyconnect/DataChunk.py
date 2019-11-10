
MAX_CHUNKS = 8192


class DataChunk:
    def __init__(self, chunk_index, data):
        self.id = chunk_index
        self.data = data


class ChunkGenerator:
    def __init__(self):
        self._chunk_id = 0

    def split(self, data, parts=2):
        chunks = []
        chunk_size = len(data)
        if chunk_size > parts:
            chunk_size = len(data) // parts

        while len(data) > 0:
            chunk = DataChunk(self._chunk_id, data[:chunk_size])
            data = data[chunk_size:]
            chunks.append(chunk)

            self._chunk_id += 1
            if self._chunk_id >= MAX_CHUNKS:
                self._chunk_id = 0

        return chunks


class ChunkCache:
    def __init__(self):
        self._chunks = [None for _ in range(MAX_CHUNKS)]
        self._cur_chunk_idx = 0

    def store(self, chunk: DataChunk):
        idx = chunk.id % len(self._chunks)

        if self._chunks[idx] is not None:
            # reached maximum limit
            return False
        self._chunks[idx] = chunk
        return True

    def dump(self):
        out = bytearray()
        while True:
            chunk = self._chunks[self._cur_chunk_idx]
            if chunk is not None:
                out.extend(chunk.data)
                self._chunks[self._cur_chunk_idx] = None
                self._cur_chunk_idx += 1
                if self._cur_chunk_idx >= len(self._chunks):
                    self._cur_chunk_idx = 0
            else:
                break
        return out
