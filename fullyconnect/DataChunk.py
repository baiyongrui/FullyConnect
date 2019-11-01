
class DataChunk:
    def __init__(self, chunk_index, data):
        self.chunk_index = chunk_index
        self.data = data


class ChunkCache:
    def __init__(self, size=4096):
        self._chunks = [None for _ in range(size)]
        self._cur_chunk_idx = 0

    def store(self, chunk: DataChunk):
        idx = chunk.chunk_index % len(self._chunks)

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
