
class ConnectionPool:
    def __init__(self):
        self._pool = []
        self._fetched_idx = 0

    def add(self, conn):
        self._pool.append(conn)

    def remove(self, conn):
        self._pool.remove(conn)

        if self._fetched_idx >= len(self._pool):
            self._fetched_idx = 0

    def fetch(self):
        num_conns = len(self._pool)
        if num_conns > 0:
            conn = self._pool[self._fetched_idx % num_conns]
            self._fetched_idx += 1
            return conn
        else:
            return None

    def __len__(self):
        return len(self._pool)


class ConnectionGroup:
    def __init__(self):
        self._container = []
        self._cur = 0

        self._groups = {}

    def add_connection(self, connection, group_name):
        connection_pool = self._groups.get(group_name, None)
        if connection_pool is None:
            connection_pool = ConnectionPool()
            self._groups[group_name] = connection_pool
        connection_pool.add(connection)

    def remove_connection(self, connection, group_name):
        connection_pool = self._groups.get(group_name, None)
        if connection_pool:
            connection_pool.remove(connection)
            if len(connection_pool) == 0:
                self._groups.pop(group_name, None)

    def get(self, group_name):
        return self._groups.get(group_name, None)

    def __len__(self):
        return len(self._groups)


if __name__ == "__main__":
    group = ConnectionGroup()
    group.add_connection("c1", "g1")
    group.add_connection("c2", "g1")

    conn_pool = group.get("g1")
    for i in range(20):
        print(conn_pool.fetch())
