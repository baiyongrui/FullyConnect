
class ConnectionGroup:
    def __init__(self):
        self._container = []
        self._cur = 0

    def add_connection(self, connection):
        self._container.append(connection)

    def remove_connection(self, connection):
        self._container.remove(connection)

        if self._cur >= len(self._container):
            self._cur = 0

    # TODO: add more strategy
    def pick_connection(self):
        items = len(self._container)
        if items > 0:
            client = self._container[self._cur % items]
            self._cur += 1
            return client
        else:
            return None

    def __len__(self):
        return len(self._container)


if __name__ == "__main__":
    group = ConnectionGroup()
    group.add_connection("c1")
    group.add_connection("c2")

    for i in range(20):
        print(group.pick_connection())
