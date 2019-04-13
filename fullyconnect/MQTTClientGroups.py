
class MQTTClientGroups:
    def __init__(self):
        self._container = []
        self._cur = 0

    def add_client(self, client):
        self._container.append(client)

    # TODO: add more strategy
    def pick_client(self):
        client = self._container[self._cur % len(self._container)]
        self._cur += 1

        return client


if __name__ == "__main__":
    groups = MQTTClientGroups()
    groups.add_client("c1")
    groups.add_client("c2")

    for i in range(20):
        print(groups.pick_client())
