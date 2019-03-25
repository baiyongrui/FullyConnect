# 
# udpsession_lru.py
# FullyConnect
# 
# Created by baiyongrui on 2019-03-25
#

from collections import OrderedDict


class UDPSessionLRU(object):

    def __init__(self, size, topic_generator):
        super(UDPSessionLRU, self).__init__()
        self._container = OrderedDict()   # addr to topic
        self._size = size
        self._topic_generator = topic_generator
        self._topic_to_addr = {}

    def addr_to_topic(self, addr):
        topic = self._container.get(addr, None)
        if topic is None:
            topic = next(self._topic_generator)

            if len(self._container) == self._size:
                item = self._container.popitem()
                self._topic_to_addr.pop(item[1])

            self._container[addr] = topic
            self._topic_to_addr[topic] = addr

        self._container.move_to_end(addr, last=False)

        return topic

    def topic_to_addr(self, topic):
        addr = self._topic_to_addr.get(topic, None)
        return addr

    def clear(self):
        self._container.clear()
        self._topic_to_addr.clear()
