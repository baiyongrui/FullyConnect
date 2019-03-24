# 
# publish.py
# FullyConnect
# 
# Created by baiyongrui on 2019-03-21
#

from struct import pack, unpack

from fullyconnect.mqtt_sn.packet import PUBLISH


class PublishPacket(object):

    DUP_FLAG = 0x80
    RETAIN_FLAG = 0x10
    QOS_FLAG = 0x60
    TOPIC_ID_TYPE = 0x3

    def __init__(self, topic_id: int, msg_id: int, data: bytes, flags=0):
        super().__init__()

        self.length = 9 + len(data)
        self.msg_type = PUBLISH

        self.flags = flags
        self.dup_flag = False
        self.qos_flag = 0
        self.retain_flag = False
        self.topic_id_type = 0

        self.topic_id = topic_id
        self.msg_id = msg_id
        self.data = data

    @property
    def dup_flag(self) -> bool:
        return self._get_header_flag(self.DUP_FLAG)

    @dup_flag.setter
    def dup_flag(self, val: bool):
        self._set_header_flag(val, self.DUP_FLAG)

    @property
    def retain_flag(self) -> bool:
        return self._get_header_flag(self.RETAIN_FLAG)

    @retain_flag.setter
    def retain_flag(self, val: bool):
        self._set_header_flag(val, self.RETAIN_FLAG)

    @property
    def qos_flag(self) -> int:
        return (self.flags & self.QOS_FLAG) >> 5

    @qos_flag.setter
    def qos_flag(self, val: int):
        self.flags &= 0x9f
        self.flags |= (val << 5)

    @property
    def topic_id_type(self) -> int:
        return self.flags & self.TOPIC_ID_TYPE

    @topic_id_type.setter
    def topic_id_type(self, val: int):
        self.flags &= 0xFC
        self.flags |= val

    def to_bytes(self):
        out = bytearray()

        # Length
        out.extend(pack("!B", 1))
        out.extend(pack("!H", self.length))
        # MsgType
        out.extend(pack("!B", self.msg_type))
        # Flags
        out.extend(pack("!B", self.flags))
        # TopicId
        out.extend(pack("!H", self.topic_id))
        # MsgId
        out.extend(pack("!H", self.msg_id))
        # Data
        out.extend(self.data)

        return out

    def _get_header_flag(self, mask):
        if self.flags & mask:
            return True
        else:
            return False

    def _set_header_flag(self, val, mask):
        if val:
            self.flags |= mask
        else:
            self.flags &= ~mask

    @classmethod
    def decode(cls, buf):
        if len(buf) < 9:
            return None

        length, msg_type, flags, topic_id, msg_id = unpack("!HBBHH", buf[1:9])
        data = buf[9:]

        return cls(topic_id, msg_id, data, flags)


if __name__ == "__main__":
    packet = PublishPacket(topic_id=22, msg_id=5, data="data".encode())
    bytes = packet.to_bytes()

    print(bytes)

    packet = PublishPacket.decode(bytes)
    packet.retain_flag = True
    packet.dup_flag = True
    packet.qos = 2

    flags = packet.flags
    length = packet.length
    msg_type = packet.msg_type
    topic_id = packet.topic_id
    msg_id = packet.msg_id

    dup_flag = packet.dup_flag
    qos = packet.qos
    retain_flag = packet.retain_flag

    print("ok")

