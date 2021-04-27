# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.
from fullyconnect.errors import FullyConnectException
from fullyconnect.mqtt.packet import (
    CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE,
    SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT,
    MQTTFixedHeader)
from fullyconnect.mqtt.connect import ConnectPacket
from fullyconnect.mqtt.connack import ConnackPacket
from fullyconnect.mqtt.disconnect import DisconnectPacket
from fullyconnect.mqtt.pingreq import PingReqPacket
from fullyconnect.mqtt.pingresp import PingRespPacket
from fullyconnect.mqtt.publish import PublishPacket
from fullyconnect.mqtt.puback import PubackPacket
from fullyconnect.mqtt.pubrec import PubrecPacket
from fullyconnect.mqtt.pubrel import PubrelPacket
from fullyconnect.mqtt.pubcomp import PubcompPacket
from fullyconnect.mqtt.subscribe import SubscribePacket
from fullyconnect.mqtt.suback import SubackPacket
from fullyconnect.mqtt.unsubscribe import UnsubscribePacket
from fullyconnect.mqtt.unsuback import UnsubackPacket

packet_dict = {
    CONNECT: ConnectPacket,
    CONNACK: ConnackPacket,
    PUBLISH: PublishPacket,
    PUBACK: PubackPacket,
    PUBREC: PubrecPacket,
    PUBREL: PubrelPacket,
    PUBCOMP: PubcompPacket,
    SUBSCRIBE: SubscribePacket,
    SUBACK: SubackPacket,
    UNSUBSCRIBE: UnsubscribePacket,
    UNSUBACK: UnsubackPacket,
    PINGREQ: PingReqPacket,
    PINGRESP: PingRespPacket,
    DISCONNECT: DisconnectPacket
}


def packet_class(fixed_header: MQTTFixedHeader):
    try:
        cls = packet_dict[fixed_header.packet_type]
        return cls
    except KeyError:
        raise FullyConnectException("Unexpected packet Type '%s'" % fixed_header.packet_type)
