# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.


class fullyconnectException(Exception):
    """
    fullyconnect base exception
    """
    pass


class MQTTException(Exception):
    """
    Base class for all errors refering to MQTT specifications
    """
    pass


class CodecException(Exception):
    """
    Exceptions thrown by packet encode/decode functions
    """
    pass


class NoDataException(Exception):
    """
    Exceptions thrown by packet encode/decode functions
    """
    pass
