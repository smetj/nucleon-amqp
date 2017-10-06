import socket
from .spec_exceptions import *


class ConnectionError(socket.error):
    """The connection was lost."""



class UnsupportedProtocol(socket.error):
    pass


class ChannelClosed(AMQPSoftError):
    """The channel has been closed by user intervention."""


class MessageReturned(AMQPSoftError):
    """A message was not delivered.

    The first argument will be the message that was returned.
    """


class PublishFailed(AMQPSoftError):
    """The server sent a basic.nack.

    The first argument of this exception will be the frame that was received.
    """


class MessageReturnedNoRoute(MessageReturned):
    """A message was not delivered because there was no binding to do so."""
    reply_code = 312


class MessageReturnedNoConsumers(MessageReturned):
    """A message was not delivered because nothing was accepting."""
    reply_code = 313


ERRORS[MessageReturnedNoRoute.reply_code] = MessageReturnedNoRoute
ERRORS[MessageReturnedNoConsumers.reply_code] = MessageReturnedNoConsumers


def exception_from_frame(frame):
    """Get an exception from a frame."""
    if frame.name == 'channel.close-ok':
        return ChannelClosed("Channel closed by local client")
    else:
        cls = ERRORS.get(frame.reply_code, AMQPError)
        return cls(frame.reply_text, frame.reply_code)


def return_exception_from_frame(frame):
    """Get a return exception.

    This differs only in that we default to MessageReturned.
    """
    try:
        code = frame._frame.reply_code
        text = frame._frame.reply_text
    except AttributeError:
        code = frame.reply_code
        text = frame.reply_text
    cls = ERRORS.get(code, MessageReturned)
    return cls(text, code)
