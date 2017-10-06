import logging
from collections import deque
from cStringIO import StringIO

import gevent
from gevent.queue import Queue, Empty
from gevent.lock import RLock
from gevent.event import AsyncResult

from .message import Message
from .encoding import encode_message

from . import spec
from . import exceptions


log = logging.getLogger(__name__)


class EventRegister(object):
    """Handle event registration according to AMQP semantics.

    Effectively, we register synchronous events to be received once only, while
    some events (eg. connection close) can be handled at any time, and persist.

    """
    def __init__(self):
        self.permanent = {}
        self.once = deque()

    def set_handler(self, method, callback):
        """Set a permanent handler for an event."""
        self.permanent[method] = callback

    def unset_handler(self, method):
        """Unset a permanent handler for an event."""
        del(self.permanent[method])

    def wait_event(self, methods, on_success, on_error):
        """Set a temporary handler for one or more events."""
        self.once.append((frozenset(methods), on_success, on_error))

    def cancel_event(self, methods, on_success, on_error):
        """Cancel a temporary handler for one or more events."""
        try:
            self.once.remove((frozenset(methods), on_success, on_error))
        except ValueError:
            pass

    def get_error_handlers(self):
        """Get all error handlers."""
        hs = [on_error for _, _, on_error in self.once]
        self.once.clear()
        return hs

    def get(self, method):
        """Get the callbacks for a method.

        This method returns at most one temporary callback and one permanent
        callback.

        """
        cbs = []
        for handler in self.once:
            methods, on_success, _ = handler
            if method in methods:
                self.once.remove(handler)
                cbs.append(on_success)
                break

        try:
            cbs.append(self.permanent[method])
        except KeyError:
            pass
        return cbs


class Channel(spec.FrameWriter):
    """A communication channel.

    Multiple channels are multiplexed onto the same connection.
    """
    def __init__(self, connection, id):
        self.connection = connection
        self.send_lock = RLock()
        self.id = id
        self.exc = None
        self._method = None
        self.listeners = EventRegister()
        self.queue = Queue()
        self.listeners.set_handler('channel.close', self.on_channel_close)
        self.listeners.set_handler('channel.close-ok', self.on_channel_close)
        self.listeners.set_handler('connection.close', self.on_connection_close)
        self.listeners.set_handler('connection.close-ok', self.on_connection_close)

        # Start an initial dispatcher
        self.replace_dispatcher()

    def on_connection_close(self, frame):
        """Handle a connection close error."""
        exc = exceptions.exception_from_frame(frame)

        # Let the connection class dispatch this to all channels
        self.connection._on_error(exc)

    def on_error(self, exc):
        """Handle an error that is causing this channel to close."""
        self.connection._remove_channel(self.id)
        self.exc = exc
        for handler in self.listeners.get_error_handlers():
            self.queue.put((handler, (exc,)))
        self.stop_dispatcher()

    def on_channel_close(self, frame):
        """Handle a channel.close method."""
        exc = exceptions.exception_from_frame(frame)
        if frame.name == 'channel.close':
            self.channel_close_ok()
        self.on_error(exc)

    def _send(self, frame):
        """Send one frame over the channel."""
        if self.exc:
            raise self.exc
        with self.send_lock:
            self.connection._send_frames(self.id, frame.encode())

    def _send_message(self, frame, headers, payload):
        """Send method, header and body frames over the channel."""
        if self.exc:
            raise self.exc
        fs = encode_message(frame, headers, payload, self.connection.frame_max)
        with self.send_lock:
            self.connection._send_frames(self.id, fs)

    def _on_method(self, frame):
        """Called when the channel has received a method frame."""
        if frame.has_content:
            self._method = frame
        else:
            self.dispatch(frame.name, frame)

    def _on_headers(self, size, props):
        """Called when the channel has received a headers frame."""
        self._headers = props
        self._to_read = size
        self._body = StringIO()
        self._on_content_receive()

    def _on_body(self, payload):
        """Called when the channel has received a body frame."""
        self._body.write(payload)
        self._to_read -= len(payload)
        self._on_content_receive()

    def _on_content_receive(self):
        """Check whether a full message has been read, and if so, dispatch it.

        No payload frame is sent if the body was empty.
        """
        if self._to_read <= 0:
            if self._headers is None or self._method is None:
                return

            m = Message(self,
                self._method,
                self._headers,
                self._body.getvalue()
            )
            self.dispatch(self._method.name, m)
            self._headers = None
            self._method = None
            self._body = None

    def _call_sync(self, method, responses, *args, **kwargs):
        """Call a method, using AsyncResult to wait on the response."""
        result = AsyncResult()

        self.listeners.wait_event(responses, result.set, result.set_exception)

        try:
            method(*args, **kwargs)
        except:
            self.listeners.cancel_event(
                responses, result.set, result.set_exception
            )
            raise

        self.must_now_block()
        return result.get()

    def dispatch(self, method, *args):
        """Fire the listener for a given method.

        This enqueues all currently registered listeners for eventual dispatch
        by the dispatcher greenlet.

        This means that if the dispatcher greenlet abdicates, the new
        dispatcher can pick up with the next listener to be called.

        """
        l = self.listeners.get(method)
        if l:
            for h in l:
                self.queue.put((h, args))
        else:
            print "Unhandled method", method

    def stop_dispatcher(self):
        """Tell the dispatcher to stop after processing all current events"""
        self.queue.put((None, None))

    def start_dispatcher(self):
        """Dispatch callbacks, intended to be run as a separate greenlet.

        Loops until the connection is closed or the current greenlet is no
        longer the dispatcher. In the latter case, don't execute any more
        callbacks, as they will be executed by the real dispatcher.

        """
        while True:
            callback, args = self.queue.get()
            if callback is None:    # None tells the dispatcher to stop
                return

            try:
                callback(*args)
            except Exception:
                # FIXME: log it
                #import traceback
                #traceback.print_exc()
                pass
            if not self.current_is_dispatcher():
                break

    def must_now_block(self):
        """Signal that something is about to block on this connection.

        If the current greenlet is the dispatcher, we stop being so, and spawn
        a new dispatcher to provide us with the result we need to unblock
        ourselves.

        """
        if self.current_is_dispatcher():
            self.replace_dispatcher()

    def current_is_dispatcher(self):
        """Return True if the calling greenlet is the dispatcher."""

        return gevent.getcurrent() is self.dispatcher

    def replace_dispatcher(self):
        """Spawn a new dispatcher greenlet, replacing the current one.

        This automatically triggers the old dispatcher to stop dispatching
        after processing the current callback.

        """
        self.dispatcher = gevent.spawn(self.start_dispatcher)

    def close(self):
        from .connection import STATE_CONNECTED
        if self.exc:
            return

        if self.connection and self.connection.state == STATE_CONNECTED:
            self.channel_close(reply_code=200)


class StartChannel(Channel):
    """A channel to handle connection.

    The is the initial control channel opened by the server; we pre-register
    events so as to handle connection start.

    From the AMQP spec:

    * The server responds with its protocol version and other properties,
      including a list of the security mechanisms that it supports (the Start
      method).

    * The client selects a security mechanism (Start-Ok).

    * The server starts the authentication process, which uses the SASL
      challenge-response model. It sends the client a challenge (Secure).

    * The client sends an authentication response (Secure-Ok). For example
      using the "plain" mechanism, the response consist of a login name and
      password.

    * The server repeats the challenge (Secure) or moves to negotiation,
      sending a set of parameters such as maximum frame size (Tune).

    * The client accepts or lowers these parameters (Tune-Ok).

    * The client formally opens the connection and selects a virtual host
      (Open).

    * The server confirms that the virtual host is a valid choice (Open-Ok).

    """
    def __init__(self, connection, id):
        super(StartChannel, self).__init__(connection, id)
        self.listeners.set_handler('connection.start', self.on_start)
        self.listeners.set_handler('connection.tune', self.on_tune)
        self.listeners.set_handler('connection.close-ok', self.on_close_ok)

    def on_start(self, frame):
        """Handle the start frame."""
        # TODO: support SASL authentication
        assert 'PLAIN' in frame.mechanisms.split(), "Only PLAIN auth supported."

        auth = '\0%s\0%s' % (
            self.connection.username, self.connection.password
        )
        self.connection.server_properties = frame.server_properties
        scapa = frame.server_properties.get('capabilities', {})
        ccapa = {}
        if scapa.get('consumer_cancel_notify'):
            ccapa['consumer_cancel_notify'] = True

        self.connection_start_ok(
            {'product': 'nucleon.amqp', 'capabilities': ccapa},
            'PLAIN',
            auth,
            'en_US'
        )

    def on_close_ok(self, frame):
        self.connection._on_normal_disconnect()

    def on_tune(self, frame):
        """Handle the tune message.

        This message signals that we are allowed to open a virtual host.
        """
        self.connection._tune(frame.frame_max, frame.channel_max, frame.heartbeat)
        self.connection_tune_ok(frame.channel_max, frame.frame_max, self.connection.heartbeat)

        # open the connection
        self.connection_open(self.connection.vhost)
        self.connection._on_connect()


class MessageQueue(Queue):
    """A queue that receives messages from a consumer, and also exceptions."""
    def __init__(self, channel, consumer_tag):
        self.consumer_tag = consumer_tag
        self.channel = channel
        super(MessageQueue, self).__init__()

    def get(self, block=True, timeout=None):
        """Get a message from the queue.

        This method may also raise arbitrary AMQP exceptions, such as
        ConnectionError when the connection is lost.

        :param block: If True, block until a message is available in the queue.
            If False, raise Empty immediately if there are no messages in the
            queue.
        :param timeout: Time in seconds to wait. If not message is received
            after this time, Empty is raised.

        """
        if block:
            self.channel.must_now_block()
        resp = super(MessageQueue, self).get(block=block, timeout=timeout)
        if isinstance(resp, Exception):
            self.put(resp)  # re-queue the exception, as it should be raised
                            # for all subsequent gets
            raise resp
        return resp

    def get_nowait(self):
        """Get a message from the queue.

        Raises Empty immediately if there are no messages in the queue.

        """
        self.get(False)

    def cancel(self):
        """Cancel consuming with this consumer.

        There may still be messages in the queue, of course, which can still be
        processed, acknowledged, etc.

        """
        self.channel.basic_cancel(self.consumer_tag)


class MessageChannel(Channel):
    """A channel that adds useful semantics for publishing and consuming messages.

    Semantics that are added:

    * Support for registering consumers to receive basic.deliver events.
      Consumers also receive errors and are automatically deregistered when
      basic_cancel is received.

    * Support for the RabbitMQ extension confirm_select, which makes
      basic_publish block

    * Can check for messages returned with basic_return

    """
    def __init__(self, connection, id):
        super(MessageChannel, self).__init__(connection, id)
        self.consumer_id = 1
        self.consumers = {}
        self.returned = AsyncResult()
        self.listeners.set_handler('basic.deliver', self.on_deliver)
        self.listeners.set_handler('basic.return', self.on_basic_return)
        self.listeners.set_handler('basic.cancel-ok', self.on_cancel_ok)
        self.listeners.set_handler('basic.cancel', self.on_cancel_ok)

    def on_deliver(self, message):
        """Called when a message is received.

        Dispatches the message to the registered consumer.
        """
        self.consumers[message.consumer_tag](message)

    def on_basic_return(self, msg):
        """When we receive a basic.return message, store it.

        The value can later be checked using .check_returned().

        """
        self.returned.set(msg)

    def check_returned(self):
        """Raise an error if a message has been returned.

        This also clears the returned frame, with the intention that each
        basic.return message may cause at most one MessageReturned error.

        """
        if self._method and self._method.name == 'basic.return':
            self.must_now_block()
            returned = self.returned.get()
        else:
            try:
                returned = self.returned.get_nowait()
            except gevent.Timeout:
                return

        self.clear_returned()
        if returned:
            raise exceptions.return_exception_from_frame(returned)

    def clear_returned(self):
        """Discard any returned message."""
        if self.returned.ready():
            # we can only replace returned if it is ready - otherwise anything
            # that was blocked waiting would wait forever.
            self.returned = AsyncResult()

    def on_error(self, exc):
        """Override on_error, to pass error to all consumers."""
        for consumer in self.consumers.values():
            self.queue.put((consumer, (exc,)))
        super(MessageChannel, self).on_error(exc)

    def on_cancel_ok(self, frame):
        """The server has cancelled a consumer.

        We can remove its consumer tag from the registered consumers."""
        del(self.consumers[frame.consumer_tag])

    def basic_consume(self, queue='', no_local=False, no_ack=False, exclusive=False, arguments={}, callback=None):
        """Begin consuming messages from a queue.

        Consumers last as long as the channel they were declared on, or until
        the client cancels them.

        :param queue: Specifies the name of the queue to consume from.
        :param no_local: Do not deliver own messages. If this flag is set the
            server will not send messages to the connection that published
            them.
        :param no_ack: Don't require acknowledgements. If this flag is set the
            server does not expect acknowledgements for messages. That is, when
            a message is delivered to the client the server assumes the
            delivery will succeed and immediately dequeues it. This
            functionality may increase performance but at the cost of
            reliability. Messages can get lost if a client dies before they
            are delivered to the application.
        :param exclusive: Request exclusive consumer access, meaning only this
            consumer can access the queue.
        :param arguments: A set of arguments for the consume. The syntax and
            semantics of these arguments depends on the server implementation.
        :param callback: A callback to be called for each message received.

        """
        tag = 'ct-%d' % self.consumer_id
        self.consumer_id += 1

        kwargs = dict(
            queue=queue,
            no_local=no_local,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
            consumer_tag=tag
        )

        if callback is not None:
            self.consumers[tag] = callback
            return super(MessageChannel, self).basic_consume(**kwargs)
        else:
            queue = MessageQueue(self, tag)
            self.consumers[tag] = queue.put
            super(MessageChannel, self).basic_consume(**kwargs)
            return queue

    def basic_get(self, *args, **kwargs):
        """Wrap basic_get to return None if the response is basic.get-empty.

        This will be easier for users to check than testing whether a response
        is get-empty.
        """
        r = super(MessageChannel, self).basic_get(*args, **kwargs)
        return r if isinstance(r, Message) else None

    def confirm_select(self, nowait=False):
        """Turn on RabbitMQ's publisher acknowledgements.

        See http://www.rabbitmq.com/confirms.html

        There are two things that need to be done:

        * Swap basic_publish to a version that blocks waiting for the
          corresponding ack.

        * Support nowait (because this method blocks or not depending on that
          argument)

        """
        self.basic_publish = self.basic_publish_with_confirm

        if nowait:
            super(MessageChannel, self).confirm_select(nowait=nowait)
        else:
            # Send frame directly, as no callback will be received
            self._send(spec.FrameConfirmSelect(1))

    def basic_publish_with_confirm(self, exchange='', routing_key='', mandatory=False, immediate=False, headers={}, body=''):
        """Version of basic publish that blocks waiting for confirm."""
        method = super(MessageChannel, self).basic_publish
        self.clear_returned()
        ret = self._call_sync(method, ('basic.ack', 'basic.nack'), exchange, routing_key, mandatory, immediate, headers, body)
        if ret.name == 'basic.nack':
            raise exceptions.PublishFailed(ret)
        if mandatory or immediate:
            self.check_returned()
        return ret
