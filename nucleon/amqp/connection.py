import sys
import errno
import struct
import weakref
from functools import partial

from collections import defaultdict
from contextlib import contextmanager

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue, Empty
from gevent.lock import RLock
from gevent import socket

from .urls import parse_amqp_url

from .exceptions import ConnectionError, AMQPHardError, ChannelError
from . import spec
from .buffers import BufferedReader, DecodeBuffer
from .channels import StartChannel, MessageChannel


FRAME_HEADER = struct.Struct('!BHI')


STATE_DISCONNECTED = 0
STATE_CONNECTING = 1
STATE_CONNECTED = 2
STATE_DISCONNECTING = 3


class Dispatcher(object):
    """Generic event dispatcher."""
    def __init__(self):
        self.handlers = defaultdict(list)

    def register(self, event, callback):
        """Register an callback for event."""
        self.handlers[event].append(callback)

    def unregister(self, event, callback):
        """Unregister a previously registered callback"""
        self.handlers[event].remove(callback)

    def fire(self, event, *args, **kwargs):
        """Fire an event synchronously.

        Handlers will be executed one-by-one in the current greenlet.
        """
        for handler in self.handlers[event][:]:
            try:
                handler(*args, **kwargs)
            except Exception:
                import traceback
                traceback.print_exc()
                continue

    def fire_async(self, event, *args, **kwargs):
        """Fire an event asynchronously.

        Handlers will be executed in parallel in different greenlets.
        """
        for handler in self.handlers[event][:]:
            gevent.spawn(handler, *args, **kwargs)


class BaseConnection(object):
    """A connection to an AMQP server.

    This class deals with establishing and reconnecting errors, and routes
    messages received off the wire to handlers registered with the
    corresponding channel. It also handles sending and receiving heartbeat
    frames to detect when the connection has been lost.

    AMQP sends connection-level errors (connection.close) that cause the
    connection to close; to support this we dispatch such errors to all
    channels.

    """
    frame_max = 131072   # adjusted by Tune frame
    channel_max = 65535  # adjusted by Tune method
    MAX_SEND_QUEUE = 32  # frames
    server_properties = None  # properties and capabilities sent by the remote connection

    heartbeat = 0  # seconds between heartbeats. 0 means off.
                   # NB. heartbeats don't start until they are negotiated

    def __init__(self, amqp_url='amqp:///', heartbeat=30, debug=False):
        super(BaseConnection, self).__init__()

        self.channel_id = 0
        self.channels = {}
        self.connect_lock = RLock()
        self.channels_lock = RLock()
        self.queue = None
        self.state = STATE_DISCONNECTED
        self.disconnect_event = Event()
        self.debug = debug

        # Negotiate for heartbeats
        self.requested_heartbeat = heartbeat

        (self.username, self.password, self.vhost, self.host, self.port) = \
            parse_amqp_url(str(amqp_url))

    def allocate_channel(self):
        """Create a new channel."""

        self.connect()  # This is a no-op if we're already connected

        with self.channels_lock:
            for i in xrange(self.channel_max):
                self.channel_id = self.channel_id % (self.channel_max - 1) + 1
                if self.channel_id not in self.channels:
                    break
            else:
                raise ChannelError("No available channels!")

            id = self.channel_id
            chan = MessageChannel(self, id)
            self.channels[id] = chan
            chan.channel_open()
        return chan

    def _remove_channel(self, id):
        """Remove a channel (presumably because it has closed.)"""
        with self.channels_lock:
            del(self.channels[id])

    def connected(self):
        return self.state == STATE_CONNECTED

    def connect(self):
        """Open the connection to the server.

        This method blocks until the connection has been opened and handshaking
        is complete.

        If connection fails, an exception will be raised instead.

        """

        # Don't bother acquiring the lock if we are already connected
        if self.connected():
            return
        with self.connect_lock:
            # Check again now we've acquired the lock, as the previous holder
            # of the lock would typically have completed the connection.
            if self.connected():
                return

            self.disconnect_event.clear()
            self.connected_event = AsyncResult()
            self._connect()
            v = self.connected_event.wait()  # Block until the connection is
                                             # properly ready

            # FFS, AsyncResult.set_exception doesn't work in gevent 1.0b4
            # so we just use normal setting, but with exception types
            if isinstance(v, Exception):
                raise v

    def _connect(self):
        """Connect to the remote server and start reader/writer greenlets."""

        # NB. this method isn't safe to call directly; must only be called
        # via connect() which adds the necessary locking

        self.state = STATE_CONNECTING

        try:
            try:
                addrinfo = socket.getaddrinfo(self.host, self.port, socket.AF_INET6, socket.SOCK_STREAM)
            except socket.gaierror:
                addrinfo = socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)

            (family, socktype, proto, canonname, sockaddr) = addrinfo[0]
            self.sock = socket.socket(family, socktype, proto)
            self.sock.connect(sockaddr)
        except:
            self.state = STATE_DISCONNECTED
            raise

        # Set up channel state
        self.channel_id = 1
        self.channels = {}

        # Set up connection greenlets
        self.queue = Queue(self.MAX_SEND_QUEUE)
        reader = gevent.spawn(self.do_read, self.sock)
        writer = gevent.spawn(self.do_write, self.sock, self.queue)
        reader.link(lambda reader: writer.kill())
        writer.link(lambda writer: reader.kill())
        self.reader = reader
        self.writer = writer

    def _on_error(self, exc):
        """Dispatch a connection error to all channels."""
        for id, channel in self.channels.items():
            channel.on_error(exc)
        self.fire_async('error')

    def _on_connect(self):
        """Called when the connection is fully open."""
        self.state = STATE_CONNECTED
        self.connected_event.set("Connected!")
        self.fire_async('connect')

    def _on_abnormal_disconnect(self, exc):
        """Called when the connection has been abnormally disconnected."""
        self.state = STATE_DISCONNECTED
        try:
            self._on_error(exc)
        except Exception:
            pass
        while True:
            try:
                self.connect()
            except Exception:
                gevent.sleep(5)
            else:
                break

    def _on_normal_disconnect(self):
        """Called when the connection has closed."""
        self.state = STATE_DISCONNECTED
        self._on_error(ConnectionError("Connection closed."))
        self.queue.put(None)
        self.disconnect_event.set()

    def do_read(self, sock):
        """Run a reader greenlet.

        This method will read a preamble then loop forever reading frames off
        the wire and dispatch them to channels.

        """
        try:
            reader = BufferedReader(sock)

            TIMEOUT_EXC = ConnectionError('Heartbeat timeout')
            while True:
                if self.heartbeat:
                    t = gevent.Timeout(
                                seconds=self.heartbeat * 2,
                                exception=TIMEOUT_EXC)
                    t.start()
                else:
                    t = None

                frame_header = reader.read(FRAME_HEADER.size)
                frame_type, channel, size = \
                    FRAME_HEADER.unpack(frame_header)

                payload = reader.read(size + 1)

                if t is not None:
                    t.cancel()

                if payload[-1] != '\xCE':
                    raise ConnectionError('Received invalid frame data')

                if self.debug:
                    self._debug_print('s->c', frame_header + payload)

                buffer = DecodeBuffer(payload)
                if frame_type == 0x01:  # Method frame
                    method_id, = buffer.read('!I')
                    frame = spec.METHODS[method_id].decode(buffer)
                    self.inbound_method(channel, frame)
                elif frame_type == 0x02:  # header frame
                    class_id, body_size = buffer.read('!HxxQ')
                    props = spec.PROPS[class_id](buffer)
                    self.inbound_props(channel, body_size, props)
                elif frame_type == 0x03:  # body frame
                    self.inbound_body(channel, payload[:-1])
                elif frame_type in [0x04, 0x08]:
                    # Heartbeat frame
                    #
                    # Catch it as both 0x04 and 0x08 - see
                    # http://www.rabbitmq.com/amqp-0-9-1-errata.html#section_29
                    #
                    # We don't need to handle this specifically - it's already
                    # covered by the read timeout above.
                    pass
                else:
                    raise ConnectionError("Unknown frame type")
        except (gevent.GreenletExit, Exception) as e:
            self.connected_event.set(e)

            if self.state in [STATE_CONNECTED, STATE_CONNECTING]:
                # Spawn a new greenlet to run the reconnect loop
                gevent.spawn(self._on_abnormal_disconnect, e)
            else:
                self.state = STATE_DISCONNECTED

    def inbound_method(self, channel, frame):
        """Dispatch an inbound method."""
        try:
            c = self.channels[channel]
        except KeyError:
            if frame.name == 'connection.start':
                c = StartChannel(self, channel)
                self.channels[channel] = c
            else:
                return
        c._on_method(frame)

    def inbound_props(self, channel, body_size, props):
        """Dispatch an inbound properties frame."""
        try:
            c = self.channels[channel]
        except KeyError:
            return
        c._on_headers(body_size, props)

    def inbound_body(self, channel, payload):
        """Dispatch an inbound body frame."""
        try:
            c = self.channels[channel]
        except KeyError:
            return
        c._on_body(payload)

    def do_write(self, sock, queue):
        """Run a writer greenlet.

        This greenlet will loop until the connection closes, writing frames
        from the queue.

        """
        # Write the protocol header
        sock.sendall(spec.PREAMBLE)

        # Enter a send loop
        while True:
            if self.heartbeat:
                try:
                    msg = queue.get(timeout=self.heartbeat)
                except Empty:
                    # Send heartbeat
                    sock.sendall(
                        FRAME_HEADER.pack(0x08, 0, 0) + '\xCE'
                    )
                    continue
            else:
                msg = queue.get()

            if msg is None:
                return
            if self.debug:
                self._debug_print('s<-c', msg)
            sock.sendall(msg)

    def _debug_print(self, direction, msg):
        """Decode a frame and print it for debugging."""
        try:
            # Print method, for debugging
            type, channel, size = FRAME_HEADER.unpack_from(msg)
            if type == 1:
                method_id = struct.unpack_from('!I', msg, FRAME_HEADER.size)[0]
                print direction, spec.METHODS[method_id].name
            else:
                print direction, {
                    2: '[headers %d bytes]',
                    3: '[payload %d bytes]',
                    4: '[heartbeat %d bytes]',
                }[type] % size
        except Exception:
            import traceback
            traceback.print_exc()

    def _send_frames(self, channel, frames):
        """Send a sequence of frames on channel.

        Each frame will be put onto a queue for the writer to write, which
        could cause the calling greenlet to block if the queue is full.

        This should cause large outgoing messages to be spliced together so
        that no caller is starved of service while a large message is sending.

        """
        assert channel in self.channels
        for type, payload in frames:
            fdata = ''.join([
                FRAME_HEADER.pack(type, channel, len(payload)),
                payload,
                '\xCE'
            ])
            self.queue.put(fdata)

    def _tune(self, frame_max, channel_max, heartbeat=0):
        """Adjust connection parameters.

        Called in response to negotiation with the server.
        """
        frame_max = frame_max if frame_max != 0 else 2**19
        # limit the maximum frame size, to ensure messages are multiplexed
        self.frame_max = min(131072, frame_max)
        self.channel_max = channel_max if channel_max > 0 else 65535

        if heartbeat:
            self.heartbeat = min(self.requested_heartbeat, heartbeat)
        else:
            self.heartbeat = self.requested_heartbeat

    def close(self, block=True, timeout=2):
        if self.state in [STATE_CONNECTED, STATE_CONNECTING]:
            self.state = STATE_DISCONNECTING
            if block:
                self.channels[0].connection_close()
                self.writer.join(timeout=timeout)
            else:
                self.channels[0]._send(
                    spec.FrameConnectionClose(200, '', 0, 0)
                )

    def set_dispatcher(self, dispatcher):
        """Set the event dispatcher.

        BaseConnection holds a weak ref to its dispatcher so that our
        connection threads don't keep it alive.

        """
        self._dispatcher = weakref.ref(dispatcher)

    def get_dispatcher(self):
        """Get the event dispatcher or return None if there is no dispatcher."""
        if self._dispatcher:
            return self._dispatcher()

    dispatcher = property(get_dispatcher, set_dispatcher)

    def fire(self, event, *args, **kwargs):
        """Fire an event using the dispatcher."""
        d = self.dispatcher
        if d:
            return self.dispatcher.fire(event, *args, **kwargs)

    def fire_async(self, event, *args, **kwargs):
        """Asynchronously fire an event using the dispatcher."""
        d = self.dispatcher
        if d:
            return self.dispatcher.fire_async(event, *args, **kwargs)


class Connection(Dispatcher):
    """The public interface to an AMQP connection.

    The raw mechanics of running an AMQP connection are provided elsewhere;
    this class adds connection-level semantics, such as a context manager to
    to allocate and automatically close a channel.

    """
    def __init__(self, amqp_url='amqp:///', heartbeat=30, debug=False):
        super(Connection, self).__init__()
        self.connection = BaseConnection(
            amqp_url=amqp_url,
            heartbeat=heartbeat,
            debug=debug
        )
        self.connection.set_dispatcher(self)

    @property
    def server_properties(self):
        """Properties and capabilities sent by the broker."""
        return self.connection.server_properties

    def connect(self):
        """Open the connection to the server.

        This method blocks until the connection has been opened and handshaking
        is complete.

        If connection fails, an exception will be raised instead.

        """
        self.connection.connect()

    def __del__(self):
        # Destructors cannot block, so we spawn a new greenlet to "run"
        # the disconnection sequence
        gevent.spawn(self.connection.close, block=False)

    def close(self):
        """Disconnect the connection."""
        self.connection.close()

    def allocate_channel(self):
        """Allocate a channel.

        The caller is responsible for closing the channel if necessary.
        """
        return self.connection.allocate_channel()

    def on_error(self, handler):
        """Register a handler to be called on connection error."""
        self.register('error', partial(handler, self))
        return handler

    def on_connect(self, handler):
        "Register handler to be called when the connection is (re)connected."
        self.register('connect', partial(handler, self))
        return handler

    @contextmanager
    def channel(self):
        """Context manager to acquire a channel.

        The channel will be closed when the context is exited.
        """
        channel = self.allocate_channel()
        try:
            yield channel
            channel.check_returned()
        finally:
            channel.close()

    def join(self):
        """Block until close() has been called and has returned.
        """
        self.connection.disconnect_event.wait()
