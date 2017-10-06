from nose.tools import eq_, with_setup
import gevent
from gevent.queue import Queue
from functools import wraps

from nucleon.amqp import Connection

from base import AMQP_URL
from utils import with_timeout

conn = None


def amqp_setup():
    global conn
    conn = Connection(AMQP_URL)
    with conn.channel() as channel:
        channel.exchange_declare("unit_test_room")
        channel.queue_declare(queue='listener1')
        channel.queue_declare(queue='listener2')

        channel.queue_bind(queue="listener1",
            exchange="unit_test_room", routing_key="user1")

        channel.queue_bind(queue="listener2",
            exchange="unit_test_room", routing_key="user2")


def amqp_teardown():
    global conn
    conn.close()
    conn2 = Connection(AMQP_URL)
    with conn2.channel() as channel:
        channel.exchange_delete("unit_test_room")
    with conn2.channel() as channel:
        channel.queue_delete(queue='listener1')
    with conn2.channel() as channel:
        channel.queue_delete(queue='listener2')
    conn2.close()


def publish_message(conn, exchange, routing_key, body, callback=None):
    """
    Utility method to publish one message synchronously to given exchange
    """
    if callback:
        with conn.channel() as channel:
            channel.tx_select()
            channel.publish(exchange=exchange, routing_key=routing_key,
                body=body)
            channel.tx_confirm()
            callback()
    else:
        with conn.channel() as channel:
            return channel.publish(exchange=exchange, routing_key=routing_key,
                body=body)


def get_message(conn, queue, callback=None):
    """
    Utility method to get one message synchronously from given queue on given
    connection.
    """
    if callback:
        resp = conn.consume(queue=queue, callback=callback)
    else:
        resp = conn.consume(queue=queue)
    return resp



@with_setup(amqp_setup, amqp_teardown)
@with_timeout(5)
def test_sync_diff_connections():
    """We can pass messages between different AMQP connections."""
    # publish a message to the exchange
    message_body = 'test_sync_diff_connections message 2'
    with conn.channel() as channel:
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=message_body
        )

    conn2 = Connection(AMQP_URL)
    with conn2.channel() as channel:
        q = channel.basic_consume(queue='listener1')
        msg = q.get()
        # check that message was correctly published
        eq_(msg.body, message_body)

        # acknowledge the message
        channel.basic_ack(msg.delivery_tag)


@with_setup(amqp_setup, amqp_teardown)
@with_timeout(5)
def test_async_publish_consume():
    message_body = 'test_async_publish_consume message 1'
    with conn.channel() as channel:
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=message_body
        )

        recv_queue = Queue()

        def recv_callback(msg):
            recv_queue.put(msg)

        channel.basic_consume(queue='listener1', callback=recv_callback)
        resp = recv_queue.get()
        eq_(resp.body, message_body)
        resp.ack()


@with_setup(amqp_setup, amqp_teardown)
@with_timeout(5)
def test_async_multi_publish_consume():
    with conn.channel() as channel:
        # first message
        message_body = 'test_async_multi_publish_consume message 1'
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=message_body
        )

    recv_queue = Queue()
    rchannel = conn.allocate_channel()
    rchannel.basic_consume(queue='listener1', callback=recv_queue.put)

    resp = recv_queue.get()
    eq_(resp.body, message_body)
    resp.ack()

    assert recv_queue.empty()

    with conn.channel() as channel:
        # second message
        message_body = 'test_async_multi_publish_consume message 2'
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=message_body
        )

    resp = recv_queue.get()
    eq_(resp.body, message_body)
    resp.ack()


@with_setup(amqp_setup, amqp_teardown)
@with_timeout(10)
def test_multiple_publishers_same_connection():
    """
    Test locking. Publish multiple messages on the same connection
    in different threads.
    """
    messages = ['pub' + str(i) for i in range(5)]

    results = Queue()

    def recv_callback(result):
        result.ack()
        results.put(result.body)

    # register consumer with callback
    rchan = conn.allocate_channel()
    rchan.confirm_select()

    consume_promise = rchan.basic_consume(
        queue="listener1",
        callback=recv_callback
    )

    def publish(msg):
        with conn.channel() as channel:
            channel.basic_publish(
                exchange="unit_test_room",
                routing_key="user1",
                body=msg
            )

    # publish messages in different gevent threads
    publishers = []
    for msg in messages:
        publishers.append(gevent.spawn(publish, msg))

    gevent.joinall(publishers)

    # check that we got all messages back
    result_msgs = set()
    i = 0
    while i < len(messages):
        result = results.get(timeout=5)
        result_msgs.add(result)
        i += 1

    assert result_msgs == set(messages)
    rchan.basic_cancel(consume_promise.consumer_tag)


@with_setup(amqp_setup, amqp_teardown)
@with_timeout(5)
def test_callback_can_call_blocking_methods():
    """Test that a callback can call blocking methods

    The pitfall is that a callback is dispatched by a dispatcher greenlet, and
    if that greenlet blocks waiting for a result from its own connection, it
    won't be able to unblock itself.

    Therefore in this test we do a blocking publish call from a callback.

    Three messages are sent:

    - initial: triggers the callback
    - callback: causes the dispatcher to block holding the connection lock
    - outer: publish a message, in order to wait for the connection lock

    If callback blocks, outer will never complete. We give the whole test 5
    seconds to complete - if it is not complete within this time, we consider
    it hung.

    """
    channel = conn.allocate_channel()
    channel.confirm_select()

    def publish(message):
        """Publish helper - publish to the queue listener1."""
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=message
        )

    received = []

    def callback(result):
        """In the callback, publish to our own connection."""
        received.append(result.body)
        result.ack()
        if result.body != 'callback':
            result.reply(body='callback')

    timeout = gevent.Timeout(seconds=5)
    try:
        timeout.start()
        channel.basic_consume(queue='listener1', callback=callback)
        publish('initial')
        publish('outer')
        outer_publisher = gevent.spawn_later(1, publish, 'outer')
        outer_publisher.join()
    except gevent.Timeout:
        # We've deadlocked the connection - we should try to close it
        # But it probably won't close - so we kill everything instead
        gevent.killall([conn.dispatcher, conn.greenlet], block=True, timeout=2)
        try:
            channel.close()
        except Exception:
            pass

        raise AssertionError("Callback appears hung after 5 seconds.")
    finally:
        timeout.cancel()

    eq_(set(received), set(['initial', 'callback', 'outer']))


@with_setup(amqp_setup, amqp_teardown)
@with_timeout(10)
def test_cancel_consume():
    """Test that we can cancel consuming."""

    channel = conn.allocate_channel()
    for c in range(10):
        channel.basic_publish(
            exchange='unit_test_room',
            routing_key='user1',
            body=str(c)
        )

    results = []

    def callback(msg):
        results.append(int(msg.body))
        if msg.body == '5':
            msg.cancel_consume()
        msg.ack()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='listener1', callback=callback)
    gevent.sleep(3)  # wait for messages to be delivered

    eq_(results, [0, 1, 2, 3, 4, 5])

