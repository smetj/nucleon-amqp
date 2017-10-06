import gevent
from gevent import socket
from gevent.queue import Queue

from nucleon.amqp.exceptions import NotFound
from nucleon.amqp.urls import parse_amqp_url, make_amqp_url
from nucleon.amqp.connection import Connection, ConnectionError

import base
from base import declares_queues, declares_exchanges


class TestConnection(base.TestCase):
    def test_broken_url(self):
        client = Connection('amqp://does.not.resolve/')
        with self.assertRaises(socket.error):
            client.connect()

    def test_connection_refused(self):
        client = Connection('amqp://127.0.0.1:15243/')
        with self.assertRaises(socket.error):
            client.connect()

    def test_parse_url(self):
        (username, password, vhost, host, port) = \
            parse_amqp_url(self.amqp_url)

    # The following tests take 3 seconds each, due to Rabbit.
    def test_wrong_user(self):
        (username, password, vhost, host, port) = \
            parse_amqp_url(self.amqp_url)

        url = make_amqp_url(
            username=username,
            password='wrongpass',
            host=host,
            port=port,
            vhost=vhost
        )

        client = Connection(url)
        with self.assertRaises(ConnectionError):
            client.connect()

    @declares_queues('banana123')
    def test_connection_close_on_del(self):
        """The connection is automatically closed when no longer referenced.

        We test this through the medium of auto-delete queues; once the
        connection is closed the server should delete the auto-delete queue.

        We test that the auto-delete queue has been deleted - if this is the
        case the connection must have closed.

        """

        conn = Connection(self.amqp_url)
        conn.connect()
        channel = conn.allocate_channel()
        channel.queue_declare(queue='banana123', auto_delete=True)
        channel.basic_consume(queue='banana123')
        del conn
        del channel

        # Assert that the auto-delete queue has been deleted
        conn = Connection(self.amqp_url)
        with self.assertRaises(NotFound):
            with conn.channel() as channel:
                channel.queue_declare(queue='banana123', passive=True)

    @declares_queues('just.connected')
    def test_on_connect_handler(self):
        """on_connect handlers are called after we connect."""
        conn = Connection(self.amqp_url)

        @conn.on_connect
        def on_connect(conn):
            with conn.channel() as channel:
                channel.queue_declare(queue='just.connected')
                channel.basic_publish(routing_key='just.connected', body='hello')

        conn.connect()
        gevent.sleep(1)
        with conn.channel() as channel:
            message = channel.basic_get(queue='just.connected', no_ack=True)
            assert message is not None and message.body == 'hello'

    @declares_queues('reconnect-test')
    def test_reconnect(self):
        """Reconnect to the server if the connection is lost.

        The test runs as follows:

        1. Connect to AMQP. This triggers the initial setup and starts
           consuming. The consumer is deliberately slow, accepting only 5
           messages a second.
        2. Publish 10 messages.
        3. At a time that we expect to be partway through receiving messages,
           shut down the connection.
        4. Wait for all 10 messages to be received.

        The test fails if we time out in step 4.

        """
        conn = Connection(self.amqp_url)
        q = Queue()

        def on_message(message):
            "Slow consumer, receive only one message at a time"
            if isinstance(message, Exception):
                return
            gevent.sleep(0.2)
            q.put(message.body)
            message.ack()

        @conn.on_connect
        def on_connect(conn):
            channel = conn.allocate_channel()
            # Tell the server to send only one message at a time
            channel.basic_qos(prefetch_count=1)
            channel.queue_declare(queue='reconnect-test')
            channel.basic_consume(queue='reconnect-test', callback=on_message)

        def publish():
            """publish 10 messages"""
            with conn.channel() as channel:
                channel.tx_select()
                for i in range(10):
                    channel.basic_publish(
                        routing_key='reconnect-test',
                        body=str(i)
                    )
                channel.tx_commit()

        def kill_conn():
            """Kill the connection (in an effectively ungraceful way)"""
            conn.connection.sock.shutdown(socket.SHUT_RDWR)

        def receive_all():
            """Block until all 10 messages have been received"""
            expecting = set(range(10))
            while expecting:
                msg = q.get()
                expecting.discard(int(msg))
                print "received", msg

        conn.connect()
        gevent.spawn_later(0.5, publish)
        gevent.spawn_later(1.5, kill_conn)

        # Count in 10 messages
        with gevent.Timeout(10):
            receive_all()


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

