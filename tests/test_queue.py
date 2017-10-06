from __future__ import with_statement
import gevent

from nucleon.amqp import Connection
from nucleon.amqp.exceptions import PreconditionFailed, NotFound

from base import TestCase, declares_queues, declares_exchanges


class TestQueue(TestCase):
    def test_queue_declare(self):
        """We can declare an autodelete queue."""
        conn = Connection(self.amqp_url)
        with conn.channel() as channel:
            resp = channel.queue_declare(auto_delete=True)
        conn.close()

        # The queue intentionally left hanging. Should be autoremoved.
        # Yes, no assertion here, we don't want to wait for 5 seconds.

    @declares_queues('test-redeclare-queue')
    def test_queue_redeclare(self):
        """We can redeclare a queue only if we don't change its settings."""
        qname = 'test-redeclare-queue'

        conn = Connection(self.amqp_url)

        with conn.channel() as channel:
            channel.queue_declare(queue=qname, auto_delete=False)

            # Test can redeclare auto_delete queue
            channel.queue_declare(queue=qname, auto_delete=False)

            with self.assertRaises(PreconditionFailed):
                channel.queue_declare(queue=qname, auto_delete=True)

    @declares_queues('test-redeclare-queue-args')
    def test_queue_redeclare_args(self):
        """We cannot redeclare a queue if we change its arguments."""
        qname = 'test-redeclare-queue-args'

        conn = Connection(self.amqp_url)
        conn.connect()

        with conn.channel() as channel:
            channel.queue_declare(queue=qname, arguments={})

            with self.assertRaises(PreconditionFailed):
                channel.queue_declare(
                    queue=qname,
                    arguments={'x-expires': 101}
                )

    def test_queue_delete_not_found(self):
        """NotFound is raised if we delete a queue that doesn't exist."""
        conn = Connection(self.amqp_url)

        with conn.channel() as channel:
            with self.assertRaises(NotFound):
                channel.queue_delete(queue='not_existing_queue')

    @declares_queues('test-queue-bind')
    @declares_exchanges('test-queue-bind')
    def test_queue_bind(self):
        "We can bind and unbind, and the queue receives messages when bound."
        qname = 'test-queue-bind'

        conn = Connection(self.amqp_url)

        with conn.channel() as channel:
            channel.queue_declare(queue=qname)
            channel.exchange_declare(exchange=qname, type='direct')

            channel.basic_publish(exchange=qname, routing_key=qname, body='a')

            channel.queue_bind(exchange=qname, queue=qname, routing_key=qname)

            channel.basic_publish(exchange=qname, routing_key=qname, body='b')

            channel.queue_unbind(exchange=qname, queue=qname, routing_key=qname)

            channel.basic_publish(exchange=qname, routing_key=qname, body='c')

            msg = channel.basic_get(queue=qname)
            self.assertEquals(msg.body, 'b')
            self.assertEquals(msg['message_count'], 0)
