import gevent
from gevent.pool import Pool

from nucleon.amqp import Connection
from nucleon.amqp.exceptions import ResourceLocked, AccessRefused

from base import TestCase, declares_queues
from utils import with_timeout


class TestChannels(TestCase):
    @declares_queues('large-messages')
    @with_timeout(10)
    def test_channel_interleaving(self):
        """Messages published on different channels are interleaved.

        We test that if we try to publish a small message while a large message
        is uploading on a different channel, the small message will be received
        first.

        """
        conn = Connection(self.amqp_url)

        messages = [
            'ghjk',
            'asdf' * 10000000,
        ]

        def publish_message(msg):
            with conn.channel() as channel:
                channel.basic_publish(
                    routing_key='large-messages',
                    body=msg
                )

        channel = conn.allocate_channel()
        channel.queue_declare(queue='large-messages')
        q = channel.basic_consume(queue='large-messages')

        gevent.spawn(publish_message, messages[1])
        gevent.spawn_later(0.1, publish_message, messages[0])

        for i in range(2):
            msg = q.get()
            assert msg.body == messages[i]
            msg.ack()

    @declares_queues('large-messages')
    @with_timeout(10)
    def test_greenlet_no_interleaving(self):
        "Messages published at the same time and channel are not garbled."
        conn = Connection(self.amqp_url)

        messages = [
            'asdf' * 10000000,
            'ghjk' * 10000000
        ]

        def publish_message(msg):
            channel.basic_publish(
                routing_key='large-messages',
                body=msg
            )

        channel = conn.allocate_channel()
        channel.queue_declare(queue='large-messages')
        q = channel.basic_consume(queue='large-messages')

        pool = Pool()
        pool.map_async(publish_message, messages)

        for i in range(2):
            msg = q.get()
            try:
                messages.remove(msg.body)
            except ValueError:
                raise AssertionError("Received unknown message")
            msg.ack()


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
