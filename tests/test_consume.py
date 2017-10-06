from nucleon.amqp import Connection
from nucleon.amqp.exceptions import ResourceLocked, AccessRefused

from base import TestCase, declares_queues


class TestBasicConsumeMulti(TestCase):
    @declares_queues('exclusive-queue')
    def test_exclusive_queue(self):
        conn = Connection(self.amqp_url)

        with conn.channel() as channel:
            channel.queue_declare(queue='exclusive-queue', exclusive=True)

        with conn.channel() as channel:
            with self.assertRaises(ResourceLocked):
                channel.queue_declare(queue='exclusive-queue')

    @declares_queues('exclusive-consume')
    def test_exclusive_consume(self):
        """Testing exclusive basic_consume"""
        conn = Connection(self.amqp_url)

        channel = conn.allocate_channel()
        channel.queue_declare('exclusive-consume')
        channel.basic_consume(queue='exclusive-consume', exclusive=True)

        channel2 = conn.allocate_channel()
        with self.assertRaises(AccessRefused):
            channel2.basic_consume(queue='exclusive-consume')


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

