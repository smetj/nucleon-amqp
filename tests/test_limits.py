import random

from gevent.pool import Group
from base import TestCase, declares_queues

from nucleon.amqp import Connection
from nucleon.amqp.spec import FrameQueueDeclareOk


qname = 'test%s' % (random.random(),)
queues = [qname + '.%s' % (i,) for i in xrange(100)]


class TestLimits(TestCase):
    @declares_queues(*queues)
    def test_parallel_queue_declare(self):

        conn = Connection(self.amqp_url)
        conn.connect()

        channel = conn.allocate_channel()

        def declare(name):
            return channel.queue_declare(queue=name)

        g = Group()
        res = g.map(declare, queues)

        assert len(res) == len(queues)
        assert all(isinstance(r, FrameQueueDeclareOk) for r in res)
