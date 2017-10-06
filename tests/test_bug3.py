# https://github.com/majek/puka/issues/3
from __future__ import with_statement

import gevent
import random

import base
from utils import with_timeout

from nucleon.amqp import Connection


class TestBug3(base.TestCase):
    @with_timeout
    def test_bug3_wait(self):
        conn = Connection(self.amqp_url)
        qname = 'test-bug3-%s' % random.random()
        with conn.channel() as channel:
            channel.queue_declare(queue=qname)

            for i in range(3):
                channel.basic_publish(
                   routing_key=qname,
                   body='x'
                )

            q = channel.basic_consume(qname)
            for i in range(3):
                msg = q.get()
                channel.basic_ack(msg.delivery_tag)
                self.assertEqual(msg.body, 'x')

            self._epilogue(qname, 0)
        conn.close()

    def _epilogue(self, qname, expected):
        conn = Connection(self.amqp_url)
        with conn.channel() as channel:
            q = channel.queue_declare(queue=qname)
            try:
                self.assertEqual(q.message_count, expected)
            finally:
                channel.queue_delete(queue=qname)

    @with_timeout
    def test_bug3_loop(self):
        i = [0]

        conn = Connection(self.amqp_url)
        qname = 'test-bug3-%s' % random.random()

        def cb(msg):
            i[0] += 1
            msg.ack()
            self.assertEqual(msg.body, 'x')
            if i[0] == 3:
                conn.close()

        channel = conn.allocate_channel()
        channel.queue_declare(queue=qname)

        for _ in range(3):
            channel.basic_publish(
                routing_key=qname,
                body='x'
            )

        channel.basic_consume(queue=qname, callback=cb)

        conn.join()
        self._epilogue(qname, 0)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

