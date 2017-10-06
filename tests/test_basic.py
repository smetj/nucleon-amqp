from __future__ import with_statement
from nose.plugins.skip import SkipTest

from nucleon.amqp import Connection
from nucleon.amqp import exceptions

from gevent.queue import Queue, Empty

import base
from base import declares_queues, declares_exchanges

from utils import with_timeout


class TestBasic(base.TestCase):
    def test_consume_queue(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)
            channel.basic_publish(
                exchange='',
                routing_key=self.name,
                body=self.msg
            )

            queue = channel.basic_consume(queue=self.name, no_ack=True)
            result = queue.get()

            self.assertEqual(result.body, self.msg)
            channel.queue_delete(queue=self.name)

    def test_consume_callback(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)
            channel.basic_publish(
                exchange='',
                routing_key=self.name,
                body=self.msg
            )

            queue = Queue()
            channel.basic_consume(callback=queue.put, queue=self.name, no_ack=True)
            result = queue.get()

            self.assertEqual(result.body, self.msg)
            channel.queue_delete(queue=self.name)

    def test_purge(self):
        client = Connection(self.amqp_url)
        client.connect()
        with client.channel() as channel:
            channel.queue_declare(queue=self.name)
            channel.basic_publish(
                exchange='',
                routing_key=self.name,
                body=self.msg
            )
            r = channel.queue_purge(queue=self.name)
            self.assertEqual(r.message_count, 1)

            r = channel.queue_purge(queue=self.name)
            self.assertEqual(r.message_count, 0)

            channel.queue_delete(queue=self.name)

    def test_basic_get_ack(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)

            for i in range(4):
                channel.basic_publish(exchange='', routing_key=self.name,
                                               body=self.msg + str(i))

            msgs = []
            for i in range(4):
                result = channel.basic_get(queue=self.name)
                self.assertEqual(result.body, self.msg + str(i))
                self.assertEqual(result.redelivered, False)
                msgs.append(result)

            result = channel.basic_get(queue=self.name)
            self.assertTrue(result is None)

            channel.queue_delete(queue=self.name)

    def test_basic_publish_bad_exchange_tx(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            with self.assertRaises(exceptions.NotFound):
                channel.tx_select()
                channel.basic_publish(
                    exchange='invalid_exchange',
                    routing_key='xxx', body='')
                channel.tx_commit()

    def test_basic_publish_bad_exchange_publisher_acks(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.confirm_select()
            with self.assertRaises(exceptions.NotFound):
                channel.basic_publish(
                    exchange='invalid_exchange',
                    routing_key='xxx', body='')

    @with_timeout
    def test_basic_return(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.confirm_select()
            with self.assertRaises(exceptions.MessageReturnedNoRoute):
                channel.basic_publish(exchange='', routing_key=self.name,
                                               mandatory=True, body='')

    @with_timeout
    def test_basic_return2(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.confirm_select()
            channel.queue_declare(queue=self.name)

            channel.basic_publish(exchange='', routing_key=self.name,
                                           mandatory=True, body='')

            with self.assertRaises(exceptions.MessageReturnedNoConsumers):
                channel.basic_publish(exchange='', routing_key=self.name,
                                           immediate=True, body='')

        with client.channel() as channel:
            channel.queue_delete(queue=self.name)

    def test_persistent(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)

            channel.basic_publish(exchange='', routing_key=self.name,
                                           body=self.msg)  # persistence=default

            channel.basic_publish(exchange='', routing_key=self.name,
                                           body=self.msg,
                                           headers={'delivery_mode': 2})

            channel.basic_publish(exchange='', routing_key=self.name,
                                           body=self.msg,
                                           headers={'delivery_mode': 1})

            result = channel.basic_get(queue=self.name, no_ack=True)
            self.assertTrue('delivery_mode' not in result.headers)

            result = channel.basic_get(queue=self.name, no_ack=True)
            self.assertTrue('delivery_mode' in result.headers)
            self.assertEquals(result.headers['delivery_mode'], 2)

            result = channel.basic_get(queue=self.name, no_ack=True)
            self.assertTrue('delivery_mode' in result.headers)
            self.assertEquals(result.headers['delivery_mode'], 1)

            channel.queue_delete(queue=self.name)

    def test_basic_reject(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)

            channel.basic_publish(exchange='', routing_key=self.name,
                                           body='a')

            r = channel.basic_get(queue=self.name)
            self.assertEqual(r.body, 'a')
            self.assertTrue(not r.redelivered)
            channel.basic_reject(r.delivery_tag)

            r = channel.basic_get(queue=self.name)
            self.assertEqual(r.body, 'a')
            self.assertTrue(r.redelivered)

            channel.queue_delete(queue=self.name)

    def test_basic_reject_no_requeue(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)
            channel.basic_publish(exchange='', routing_key=self.name,
                                           body='a')

            r = channel.basic_get(queue=self.name)
            self.assertEqual(r.body, 'a')
            self.assertTrue(not r.redelivered)
            channel.basic_reject(r.delivery_tag, requeue=False)

            r = channel.basic_get(queue=self.name)
            self.assertTrue(r is None)

            channel.queue_delete(queue=self.name)

    @declares_exchanges('dead-letter')
    @declares_queues('test-dead-letter')
    def test_basic_reject_dead_letter_exchange(self):
        client = Connection(self.amqp_url)
        client.connect()

        product = client.server_properties.get('product')
        vstring = client.server_properties.get('version', '')
        version = tuple(int(v) for v in vstring.split('.') if v.isdigit())

        if product != 'RabbitMQ' or version < (2, 8, 0):
            raise SkipTest(
                "Dead letter exchanges are only supported in RabbitMQ 2.8.0 "
                "and later."
            )

        with client.channel() as channel:
            # Set up the dead letter exchange
            channel.exchange_declare(exchange='dead-letter', type='fanout')
            queue = channel.queue_declare(exclusive=True, auto_delete=True)
            dlxqname = queue.queue
            channel.queue_bind(queue=dlxqname, exchange='dead-letter')
            dead = channel.basic_consume(queue=dlxqname)

            # Declare a new queue and publish a message to it
            channel.queue_declare(
                queue='test-dead-letter',
                arguments={'x-dead-letter-exchange': 'dead-letter'}
            )
            channel.basic_publish(
                exchange='',
                routing_key='test-dead-letter',
                body='a'
            )

            # Get the message and reject it
            r = channel.basic_get(queue='test-dead-letter')
            self.assertEqual(r.body, 'a')
            self.assertTrue(not r.redelivered)
            channel.basic_reject(r.delivery_tag, requeue=False)

            # Check that we received it via the dead letter queue
            r = dead.get(timeout=5)
            assert r is not None
            self.assertEqual(r.body, 'a')
            self.assertEqual(r.headers['x-death'][0]['reason'], 'rejected')
            self.assertTrue(not r.redelivered)

            dead.cancel()

    def test_properties(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.queue_declare(queue=self.name)

            headers = {
                "content_type": 'a',
                "content_encoding": 'b',
                #"headers":
                "delivery_mode": 2,
                "priority": 1,
                "correlation_id": 'd',
                "reply_to": 'e',
                "expiration": 'f',
                "message_id": 'g',
                "timestamp": 1,
                "type_": 'h',
                "user_id": 'guest',  # that one needs to match real user
                "app_id": 'j',
                "cluster_id": 'k',
                "custom": 'l',
                "blah2": [True, 1, -1, 4611686018427387904L,
                          -4611686018427387904L, [1,2,3,4, {"a":"b", "c":[]}]],
                }

            channel.basic_publish(exchange='', routing_key=self.name,
                                     body='a', headers=headers.copy())

            r = channel.basic_get(queue=self.name, no_ack=True)
            self.assertEqual(r.body, 'a')
            self.assertEqual(headers, r.headers)

            channel.queue_delete(queue=self.name)

    def test_basic_ack_fail(self):
        client = Connection(self.amqp_url)
        client.connect()
        with client.channel() as channel:
            channel.queue_declare(queue=self.name)
            channel.basic_publish(exchange='', routing_key=self.name,
                                           body='a')

            queue = channel.basic_consume(queue=self.name)

            with self.assertRaises(exceptions.PreconditionFailed):
                channel.tx_select()
                channel.basic_ack(999)
                channel.tx_commit()

        with client.channel() as channel:
            queue = channel.basic_consume(queue=self.name)

            result = queue.get()

            channel.tx_select()
            channel.basic_ack(result.delivery_tag)
            channel.tx_commit()

            with self.assertRaises(exceptions.PreconditionFailed):
                channel.basic_ack(result.delivery_tag)
                channel.tx_commit()

        with client.channel() as channel:
            channel.queue_delete(queue=self.name)

    def test_basic_cancel(self):
        client = Connection(self.amqp_url)
        client.connect()
        with client.channel() as channel:

            channel.queue_declare(queue=self.name)

            for i in range(2):
                channel.basic_publish(exchange='', routing_key=self.name,
                                               body='a')

            queue = channel.basic_consume(queue=self.name)

            for i in range(2):
                msg1 = queue.get()
                self.assertEqual(msg1.body, 'a')
                channel.basic_ack(msg1.delivery_tag)

            result = channel.basic_cancel(queue.consumer_tag)

            self.assertEqual(result.consumer_tag, queue.consumer_tag)

            channel.basic_publish(exchange='', routing_key=self.name,
                                           body='b')

            with self.assertRaises(Empty):
                queue.get(timeout=0.5)

            channel.queue_delete(queue=self.name)

    def test_close(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            channel.channel_close()

    def test_basic_consume_fail(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            with self.assertRaises(exceptions.NotFound):
                channel.basic_consume(queue='bad_q_name')

    def test_broken_ack_on_close(self):
        client = Connection(self.amqp_url)
        client.connect()

        with client.channel() as channel:
            decl = channel.queue_declare()
            qname = decl.queue

            channel.basic_publish(exchange='', routing_key=qname, body='a')

            r = channel.basic_get(queue=qname)
            self.assertEquals(r.body, 'a')

            channel.queue_delete(queue=qname)

        client.close()

    @base.connect
    def test_basic_qos(self, channel):
        channel.queue_declare(queue=self.name)

        for msg in 'abc':
            channel.basic_publish(
                exchange='',
                routing_key=self.name,
                body=msg
            )

        channel.basic_qos(prefetch_count=1)
        queue = channel.basic_consume(queue=self.name)
        result = queue.get(timeout=0.1)
        self.assertEqual(result.body, 'a')

        with self.assertRaises(Empty):
            queue.get(timeout=0.1)

        # Now adjust QoS
        channel.basic_qos(prefetch_count=2)
        result = queue.get(timeout=0.1)
        self.assertEqual(result.body, 'b')

        with self.assertRaises(Empty):
            queue.get(timeout=0.1)

        channel.queue_delete(queue=self.name)



if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
