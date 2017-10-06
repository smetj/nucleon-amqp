import functools
import os
import random
import unittest_backport as unittest

from nucleon.amqp import Connection

AMQP_URL = os.getenv('AMQP_URL', 'amqp://guest:guest@blip.vm/')


class TestCase(unittest.TestCase):
    def setUp(self):
        self.name = 'test%s' % (random.random(),)
        self.name1 = 'test%s' % (random.random(),)
        self.name2 = 'test%s' % (random.random(),)
        self.msg = '%s' % (random.random(),)
        self.declared_queues = []
        self.declared_exchanges = []
        self.amqp_url = AMQP_URL

    def tearDown(self):
        conn = Connection(self.amqp_url)
        conn.connect()
        channel = conn.allocate_channel()
        for queue in self.declared_queues:
            try:
                channel.queue_delete(queue=queue)
            except Exception:
                channel = conn.allocate_channel()

        for exchange in self.declared_exchanges:
            try:
                channel.exchange_delete(exchange=exchange)
            except Exception:
                channel = conn.allocate_channel()
        conn.close()


def connect(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        client = Connection(self.amqp_url)
        client.connect()
        with client.channel() as channel:
            r = method(self, channel, *args, **kwargs)
        client.close()
        return r
    return wrapper


def declares_queues(*names):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            self.declared_queues.extend(names)
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def declares_exchanges(*names):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            self.declared_exchanges.extend(names)
            return func(self, *args, **kwargs)
        return wrapper
    return decorator
