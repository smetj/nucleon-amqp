from base import TestCase, connect, declares_queues
from nose.plugins.skip import SkipTest


class TestCancel(TestCase):
    @declares_queues('test-cancel')
    @connect
    def test_cancel_single(self, channel):
        channel.queue_declare(queue='test-cancel')

        channel.basic_publish(exchange='', routing_key='test-cancel',
                                       body='a')

        channel.basic_qos(prefetch_count=1)
        queue = channel.basic_consume(queue='test-cancel')
        result = queue.get()
        self.assertEqual(result.body, 'a')
        result.ack()

        channel.basic_cancel(queue.consumer_tag)
        assert queue.consumer_tag not in channel.consumers

    @connect
    def test_remote_cancel(self, channel):
        """Test handling a basic.cancel message from the server.

        This is a RabbitMQ extension.
        """

        # Check we have the required capability - skip the test otherwise
        capabilities = channel.connection.server_properties.get(
            'capabilities', {})
        if not capabilities.get('consumer_cancel_notify', False):
            raise SkipTest(
                'Server does not support consumer cancel notifications'
            )

        channel.queue_declare(queue='test-remote-cancel')
        channel.basic_publish(exchange='', routing_key='test-remote-cancel',
                                       body='a')

        queue = channel.basic_consume(queue='test-remote-cancel', no_ack=True)
        result = queue.get()
        self.assertEqual(result.body, 'a')

        channel.queue_delete(queue='test-remote-cancel')

        # Make sure the consumer died:
        result = channel.queue_declare(queue='test-remote-cancel')
        self.assertEqual(result.consumer_count, 0)

        assert queue.consumer_tag not in channel.consumers


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
