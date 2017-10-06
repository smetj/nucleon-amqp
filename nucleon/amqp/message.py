class Message(object):
    """A wrapper for a received AMQP message.

    This class presents an API that allows messages to be conveniently
    consumed; typical operations can be performed in the callback by accessing
    properties and calling methods of the message object.

    """

    def __init__(self, channel, frame, headers, body):
        """Construct a message.

        `conn` is a PukaConnection that will be used to communicate with the
        source AMQP server. `result` is the puka Frame received.

        """
        self.channel = channel
        self._frame = frame
        self.headers = headers
        self.body = body

    def __getitem__(self, key):
        """Allow attributes to be read with subscript.

        This is for backwards compatibility.
        """
        if key == 'body':
            return self.body
        try:
            return getattr(self._frame, key)
        except AttributeError:
            return self.headers[key]

    @property
    def exchange(self):
        """Retrieve the exchange."""
        return self._frame.exchange

    @property
    def routing_key(self):
        """Retrieve the routing key."""
        return self._frame.routing_key

    @property
    def redelivered(self):
        """Retrieve the redelivery status."""
        return self._frame.redelivered

    @property
    def delivery_tag(self):
        """Retrieve the delivery tag."""
        return self._frame.delivery_tag

    @property
    def consumer_tag(self):
        """Retrieve the consumer tag.

        If the message has been retrieved with basic_get, it won't have this.
        """
        return self._frame.consumer_tag

    def ack(self, **kwargs):
        """Acknowledge the message."""
        self.channel.basic_ack(self.delivery_tag, **kwargs)

    def reply(self, **kwargs):
        """Publish a new message back to the connection"""
        params = {
            'exchange': self.exchange,
            'routing_key': self.routing_key
        }
        params.update(kwargs)
        self.channel.basic_publish(**params)

    def reject(self, **kwargs):
        """Reject a message, returning it to the queue.

        Note that this doesn't mean the message won't be redelivered to this
        same client. As the spec says:

            "The client MUST NOT use this method as a means of selecting
            messages to process."

        """
        self.channel.basic_reject(self.delivery_tag, **kwargs)

    def cancel_consume(self, **kwargs):
        """Cancel the consumer."""
        self.channel.basic_cancel(self.consumer_tag, **kwargs)
