Consuming messages
==================

.. module:: nucleon.amqp.channels

AMQP normally applies a policy that the consumer of a message is responsible
for acknowledging that it has been successfully processed (or otherwise).
Messages that are not acknowledged can be redelivered, both upon request (with
:py:meth:`Channel.basic_recover`) and to another consumer if the connection is
closed before acknowledgements have been received. This ensures that messages
are not deleted from the server before they have been processed, but does not
guarantee that all messages will be processed in a timely fashion.

The core method to begin consuming messages is ``basic_consume``:

    .. automethod:: MessageChannel.basic_consume

In nucleon.amqp ``basic_consume`` has two modes of operations: callback-based, and
queue-based.

To use the callback semantics, pass a callback that takes a single parameter.
This callback will be called for each message received::

    def on_message(message):
        """Process an incoming message."""
        print message.body
        message.ack()

    # NB. can't use the context manager here because leaving the context would
    # close the channel and terminate the consumer. So use .allocate_channel()
    # which allocates a persistent channel.
    channel = conn.allocate_channel()

    # Set up the queue/binding
    channel.queue_declare(queue=qname)
    channel.queue_bind(
        queue=qname,
        exchange=exchange,
        routing_key=routing_key
    )

    # Start consuming
    channel.basic_consume(queue=qname, callback=on_message)


The other alternative is used when no callback is passed. In this case,
``basic_consume`` returns an object that implements the Python `Queue`_
interface. It is then possible to write synchronous consumer code by repeatedly
calling :py:meth:`MessageQueue.get`::

    with conn.channel() as channel:
        # Set up the queue/binding
        channel.queue_declare(queue=qname)
        channel.queue_bind(
            queue=qname,
            exchange=exchange,
            routing_key=routing_key
        )

        # Start consuming
        queue = channel.basic_consume(queue=qname)

        while True:
            # Loop forever, handling messages from the queue
            message = queue.get()
            print message.body
            message.ack()


The advantage of this approach is that exceptions will be raised from the call
to ``queue.get()``. The queue object also supports :py:meth:`cancelling the
consumer <MessageQueue.cancel>`.

    .. autoclass:: MessageQueue
        :members:

.. _`Queue`: http://docs.python.org/library/queue.html#queue-objects


Whether you are using the callback approach or the queue-based approach, but
perhaps particularly in the case of the latter, you may want to control how
many messages can be delivered to nucleon.amqp before being acknowledged. Note
that even when using callbacks messages are still queued in the nucleon.amqp
machinery pending dispatch, so it is useful to set a limit:


    .. automethod:: Channel.basic_qos

Note that this limit is ignored if the consumer was started with the ``no_ack``
flag.

.. _message object:

Message Object
''''''''''''''

.. module:: nucleon.amqp.message

Methods that contain messages are received from nucleon.amqp as a **message
object** which provides a higher-level interface for interpreting messages, as
well as operations that act on an already received message, such as
:py:meth:`basic_ack() <Message.ack>` and :py:meth:`basic_reject()
<Message.reject>`.

    .. autoclass:: Message
        :members:

        .. py:attribute:: headers

           A dictionary of headers and :ref:`basic message properties
           <basic-properties>` associated with the message.


Acknowledging or Rejecting Messages
'''''''''''''''''''''''''''''''''''

.. currentmodule:: nucleon.amqp.channels

Unless you have set the ``no_ack`` flag when consuming the message, it is
necessary to acknowledge or reject a message to inform the server that the
message has been successfully processed, or couldn't be processed respectively.

The :ref:`message object` has shortcuts to do this, but it is also possible to
call these methods on the channel itself. To call these it is necessary to pass
the message's server-generated ``delivery_tag`` to the corresponding method:

    .. automethod:: Channel.basic_ack
    .. automethod:: Channel.basic_reject

Unacknowledged messages should not be allowed to build up on the server as they
will consume resources. Closing the channel without acknowledging messages will
automatically cause unacknowledged messages to be requeued, but it is also
possible to use ``recover`` methods to redeliver or requeue unacknowledged
messages:

    .. automethod:: Channel.basic_recover
    .. automethod:: Channel.basic_recover_async


Poll a queue
''''''''''''

It may be useful to use poll to get the first message in a queue:

    .. automethod:: Channel.basic_get

If there are no messages in a queue, this method returns ``None``, otherwise it
returns the first message (which must be acknowledged as usual unless the
``no_ack`` flag was set).

Note that this is not the normal way to retrieve messages, for a number of
reasons. One is performance - if you are polling then there is unnecessary
additional latency between publishing and consuming a message, not to mention a
CPU and network cost to making the request even when there are no messages
present. Another reason is that some AMQP functionality is built around the
concept of active consumers - for example, auto-delete queues. It can also
impact the performance of the broker, as it must persist a message rather than
simply delivering it straight to an active consumer.

RabbitMQ Extensions
'''''''''''''''''''

.. currentmodule:: nucleon.amqp.channels

`basic.nack`_ is a RabbitMQ extensions for rejecting multiple messages at the
same time:

    .. automethod:: Channel.basic_nack

.. _`basic.nack`: http://www.rabbitmq.com/nack.html
