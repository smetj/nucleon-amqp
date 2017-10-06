Publishing Messages
===================

In AMQP publish a message is an asynchronous operation, which avoids the
latency of a synchronous operation but makes it harder to correctly handle
errors. See :doc:`asyncmodel` for more details about asynchronous methods and
workarounds.

The method to publish a method is this:

    .. module:: nucleon.amqp.channels

    .. automethod:: Channel.basic_publish


.. _basic-properties:

Basic Message Properties
''''''''''''''''''''''''

There are dozens of options regarding the reliability and durability of message
delivery as well as ``immediate`` and ``mandatory``, which can be passed as the
``headers`` parameter to :py:meth:`Channel.basic_publish` above.

The following properties are defined in the AMQP specification:

    .. describe:: (shortstr) content-type

       The MIME content type of the message.

    .. describe:: (shortstr) content-encoding

       The MIME content encoding of the message.

    .. describe:: (octet) delivery-mode

        For queues that are persistent, specify whether the message should be persisted:

        * ``1`` if the message should be non-persistent.
        * ``2`` if the message should be persistent.

    .. describe:: (octet) priority

        The priority of the message, a value from 0 to 9. Higher-priority messages
        will be delivered before all lower-priority messages (if the queue supports it).

    .. describe:: (shortstr) correlation-id

        Application correlation identifier, for application use.

    .. describe:: (shortstr) reply-to

        Address to reply to. For application use. No formal behaviour but may hold
        the name of a private response queue, when used in request messages.

    .. describe:: (shortstr) expiration

        Message expiration specification. For implementation use, no formal behaviour.

    .. describe:: (shortstr) message-id

        Application message identifier. For application use, no formal behaviour.

    .. describe:: (timestamp) timestamp

        Message timestamp. For application use, no formal behaviour.

    .. describe:: (shortstr) type

        Message type name. For application use, no formal behaviour.

    .. describe:: (shortstr) user-id

        The user ID of the publishing application. AMQP specifies no formal
        behaviour for this property, but RabbitMQ `validates that it matches the
        login username`__.

        .. __: http://www.rabbitmq.com/validated-user-id.html

    .. describe:: (shortstr) app-id

        The application ID of the publishing application. For application use, no formal behaviour.


Any property not in the above list will be sent as part of the headers table:

    .. describe:: (table) headers

       Application-defined headers; also used for header exchange routing.


Note that RabbitMQ provides an `extension for CC- and BCC-style lists of
additional routing keys`__, which are also passed as headers:

.. __: http://www.rabbitmq.com/sender-selected.html

    .. describe:: (array of longstr) CC

       Additional routing keys to deliver the message to.

    .. describe:: (array of longstr) BCC

       Additional routing keys to deliver the message to. This header will be
       removed from the headers table before the message is delivered.

.. _basic_return:

Returned Messages
'''''''''''''''''

AMQP can return a message to the sender if either the :py:meth:`immediate or
mandatory <Channel.basic_publish>` attributes are set. It is possible to test
whether a message has been returned:

    .. automethod:: MessageChannel.check_returned

The exception that is raised is

    .. autoclass:: nucleon.amqp.exceptions.MessageReturned

or one of the subclasses:

    .. autoclass:: nucleon.amqp.exceptions.MessageReturnedNoRoute

    .. autoclass:: nucleon.amqp.exceptions.MessageReturnedNoConsumers

If the :py:meth:`Connection.channel` context manager is used, this check will
automatically be made when the context manager is left (assuming there has been
no more significant exception)::

    with conn.channel() as channel:
        channel.basic_publish(
            exchange='exchange',
            routing_key='nothing.bound.to.this.routing.key',
            payload='woop',
            mandatory=True
        )
    # Exception will be raised before we get to the next statement
    return True

.. _publish confirmation:

Publish Confirmation (RabbitMQ Extension)
'''''''''''''''''''''''''''''''''''''''''

RabbitMQ supports **confirms** or publish acknowledgements, which allow a
publisher to know when a message has been processed. The alternative is to use
transactions.

Publish acknowledgements can be enabled on a per-channel basis by calling
:py:meth:`Channel.confirm_select`, which is normally synchronous but can be
called in an asynchronous mode if the optional ``nowait`` parameter is
``True``.

After this call, :py:meth:`Channel.basic_publish` will block until it
receives an acknowledgement.

    .. automethod:: Channel.confirm_select

After a channel that has been enabled for publish confirms, ``basic_publish()``
will raise a :py:class:`MessageReturned
<nucleon.amqp.exceptions.MessageReturned>` exception if the outgoing message is
returned.

