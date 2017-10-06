Glossary
========

AMQP is a complicated protocol that comes with its own terminology. If you are
lost, perhaps the glossary below may be of use.

For more detail about these terms, see the `specification documents`_, hosted
by RabbitMQ.


.. _`specification documents`: http://www.rabbitmq.com/protocol.html


.. glossary::

    message
        A message in AMQP usually consists of some data (the payload), an
        arbitary set of application-defined headers, and some basic properties
        that may control message delivery.

    queue
        A queue of messages waiting to be consumed. Queues may be configured to
        save the messages they contain to disk so that the messages will
        survive a broker restart.

    exchange
        The entry point for messages into an AMQP system. Messages are
        published *to* an exchange, and are immediately routed to one or more
        queues based on routing keys and bindings. There are three types of
        exchange: direct, fanout, and topic. Each type interprets the queue
        bindings a different way.

    routing key
        When a message is published, the routing key it is published with
        defines which queues will receive a copy of the message.  Effectively
        this is like specifying the *addressee* of the message, though the
        exact configuration of the exchanges and queues will define which
        queues actually *receive* a copy.

    binding
        The link from an exchange to a queue by which the queue may receive
        messages from that exchange. Each binding also contains a routing key
        "pattern" that acts as a filter. Messages published to that exchange,
        whose routing key "matches" the binding, will be put in the queue. If
        the exchange is a topic exchange, the routing key binding may contain
        wildcards.

    publisher
        A program that sends messages.

    consumer
        A program that receives messages.

    broker
        A network service that talks the AMQP protocol, hosts exchanges and
        queues, and ultimately passes messages from publishers to consumers.

    channel
        A single TCP connection to an AMQP service can be used for multiple
        operations at the same time. Each such operation must be performed on a
        different channel. Channels are multiplexed together so that messages
        can be received on one channel even while another channel is receiving
        a very large message. This means that clients rarely need to make
        multiple connections to an AMQP server.

    fanout exchange
        A ``fanout`` (fan out, because of the shape of the diagram when this is
        drawn) exchange disregards the routing key when delivering messages.
        All queues that are bound to the exchange receive a copy of the
        message.

    direct exchange
        A ``direct`` exchange delivers messages to all queues whose binding
        routing key exactly matches the message routing key.

    topic exchange
        A ``topic`` exchange delivers messages to all queues whose binding
        routing key is a pattern match for the message routing key. ``#`` is a
        wildcard meaning "zero or more dot-separated parts``. ``*`` is a
        wildcard meaning "exactly one dot-separated part. So if the binding
        routing key is ``profile.update`` and a queue is bound with the routing
        key ``profile.#`` it will receive the message, as well as any other
        messages whose topic starts with ``profile.``. Likewise a queue could
        be bound with a routing key ``*.update`` to receive all messages of the
        form ``something.update`` (but not ``something.something.update``).

    default exchange
        A pre-existing exchange, whose name is the empty string. When a queue
        is first created, it is bound to the default exchange, with the queue's
        name as the routing key.
