Introduction to AMQP
====================

AMQP specifies the concept of a :term:`broker`, a network service that routes
messages between communicating parties with various levels of reliability
guarantees.

AMQP offers the ability to decouple services:

* Decouple in time (asynchronicity)
* Decouple in space (happen somewhere else, perhaps just on the machine that
  has the necessary data)
* Decouple languages (empowering you to use the right tool for the job)
* Decouple concerns (build new services without changes to existing services)

It also provides the benefits of a distributed architecture:

* Availability
* Performance/scalability

The protocol allows clients to set up within the broker a system of
:term:`exchanges <exchange>`, :term:`queues <queue>` and the :term:`bindings
<binding>` between them.

Having set this up, various distributed clients can connect, open a
:term:`channel`, and publish messages to an exchange or begin consuming
messages from a queue:

.. image:: overview.png

The important thing to note when designing message routing topologies is that
**at the routing stage messages are copied into each matching queue, while at
the delivery stage each message is delivered once only.** This property is
exploited to create a variety of routing strategies that include delivering a
message to a specific consumer, any consumer, or all consumers, among others.

Routing a message
-----------------

Messages are routed to one or more queues based on the **exchange** that they
are published to, and the **routing key** of the message. To draw a parallel
with e-mail, you could think of each message being sent to an e-mail address
like *routing_key@exchange*.

Let's take a closer look at how messages are routed. The ``type`` of the
exchange controls how the routing key of an incoming message is matched to the
various bound queues.

.. _topic exchange:

Topic Exchange
''''''''''''''

The :term:`topic exchange` is the most general type of exchange. When a message
is published, the exchange works out which queues will receive it by matching
the message's routing key against the queue binding's **routing key
pattern**:

.. image:: routing.png

For example, if messages are published with routing keys such as
``article.new`` or ``comment.new`` we could bind a queue to receive messages
about both by binding it with a routing key pattern ``*.new``.

For more details of the pattern matching, see :ref:`topic-exchange-patterns`
below.

Note that I've drawn specific input routing keys, but routing keys do not need
to be pre-declared; a message could be published with any routing key. Of
course, the patterns must have been bound before the message is published or
the message will not be routed to a queue.


Direct Exchange
'''''''''''''''

A :term:`direct exchange` is a simplification - you could consider there is a
single layer of routing keys to which both messages are published and queues
are directly bound. However there is no reason queues cannot be bound to
multiple routing keys, or routing keys cannot deliver to multiple queues:

.. image:: direct.png

More strictly, queues are bound to one or more specific routing keys. The queue
will receive all messages published to that exchange with one of those routing
keys.

Fan-out Exchange
''''''''''''''''

Finally, a :term:`fanout exchange` is simpler still. Routing keys are
disregarded, so there is only one endpoint to which all messages are published
and queues can be bound. Simply put, every message published to the exchange
will be broadcast to every queue bound to the exchange.

.. image:: fanout.png

Of course, the routing key is part of the message that
consumers receive, so this could be used to control how the message is
processed by an application.

.. _topic-exchange-patterns:

Topic Exchange Patterns
-----------------------

A ``topic`` exchange delivers messages to all queues whose binding routing key
is a pattern match for the message routing key. The routing key should consist
of dot-separated parts, such as ``acme.accounting.invoice.new``.

* ``#`` is a wildcard meaning "zero or more dot-separated parts".
* ``*`` is a wildcard meaning "exactly one dot-separated part".

So if the binding routing key is ``profile.update`` and a queue is bound with
the routing key ``profile.#`` it will receive the message, as well as any other
messages whose topic starts with ``profile.``. Likewise a queue could be bound
with a routing key ``*.update`` to receive all messages of the form
``something.update`` (but not ``something.something.update``).
