Queues and Exchanges
====================

**Queues** and **exchanges** - and the **bindings** that connect them - are at
the heart of message routing with AMQP.

The process of configuring queues and exchanges is as follows:

1. Declare an exchange
2. Declare a queue
3. Bind the queue to the exchange, with a particular routing key pattern.

Then the queue will receive a copy of all messages published to the exchange,
where the message routing key "matches" the routing key of the binding.
Multiple queues can be bound to an exchange. Queues can be bound to more than
one exchange, or bound multiple times to the same exchange.

Having created a queue, one or more clients may start consuming messages from
the queue. In the case that more than one consumer is consuming from the same
queue, each message is delivered to only one consumer.

See :doc:`amqp` for an introduction to AMQP Exchange/Queue/Binding topologies.

Exchange Management
'''''''''''''''''''

.. class:: nucleon.amqp.channels.Channel

    .. automethod:: exchange_declare
    .. automethod:: exchange_delete

Queue Management
''''''''''''''''

.. class:: nucleon.amqp.channels.Channel

    .. automethod:: queue_declare
    .. automethod:: queue_delete
    .. automethod:: queue_purge


Binding and Unbinding Queues
''''''''''''''''''''''''''''

.. class:: nucleon.amqp.channels.Channel

    .. automethod:: queue_bind
    .. automethod:: queue_unbind


Exchange-to-Exchange Binding (RabbitMQ Extension)
'''''''''''''''''''''''''''''''''''''''''''''''''

RabbitMQ allows `exchanges to be bound to other exchanges`__,
in order to allow for more sophisticated routing topologies.

.. __: http://www.rabbitmq.com/e2e.html


These methods are available for exchange-to-exchange binding:

.. class:: nucleon.amqp.channels.Channel

    .. automethod:: exchange_bind
    .. automethod:: exchange_unbind

