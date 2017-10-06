Transactions
============

Transactions are used to wrap a block of :doc:`asynchronous operations
<asyncmodel>` to make them synchronous as well as atomic. The cost is in
performance, both in terms of the server providing atomicity and added
round-trip time in communicating with the server.

.. module:: nucleon.amqp.channels

.. automethod:: Channel.tx_select
.. automethod:: Channel.tx_commit
.. automethod:: Channel.tx_rollback

