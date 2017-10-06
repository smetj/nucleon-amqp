.. nucleon.amqp documentation master file, created by
   sphinx-quickstart on Wed Sep 26 14:40:01 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introducing nucleon.amqp
========================

**nucleon.amqp** is an AMQP 0.9.1 library built from the ground up for
integration with `gevent`_.

Unlike other AMQP libraries which try hard to present an asynchronous API to
synchronous code, nucleon.amqp takes the gevent approach - to the programmer we
present a **synchronous** model, while remaining asynchronous behind the scenes
(See :doc:`asyncmodel`). This provides several benefits:

* code is simpler to write
* therefore, code is more likely to be correct
* exceptions are raised "in the proper place" if operations fail

Of course, this is a simplification - AMQP is a complicated protocol. The other
aim of nucleon.amqp is to be **the best documented Python AMQP library**. This
documentation contains not just API docs but patterns for using nucleon.amqp
both for high-performance messaging and bullet-proof reliability.


.. _`gevent`: http://www.gevent.org/


API Documentation
'''''''''''''''''

.. toctree::
    :maxdepth: 2

    amqp
    topologies
    asyncmodel
    connection
    queues_and_exchanges
    publishing
    consuming
    transactions
    glossary


Internal Documentation
''''''''''''''''''''''

.. note::

   The following documentation is intended for developers working on
   nucleon.amqp, and should not be needed by end users.


.. toctree::
    :maxdepth: 2

    codegen


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

