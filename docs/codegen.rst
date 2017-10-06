Code Generation
===============

Significant portions of nucleon.amqp are code generated from various AMQP
specifications and templates.

The actual specifications used are:

* ``amqp0-9-1.extended.xml`` from `RabbitMQ's protocol docs` - all the objects
  defined in AMQP plus several defined for RabbitMQ extensions. Note that this
  has actually been modified to address one or two errata.

* ``amqp-rabbitmq-0.9.1.json``, part of `rabbitmq-codegen`

In principle the JSON document could be eliminated as I believe the XML file
contains a superset of information, eg. documentation that is used in
docstrings.

The templates used are in ``templates/``.

.. _`RabbitMQ's protocol docs`: http://www.rabbitmq.com/protocol.html
.. _`rabbitmq-codegen`: http://hg.rabbitmq.com/rabbitmq-codegen/file/49c5b25b9dd6


How to run the code generation
------------------------------

Simply run::

    $ make

in the root of the project. This should clone the rabbitmq-codegen Mercurial
repo if it does not already exist locally.

This ultimately invokes a method in ``codegen.py`` to generate each of the
necessary files.

What is generated
-----------------

The output of the code generation are two files ``spec.py`` and
``spec_exception.py``.

nucleon/amqp/spec.py
''''''''''''''''''''

``spec.py`` contains classes corresponding to all of the method frames in the
AMQP specification.

Each frame has these properties and methods:

    .. py:class:: Frame

        .. py:attribute:: METHOD_ID

            A numeric constant; the ID of the type of frame as it appears on the wire.

        .. py:attribute:: name

            A string that is the protocol-level name of the frame, eg. queue.declare-ok

        .. py:attribute:: has_content

            Whether or not the frame has content, ie. a message. Such a frame is
            followed on the wire by a properties/header frame and then zero or more
            payload frames.

        .. py:staticmethod:: decode(buffer)

            Decode an instance of the frame from the data encoded in buffer.

        .. py:method:: encode()

            Encode the frame instance. This returns an iterable of tuples of the
            form (frame_type, payload).

Because Frame classes are intended for performance and are not expected to be
subclasses, they define appropriate ``__slots__`` and therefore do not possess
an instance dictionary.

Also defined in ``spec.py`` are functions for encoding and decoding message properties:

    .. module:: nucleon.amqp.spec

    .. autofunction:: decode_basic_properties
    .. autofunction:: encode_basic_properties

Finally, and most importantly, there is defined the :py:class:`FrameWriter`
abstract class, which defines all of the AMQP methods as method calls.

    .. py:decorator:: syncmethod(*responses)

        This decorator is used to add blocking semantics to FrameWriter methods
        that are defined as synchronous. Synchronous methods are not invoked
        directly, but are instead passed to ``self._call_sync``
        with the responses specified in the decorator.

    .. autoclass:: FrameWriter

        .. automethod:: _send

        .. automethod:: _send_message

        .. automethod:: _call_sync


nucleon/amqp/spec_exceptions.py
'''''''''''''''''''''''''''''''

This file contains exceptions for each of the error codes defined in the
specification.
