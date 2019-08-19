mood.msgpack
============

Python MessagePack implementation

`MessagePack <https://msgpack.org/>`_ is an efficient binary serialization
format. It lets you exchange data among multiple languages like JSON. But it's
faster and smaller. Small integers are encoded into a single byte, and typical
short strings require only one extra byte in addition to the strings themselves.

**Note:** This implementation is not geared towards inter language exchange but
towards serialization/de-serialization of Python object structures (it was
designed as a `pickle <https://docs.python.org/3.5/library/pickle.html>`_
substitute). It does not expose MessagePack's extensions mechanism but uses it
internally to pack/unpack non-standard types.
That said, if you only deal with standard objects/types (``None``, ``True``,
``False``, integers, floating point numbers, bytes, strings, tuples and
dictionaries) you are fine to use this module to produce or consume data that is
targeted at or originates from other programming languages.
The following documentation is largely adapted from Python's `pickle module
documentation <https://docs.python.org/3.5/library/pickle.html>`_.

**See also:** `MessagePack specification
<https://github.com/msgpack/msgpack/blob/master/spec.md>`_


-----


What can be packed and unpacked?
--------------------------------

The following standard types can be packed/unpacked:

* ``None``, ``True`` and ``False``

* integers, floating point numbers

* strings, bytes

* tuples and dictionaries containing only packable objects

Additionally, the following Python types are supported by default:

* complex numbers

* bytearrays

* lists, sets and frozensets containing only packable objects

* classes (these **must** be `registered`_ in order to be unpacked)

* instances of classes whose ``__reduce__`` method conforms to the interface
  defined in `Packing Class Instances`_

The following MessagePack extension types are also supported:

* `Timestamp`_ (`specification
  <https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type>`_)


Module Interface
----------------

.. _registered:

register(object)
  Add *object* to the *registry*. *object* must be a class or a singleton
  (instance whose ``__reduce__`` method returns a string).

pack(object)
  Return the packed representation of *object* as a bytearray object.

unpack(message)
  Read a packed object hierarchy from a `bytes-like
  <https://docs.python.org/3.5/glossary.html#term-bytes-like-object>`_
  *message* and return the reconstituted object hierarchy specified therein.


Packing Class Instances
-----------------------

**Note:** This being chiefly based on `pickle's object.__reduce__() interface
<https://docs.python.org/3.5/library/pickle.html#object.__reduce__>`_,
most built-in objects and most objects defined in the `Python standard library
<https://docs.python.org/3.5/library/index.html>`_ already conform to it.

.. _reduce:

object.__reduce__()
    The interface is currently defined as follows: the ``__reduce__`` method
    takes no argument and shall return either a unique string or preferably a
    tuple.

    If a string is returned, the string should be interpreted as the name of a
    global variable. This behaviour is typically useful for singletons (and is
    the case for built-in functions).

    When a tuple is returned, it must be between two and five items long.
    Optional items can either be omitted, or ``None`` can be provided as their
    value. The semantics of each item are in order:

    * A callable object that will be invoked to create the initial version of
      the object (it **must** be `registered`_ in order to be unpacked).

    * A tuple of arguments for the callable object. An empty tuple must be given
      if the callable does not accept any argument.

    * Optionally, the object's state, which will be passed to the object's
      ``__setstate__`` method. If the object has no such method then, the value
      must be a dictionary and it will be merged to the object's ``__dict__``
      attribute.

    * Optionally, a sequence/iterator yielding successive items. These items
      will be appended to the object using ``object.extend(items)``. This is
      primarily used for list subclasses, but may be used by other classes as
      long as they have an ``extend`` method with the appropriate signature. If
      the object has no such method then, an in-place concatenation will be
      attempted (equivalent to ``object += items``).

    * Optionally, a dict/mapping or a sequence/iterator yielding successive
      key-value pairs.  These pairs will be stored in the object using
      ``object.update(pairs)``. This is primarily used for dictionary subclasses,
      but may be used by other classes as long as they have an ``update`` method
      with the appropriate signature. If the object has no such method then, an
      attempt will be made to store these pairs using ``object[key] = value``.

    A simple example:

    .. code:: python

        >>> from mood.msgpack import pack, unpack, register
        >>> class Kiki(object):
        ...     def __init__(self, a, b):
        ...         self.a = a
        ...         self.b = b
        ...     def __repr__(self):
        ...         return "<Kiki object: a={0.a}, b={0.b}>".format(self)
        ...     def __reduce__(self):
        ...         return (Kiki, (self.a, self.b))
        ...
        >>> k = Kiki(1, 2)
        >>> k
        <Kiki object: a=1, b=2>
        >>> b = pack(k)
        >>> b
        bytearray(b'\xc7\x15\x7f\x92\xc7\x0e\x06\xa8__main__\xa4Kiki\x92\x01\x02')
        >>> unpack(b)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        TypeError: cannot unpack <class '__main__.Kiki'>
        >>> register(Kiki)
        >>> unpack(b)
        <Kiki object: a=1, b=2>
        >>>


Timestamp, datetime, ...
------------------------

Packing/unpacking objects from the `datetime
<https://docs.python.org/3.5/library/datetime.html#module-datetime>`_ module is
straightforward.

In the packing process:

.. code:: python

    >>> import datetime
    >>> from mood import msgpack
    >>> d = datetime.datetime.now()
    >>> d
    datetime.datetime(2019, 8, 20, 8, 40, 28, 930768)
    >>> msgpack.pack(d)
    bytearray(b'\xc7#\x7f\x92\xc7\x12\x06\xa8datetime\xa8datetime\x91\xc4\n\x07\xe3\x08\x14\x08(\x1c\x0e3\xd0')
    >>>

In the unpacking process:

.. code:: python

    >>> import datetime
    >>> from mood import msgpack
    >>> msgpack.register(datetime.datetime)
    >>> msgpack.unpack(bytearray(b'\xc7#\x7f\x92\xc7\x12\x06\xa8datetime\xa8datetime\x91\xc4\n\x07\xe3\x08\x14\x08(\x1c\x0e3\xd0'))
    datetime.datetime(2019, 8, 20, 8, 40, 28, 930768)
    >>>


.. _Timestamp:

Timestamp(seconds[, nanoseconds=0])
    * seconds (int)
        Number of seconds that have elapsed since 1970-01-01 00:00:00 UTC.

    * nanoseconds (int: 0)
        Nanoseconds precision in ``range(0, 1000000000)``.


    fromtimestamp(timestamp) (*classmethod*)
        Return a new `Timestamp`_ corresponding to the *timestamp* (int/float)
        argument.


    timestamp()
        Return the floating point timestamp corresponding to this `Timestamp`_
        instance.


    seconds (*read only*)
        TODO.


    nanoseconds (*read only*)
        TODO.

