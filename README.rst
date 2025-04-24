mood.msgpack
============

Python MessagePack implementation

`MessagePack <https://msgpack.org/>`_ is an efficient binary serialization
format. It lets you exchange data among multiple languages like JSON. But it's
faster and smaller. Small integers are encoded into a single byte, and typical
short strings require only one extra byte in addition to the strings themselves.

**Note:** This implementation is designed as a
`pickle <https://docs.python.org/3.10/library/pickle.html>`_ substitute.
It does not expose MessagePack's extensions mechanism but uses it internally to
pack/unpack non-standard types.

The following documentation is largely adapted from Python's `pickle module
documentation <https://docs.python.org/3.10/library/pickle.html>`_.

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
  <https://docs.python.org/3.10/glossary.html#term-bytes-like-object>`_
  *message* and return the reconstituted object hierarchy specified therein.


Packing Class Instances
-----------------------

**Note:** This being chiefly based on `pickle's object.__reduce__() interface
<https://docs.python.org/3.10/library/pickle.html#object.__reduce__>`_,
most built-in objects and most objects defined in the `Python standard library
<https://docs.python.org/3.10/library/index.html>`_ already conform to it.

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

    * Optionally, a callable with a ``(object, state)`` signature. This callable
      allows the user to programmatically control the state-updating behavior of
      a specific object, instead of using ``object``’s own ``__setstate__``
      method.

    A simple example:

    .. code:: python

        >>> from mood.msgpack import pack, unpack, register
        >>> class Kiki(object):
        ...     def __init__(self, a, b):
        ...         self.a = a
        ...         self.b = b
        ...     def __repr__(self):
        ...         return "<Kiki object: a={0.a}, b={0.b}>".format(self)
        ...     def change(self, a, b):
        ...         self.a = a
        ...         self.b = b
        ...     def __reduce__(self):
        ...         return (Kiki, (self.a, self.b))
        ...
        >>> k = Kiki(1, 2)
        >>> k
        <Kiki object: a=1, b=2>
        >>> k.change(3, 4)
        >>> k
        <Kiki object: a=3, b=4>
        >>> b = pack(k)
        >>> b
        bytearray(b'\xc7\x15\x7f\x92\xc7\x0e\x06\xa8__main__\xa4Kiki\x92\x03\x04')
        >>> unpack(b)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        TypeError: cannot unpack <class '__main__.Kiki'>
        >>> register(Kiki)
        >>> unpack(b)
        <Kiki object: a=3, b=4>
        >>>

    Another, less simple, example:

    .. code:: python

        >>> from mood.msgpack import pack, unpack, register
        >>> def packable(func):
        ...     func.__reduce__ = lambda: f"{func.__module__}.{func.__qualname__}"
        ...     return func
        ...
        >>> @packable
        ... def setstate(obj, state):
        ...     obj.a = state["a"]
        ...     obj.b = state["b"]
        ...
        >>> class Kiki(object):
        ...     def __init__(self, a=0, b=0):
        ...         self.a = a
        ...         self.b = b
        ...     def __repr__(self):
        ...         return "<Kiki object: a={0.a}, b={0.b}>".format(self)
        ...     def change(self, a, b):
        ...         self.a = a
        ...         self.b = b
        ...     def __reduce__(self):
        ...         return (Kiki, (), {"a": self.a, "b": self.b}, None, None, setstate)
        ...
        >>> k = Kiki(1, 2)
        >>> k
        <Kiki object: a=1, b=2>
        >>> k.change(3, 4)
        >>> k
        <Kiki object: a=3, b=4>
        >>> b = pack(k)
        >>> b
        bytearray(b'\xc71\x7f\x96\xc7\x0e\x06\xa8__main__\xa4Kiki\x90\x82\xa1a\x03\xa1b\x04\xc0\xc0\xc7\x12\x07\xb1__main__.setstate')
        >>> register(Kiki)
        >>> register(setstate)
        >>> unpack(b)
        <Kiki object: a=3, b=4>
        >>>


Timestamp, datetime, ...
------------------------

Packing/unpacking objects from the `datetime
<https://docs.python.org/3.10/library/datetime.html#module-datetime>`_ module is
straightforward.

In the packing process:

.. code:: python

    >>> import datetime
    >>> from mood import msgpack
    >>> d = datetime.datetime.now()
    >>> d
    datetime.datetime(2020, 7, 31, 9, 41, 4, 139362)
    >>> msgpack.pack(d)
    bytearray(b'\xc7#\x7f\x92\xc7\x12\x06\xa8datetime\xa8datetime\x91\xc4\n\x07\xe4\x07\x1f\t)\x04\x02 b')
    >>>

In the unpacking process:

.. code:: python

    >>> import datetime
    >>> from mood import msgpack
    >>> msgpack.register(datetime.datetime)
    >>> msgpack.unpack(bytearray(b'\xc7#\x7f\x92\xc7\x12\x06\xa8datetime\xa8datetime\x91\xc4\n\x07\xe4\x07\x1f\t)\x04\x02 b'))
    datetime.datetime(2020, 7, 31, 9, 41, 4, 139362)
    >>>

Packing/unpacking `Timestamp`_ objects is also straightforward:

.. code:: python

    >>> import time
    >>> from mood import msgpack
    >>> t = msgpack.Timestamp.fromtimestamp(time.time())
    >>> t
    mood.msgpack.Timestamp(seconds=1596180901, nanoseconds=502492666)
    >>> msgpack.pack(t)
    bytearray(b'\xd7\xffw\xcd\xb7\xe8_#\xc9\xa5')
    >>>

.. code:: python

    >>> from mood import msgpack
    >>> msgpack.unpack(bytearray(b'\xd7\xffw\xcd\xb7\xe8_#\xc9\xa5'))
    mood.msgpack.Timestamp(seconds=1596180901, nanoseconds=502492666)
    >>>

Converting between `Timestamp`_ and `datetime.datetime
<https://docs.python.org/3.10/library/datetime.html#datetime.datetime>`_ objects:

.. code:: python

    >>> import datetime
    >>> from mood import msgpack
    >>> d1 = datetime.datetime.now()
    >>> d1
    datetime.datetime(2020, 7, 31, 9, 31, 18, 40406)
    >>> t = msgpack.Timestamp.fromtimestamp(d1.timestamp())
    >>> t
    mood.msgpack.Timestamp(seconds=1596180678, nanoseconds=040405989)
    >>> d2 = datetime.datetime.fromtimestamp(t.timestamp())
    >>> d2
    datetime.datetime(2020, 7, 31, 9, 31, 18, 40406)
    >>> d2 == d1
    True
    >>>

**Note:** `Timestamp`_ objects do not carry timezone information and naive
`datetime.datetime
<https://docs.python.org/3.10/library/datetime.html#datetime.datetime>`_
instances are assumed to represent local time.

.. _Timestamp:

Timestamp(seconds[, nanoseconds=0])
    * seconds (int)
        Number of seconds that have elapsed since 1970-01-01 00:00:00 UTC.

    * nanoseconds (int: 0)
        Nanoseconds precision in ``range(0, 1000000000)``.

    **Note:** nanoseconds are always added to seconds, so negative timestamps
    like -1.2 should be instantiated as Timestamp(-2, 800000000).


    fromtimestamp(timestamp) (*classmethod*)
        Return a new `Timestamp`_ instance corresponding to the *timestamp*
        (int/float) argument. Example:

        .. code:: python

            >>> from mood import msgpack
            >>> msgpack.Timestamp.fromtimestamp(-1.2)
            mood.msgpack.Timestamp(seconds=-2, nanoseconds=800000000)
            >>>


    timestamp()
        Return the floating point timestamp corresponding to this `Timestamp`_
        instance. The result of ``self.seconds + (self.nanoseconds / 1000000000)``.


    seconds (*read only*)
        *seconds* argument passed to the constructor.


    nanoseconds (*read only*)
        *nanoseconds* argument passed to the constructor.

