mood.msgpack
============

Python MessagePack implementation

`MessagePack <https://msgpack.org/>`_ is an efficient binary serialization
format. It lets you exchange data among multiple languages like JSON. But it's
faster and smaller. Small integers are encoded into a single byte, and typical
short strings require only one extra byte in addition to the strings themselves.

**See also:** `MessagePack specification
<https://github.com/msgpack/msgpack/blob/master/spec.md>`_


What can be packed and unpacked?
--------------------------------

The following types can be packed:

* ``None``, ``True``, and ``False``

* integers, floating point numbers, complex numbers

* strings, bytes, bytearrays

* tuples, lists, sets, frozesets, and dictionaries containing only packable
  objects

* classes (***must*** be `registered`_ in order to be unpacked)

* instances of classes whose ``__reduce__`` method conforms to the interface
  defined in `Packing Class Instances`_


Module Interface
----------------

.. _registered:

register(obj)
  placeholder

pack(obj)
  placeholder

unpack(msg)
  placeholder


Packing Class Instances
-----------------------

**Note:** This is largely based on Python's `pickle
<https://docs.python.org/3.5/library/pickle.html>`_ module, this means most
built-in objects and most objects defined in the standard library already
conform to this interface (also most of the following documentation is a
copy/paste from the `pickle module object.__reduce__() documentation
<https://docs.python.org/3.5/library/pickle.html#object.__reduce__>`_).

.. _reduce:

object.__reduce__()
  The interface is currently defined as follows: the ``__reduce__`` method takes
  no argument and shall return either a string or preferably a tuple (the
  returned object is often referred to as the "reduce value").

  If a string is returned, the string should be interpreted as the name of a
  global variable. This behaviour is typically useful for singletons (and is the
  case for built-in functions).

  When a tuple is returned, it must be between two and five items long.
  Optional items can either be omitted, or ``None`` can be provided as their
  value. The semantics of each item are in order:

  * A callable object that will be called to create the initial version of the
    object (**must** be `registered`_ in order to be unpacked).

  * A tuple of arguments for the callable object. An empty tuple must be given
    if the callable does not accept any argument.

  * Optionally, the object's state, which will be passed to the object's
    ``__setstate__`` method. If the object has no such method then, the value
    must be a dictionary and it will be added to the object's ``__dict__``
    attribute.

  * Optionally, a sequence/iterator yielding successive items. These items
    will be appended to the object using ``object.extend(items)``. This is
    primarily used for list subclasses, but may be used by other classes as long
    as they have an ``extend`` method with the appropriate signature. If the
    object has no such method then, an in-place concatenation will be attempted
    (equivalent to ``object += items``).

  * Optionally, a dict/mapping or a sequence/iterator yielding successive
    key-value pairs.  These pairs will be stored to the object using
    ``object.update(pairs)``. This is primarily used for dictionary subclasses,
    but may be used by other classes as long as they have an ``update`` method
    with the appropriate signature. If the object has no such method then, an
    attempt will be made to store these pairs using ``object[key] = value``.

  A simple example::

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

