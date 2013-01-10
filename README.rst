.. contents::

Overview
========

.. role:: mod(emphasis)

:mod:`filequeue` is a Python library that provides a thread-safe queue which is a subclass of ``Queue.Queue`` from the stdlib.

``filequeue.FileQueue`` will overflow into a compressed file if the number of items exceeds maxsize, instead of blocking or raising ``Full`` like the regular ``Queue.Queue``.

There is also ``filequeue.PriorityFileQueue`` and ``filequeue.LifoFileQueue`` implementations.

**Note** ``filequeue.FileQueue`` and ``filequeue.LifoFileQueue`` will only behave the same as ``Queue.Queue`` and ``Queue.LifoQueue`` respectively if they are initialised with ``maxsize=0`` (the default). See ``__init__`` docstring for details (``help(FileQueue)``)

**Note** ``filequeue.PriorityFileQueue`` won't currently work exactly the same as a straight out replacement for ``Queue.PriorityQueue``. The interface is very slightly different (extra optional kw argument on ``put`` and ``__init__``), although it will work it won't behave the same. It might still be useful to people though and hopefully I'll be able to address this in a future version.

Requirements:

- Python 2.5+ or Python 3.x

Why?
----

The motivation came from wanting to queue a lot of work, without consuming lots of memory.

The interface of ``filequeue.FileQueue`` matches that of ``Queue.Queue`` (or ``queue.Queue`` in python 3.x). With the idea being that most people will use ``Queue.Queue``, and can swap in a ``filequeue.FileQueue`` only if the memory usage becomes an issue. (Same applies for ``filequeue.LifoFileQueue``)

Licence
-------

Made available as-is under the BSD Licence.

Issues
------
Any issues please post on the `github page <https://github.com/GP89/FileQueue/issues>`_.