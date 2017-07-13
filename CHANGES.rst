Changelog
=========

0.3.2 (2017-07-13)
------------------

- Fix for installing through pip

0.3.1 (2013-01-10)
------------------

- Added unittests from the ``LifoFileQueue``.

0.3.0 (2013-01-10)
------------------

- Added ``LifoFileQueue`` implementation that returns the most recently added items first.

- Reverted the file type from gzip to a regular file for the time being.

0.2.3 (2012-11-27)
------------------

- Fix for ``PriorityFileQueue`` where it wasn't returning items in the correct order according to the priority.

- Added ``import *`` into ``__init__.py`` to make the namespace a bit nicer.

- Added the unit tests from stdlibs ``Queue`` (quickly edited out the full checks and ``LifoQueue`` tests).

0.2.2 (2012-11-27)
------------------

- Initial public release.