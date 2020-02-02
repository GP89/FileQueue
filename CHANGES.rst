Changelog
=========

0.4.1 (2020-02-02)
------------------

- Clean up when disposing of the queue

0.4.0 (2019-10-11) Windows OS now supported!
------------------

- Fixed issue on windows, use a shared rw file descriptor. Can't use 2 separate r and w like unix based systems
- Fixed issue for LifoFileQueue on py3, was trying to write str instead of bytes to file

0.3.3 (2019-10-01)
------------------

- Fix for namespace issue on python 3

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