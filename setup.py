__author__='paul'
import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "filequeue",
    version = "0.2.3",
    author = "Paul Wiseman",
    author_email = "poalman@gmail.com",
    description = ("A thread-safe queue object which is interchangeable with "
                   "the stdlib Queue. Any overflow goes into a compressed file "
                   "to keep excessive amounts of queued items out of memory"),
    license = "BSD",
    keywords = "queue thread-safe file gzip",
    url = "http://pypi.python.org/pypi/filequeue",
    packages=["filequeue"],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
        ],
    )
