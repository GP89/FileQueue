__author__='paul'
import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "filequeue",
    version = "0.4.0",
    author = "Paul Wiseman",
    author_email = "poalman@gmail.com",
    description = ("A thread-safe queue object which is interchangeable with "
                   "the stdlib Queue. Any overflow goes into a compressed file "
                   "to keep excessive amounts of queued items out of memory"),
    long_description = read("README.rst"),
    license = "BSD",
    keywords = "queue thread-safe file gzip",
    url = "http://pypi.python.org/pypi/filequeue",
    packages = ["filequeue"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.0",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: BSD License",
        ],
    )
