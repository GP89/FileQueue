import gzip
import heapq
import tempfile
from time import time as _time
try:
    import cPickle as _pickle
except ImportError:
    import pickle as _pickle
try:
    from Queue import Queue,Empty,Full
except ImportError:
    from queue import Queue,Empty,Full

DEFAULT = None

__all__= ["Empty", "Full", "FileQueue", "PriorityFileQueue"]

class FileQueue(Queue):
    def __init__(self, maxsize=0):
        """
        Class to a thread safe file queue object. Keeps the same interface
        and is interchangeable with the Queue in the python standard lib (see note).
        Put objects into a queue, the same as the regular Queue.Queue but any
        overflow (number of items in the queue greater than maxsize) will get put
        into a gzipped file on disk to keep excessive amounts of queued items out
        of memory.

        :type maxsize: int
        :param maxsize: Set the maximum number of items to be held in the buffer

        Note: The order items are returned is guaranteed FIFO only if no buffer is
        set (maxsize=0). If a buffer size is set it remains FIFO until the point at
        which the buffer overflows in which case you cannot rely on the order of
        returned items to be the same as they were put in. I wouldn't recommend
        using this queue with maxsize set if the order of the items is important.

        (Items in the overflow will be retrieved in the order they were put into
        the overflow, but each 'put' may put some items in the buffer, and some in
        the overflow, meaning the order ends up being slightly shuffled, because
        items in the buffer will always be returned before any in the overflow,
        which is only ever accessed when nothing is available from the buffer)
        """
        Queue.__init__(self,maxsize)
        self._contains= 0
        self._temp_file= tempfile.NamedTemporaryFile(suffix=".queue")
        self._gzip_read= gzip.open(self._temp_file.name,"rb")
        self._gzip_write= gzip.open(self._temp_file.name,"wb")

    def _buffer_size(self):
        """
        Return the approximate size of the buffer (not reliable!).
        """
        return Queue._qsize(self)

    def full(self):
        """
        Return True if the queue is full, False otherwise.
        (always returns False- can't be full because of the overflow)
        """
        return False

    def _qsize(self, len=len):
        return self._contains

    def _put(self, item):
        Queue._put(self,item)
        self._put_done()

    def _put_gzip(self,item):
        _pickle.dump(item,self._gzip_write)
        self._put_done()

    def _put_done(self):
        self._contains += 1
        self.unfinished_tasks += 1
        self.not_empty.notify()

    def put(self, item, block=True, timeout=None):
        """
        Put an item into the queue (must be pickle-able)

        Note: optional arguments 'block' and 'timeout' are *IGNORED*.
        FileQueue always has a file to put any overflow into, so there is
        no time when put needs to block. These arguments only exist to hold
        the interface with Queue.Queue
        """
        self.not_full.acquire()
        try:
            if self._buffer_size() < self.maxsize:
                self._put(item)
            else:
                self._put_gzip(item)
        finally:
            self.not_full.release()

    def _get(self):
        item= Queue._get(self)
        self._get_done()
        return item

    def _get_gzip(self):
        self._gzip_write.close()
        try:
            try:
                item= _pickle.load(self._gzip_read)
            except EOFError:
                raise Empty
            self._get_done()
            return item
        finally:
            self._gzip_write= gzip.open(self._temp_file.name,"ab")

    def _get_done(self):
        self._contains -= 1
        self.not_full.notify()

    def get(self, block=True, timeout=None):
        """
        Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.not_empty.acquire()
        try:
            while True:
                self._get_block_check(block,timeout)
                if self._buffer_size():
                    item= self._get()
                else:
                    try:
                        item= self._get_gzip()
                    except Empty:
                        continue
                return item
        finally:
            self.not_empty.release()

    def _get_block_check(self,block,timeout):
        if not block:
            if not self._qsize():
                raise Empty
        elif timeout is None:
            while not self._qsize():
                self.not_empty.wait()
        elif timeout < 0:
            raise ValueError("'timeout' must be a positive number")
        else:
            endtime = _time() + timeout
            while not self._qsize():
                remaining = endtime - _time()
                if remaining <= 0.0:
                    raise Empty
                self.not_empty.wait(remaining)

class PriorityFileQueue(FileQueue):
    def __init__(self, maxsize=0, default_priority=1):
        Queue.__init__(self,1)
        self._contains= 0
        self._max_buffer_size= maxsize
        self._default_priority= default_priority
        self._queues= list()
        self._queue_index= dict()

    def _get_queue(self,priority):
        queue= self._queue_index.get(priority)
        if not queue:
            queue= FileQueue(self._max_buffer_size)
            heapq.heappush(self._queues,(priority,queue))
            self._queue_index[priority]= queue
        return queue

    def put(self, item, block=True, timeout=None, priority=DEFAULT):
        self.not_full.acquire()
        try:
            if priority is DEFAULT:
                priority= self._default_priority
            self._get_queue(priority).put(item)
            self._put_done()
        finally:
            self.not_full.release()

    def get(self, block=True, timeout=None):
        self.not_empty.acquire()
        try:
            while True:
                self._get_block_check(block,timeout)
                for id,queue in self._queues:
                    try:
                        item= queue.get(False)
                        self._get_done()
                        return item
                    except Empty:
                        pass
        finally:
            self.not_empty.release()