import gzip
import pickle
from time import time as _time
from Queue import Queue,Empty

class Complete(Exception):
    pass

class GzipQueue(Queue):

    def __init__(self,max_buffer_size,gzip_path):
        """Class to a thread safe buffer-file object. put objects into a buffer,
        any overflow will get put to a gzip file that can be accessed when the
        writer has finished.

        :param int 'max_buffer_size': Set the maximum number of items to be held
           in the buffer (Note: set to 0 for no limit)
        :param string 'gzip_path': Path to create the file for the overflowed items
          (Note this file wont get cleaned up)

        Note: The order items are returned is FIFO, unless the buffer overflows in
        which case you cannot rely on the order of returned items to be the same
        as they were put in. I wouldn't recommend using this class if the order
        of the items is important. (Items in the overflow will be retrieved in
        the order they were put in, but each put may put some items in the buffer
        meaning the order ends up being slightly shuffled)
        """
        Queue.__init__(self,max_buffer_size)
        self._complete= False
        self._q_cleared= False
        self.gzip_path= gzip_path
        self.gzip_w= gzip.open(gzip_path,"wb")
        self.gzip_r= gzip.open(gzip_path,"rb")

    def put(self,item):
        """put an item into the queue (must be pickle-able)

        :param 'item': (must be pickle-able)"""
        self.not_full.acquire()
        try:
            if self.maxsize > 0 and self._qsize() == self.maxsize:
                self._gzipPut(item)
            else:
                self._put(item)
                self.not_empty.notify()
        finally:
            self.not_full.release()

    def puts(self,items):
        """put multiple items into the queue (must be pickle-able)

        :param iterable 'items': list or iterable of items"""

        self.not_full.acquire()
        try:
            if self.maxsize <= 0:
                q_put= len(items)
            else:
                q_put= self.maxsize-self._qsize()
            for item in items[:q_put]:
                Queue._put(self,item)
                self.not_empty.notify()
            for item in items[q_put:]:
                self._gzipPut(item)
        finally:
            self.not_full.release()

    def get(self,block=True,timeout=None):
        """get an item from the queue.

        :param bool 'block': block until an item appears
        :param int/float 'timeout': if block, block for at most 'timeout' seconds

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).

        If the object is marked as complete and nothing is available it will
        raise Complete."""
        self.not_empty.acquire()
        try:
            while True:
                if self._qsize():
                    item= self._get()
                    self.not_full.notify()
                    return item
                else:
                    try:
                        self.gzip_w.close()
                        return self._gzipGet()
                    except EOFError:
                        pass
                    finally:
                        if not self._complete:
                            self.gzip_w= gzip.open(self.gzip_path,"ab")
                    self._blockCheck(block,timeout)
        finally:
            self.not_empty.release()

    def gets(self,number,block=True,timeout=None):
        """Get multiple items, will block if the optional arguement 'block' is
        set to True, or return 'number' items, or as many as are available across
        the queue and file. Will raise Empty if nothing is immediately available
        and 'block' is set to False, or if 'block' is set to True and nothing is
        available after at least 'timeout' seconds.

        :param int 'number': get 'number' items from the queue (or the max available)
        :param bool 'block': block until some items appear
        :param int/float 'timeout': if block, block for at most 'timeout' seconds

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).

        If the object is marked as complete and nothing is available it will
        raise Complete."""
        self.not_empty.acquire()
        try:
            items= []
            while True:
                q_size= self._qsize()
                if q_size:
                    for _ in xrange(min(q_size,number)):
                        items.append(self._get())
                        self.not_full.notify()
                    return items
                else:
                    try:
                        self.gzip_w.close()
                        for _ in xrange(number):
                            try:
                                items.append(self._gzipGet())
                            except IOError:
                                self.gzip_w.close()
                                items.append(self._gzipGet())
                        return items
                    except EOFError:
                        if items:
                            return items
                    finally:
                        self.gzip_w= gzip.open(self.gzip_path,"ab")
                    self._blockCheck(block,timeout)
        finally:
            self.not_empty.release()

    def _blockCheck(self,block,timeout):
        """for use internally, method to handle blocking and timeouts
           for the get methods"""
        while not self._qsize():
            if self._isComplete():
                raise Complete
            elif not block:
                raise Empty
            elif timeout is None:
                self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            else:
                endtime= _time() + timeout
                while not self._qsize():
                    if self._isComplete():
                        raise Complete
                    remaining= endtime- _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)


    def _gzipPut(self,item):
        """for use internally, puts an item into the gzip file"""
        pickle.dump(item,self.gzip_w)

    def _gzipGet(self):
        """for use internally, gets an item from the gzip file"""
        return pickle.load(self.gzip_r)

    def complete(self):
        """Call once all 'putters' have finished putting work into the queue,
        This will notify any blocked 'getters' that there will be no more items
        by raising Complete exceptions after they have emptied the Queue"""
        self.all_tasks_done.acquire()
        try:
            self.gzip_w.close()
            self._complete=True
            self.all_tasks_done.notify_all()
            self.not_empty.notify()
            self.not_full.notify()
        finally:
            self.all_tasks_done.release()

    def _isComplete(self):
        """For use internally to check if the queue has been marked as complete"""
        return self._complete
