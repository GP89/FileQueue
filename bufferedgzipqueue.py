import gzip
import time
import pickle
from Queue import Queue,Full,Empty
from threading import Lock

class GzipQueue(Queue):

    def __init__(self,max_buffer_size,gzip_path):
        """Class to a thread safe buffer-file object. put objects into a buffer,
        any overflow will get put to a gzip file that can be accessed when the
        writer has finished.

        int max_buffer_size: number of items that the buffer holds
        string gzip_path: path to the gzip file

        Note: The order items are returned is FIFO, unless put items overflow,
        in which case the overflow will be put into the gzip file which is also
        FIFO, but will be retrieved at the end after the writers have finished
        after the buffer has been cleared.
        """
        Queue.__init__(self,max_buffer_size)
        self._complete= False
        self._q_cleared= False
        self.gzip_w= gzip.open(gzip_path,"wb")
        self.gzip_r= gzip.open(gzip_path,"rb")

    def put(self,item):
        """put and item into the queue (must be pickle-able)

        item: (must be pickle-able)"""
        self.not_full.acquire()
        try:
            if self._qsize() < self.maxsize:
                Queue._put(self,item)
                self.not_empty.notify()
            else:
                self._gzipPut(item)
        finally:
            self.not_full.release()

    def puts(self,items):
        """put multiple items into the queue (must be pickle-able)

        iterable items: list or iterable of items"""
        self.not_full.acquire()
        try:
            q_put= self.maxsize-self._qsize()
            for item in items[:q_put]:
                Queue._put(self,item)
                self.not_empty.notify()
            for item in items[q_put:]:
                self._gzipPut(item)
        finally:
            self.not_full.release()

    def get(self):
        """get an item from the queue.

        will raise EOFError when the queue is emptied"""
        self.not_empty.acquire()
        try:
            if not self._q_cleared:
                while True:
                    if self._qsize():
                        item= self._get()
                        self.not_full.notify()
                        return item
                    elif not self._isComplete():
                        self.not_empty.wait()
                    else:
                        self._q_cleared= True
                        break
            return self._gzipGet()
        finally:
            self.not_empty.release()

    def gets(self,number):
        """Get multiple items, will block if the there is nothing to get, or get
        the number of items specified or all the items available if there are
        less available

        int number: get 'number' items from the queue (or the max available)

        raises EOFError when the end of the queue is reached"""
        self.not_empty.acquire()
        try:
            items=[]
            if not self._q_cleared:
                while True:
                    q_size= self._qsize()
                    if q_size:
                        for _ in xrange(min(q_size,number)):
                            items.append(self._get())
                            self.not_full.notify()
                        return items
                    elif not self._isComplete():
                        self.not_empty.wait()
                    else:
                        self._q_cleared= True
                        break
            try:
                for _ in xrange(number):
                    items.append(self._gzipGet())
                return items
            except EOFError:
                if items:
                    return items
                else:
                    raise
        finally:
            self.not_empty.release()

    def _gzipPut(self,item):
        pickle.dump(item,self.gzip_w)

    def _gzipGet(self):
        return pickle.load(self.gzip_r)

    def complete(self):
        """Call once all 'putters' have finished putting work into the queue
        will allow 'getters' to get items from the gzip overflow once they
        clear the buffer."""
        self.all_tasks_done.acquire()
        try:
            self.gzip_w.close()
            self._complete=True
            self.all_tasks_done.notify_all()
            #I expect these arn't necissary
            self.not_empty.notify()
            self.not_full.notify()
        finally:
            self.all_tasks_done.release()

    def _isComplete(self):
        return self._complete
