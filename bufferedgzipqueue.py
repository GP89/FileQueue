import gzip
import cPickle
from time import time as _time
from Queue import Queue,Empty

class Complete(Exception):
    pass

class GzipQueue(Queue):
    def __init__(self,max_buffer_size,gzip_path):
        """Class to a thread safe buffer-file object. put objects into a buffer,
        any overflow will get put to a gzip file on disk to keep excessive
        amounts of queued items out of memory.

        :param int 'max_buffer_size': Set the maximum number of items to be held
           in the buffer (Note: set to 0 for no buffer in memory, queue all on disk)
        :param string 'gzip_path': Path to create the file for the overflowed items
          (Note this file wont get cleaned up)

        Note: The order items are returned is FIFO, unless the buffer overflows in
        which case you cannot rely on the order of returned items to be the same
        as they were put in. I wouldn't recommend using this class if the order
        of the items is important. (Items in the overflow will be retrieved in
        the order they were put in, but each put may put some items in the buffer
        meaning the order ends up being slightly shuffled) (However if
        max_buffer_size is set to 0, all items go straight to disk and the order
        will be maintained)
        """
        Queue.__init__(self,max(max_buffer_size,0))
        self._complete= False
        self._q_cleared= False
        self.num_put=0
        self.num_got=0
        self.gzip_path= gzip_path
        self.gzip_w= gzip.open(gzip_path,"wb")
        self.gzip_r= gzip.open(gzip_path,"rb")

    def _put(self, item):
        Queue._put(self,item)
        self.num_put+=1
        self.unfinished_tasks+=1

    def put(self,item):
        """put an item into the queue (must be pickle-able)

        :param 'item': (must be pickle-able)"""
        with self.not_full:
            failed={"items":[]}
            if self._qsize() >= self.maxsize:
                self._gzipPutFail(failed,item)
            else:
                self._put(item)
            self.not_empty.notify()
            self.checkException(failed)

    def puts(self,items):
        """put multiple items into the queue (must be pickle-able)

        :param iterable 'items': iterable of items"""
        with self.not_full:
            failed={"items":[]}
            q_put= max(self.maxsize-self._qsize(),0)
            for item in items[:q_put]:
                self._put(item)
            for item in items[q_put:]:
                self._gzipPutFail(failed,item)
            self.not_empty.notify()
            self.checkException(failed)

    def checkException(self,failed):
        err= failed.get("exception")
        if err:
            err.items= failed["items"]
            err.message+=" (check attribute 'items' for list of items unable to put into queue)"
            raise err

    def _gzipPutFail(self,failed,item):
        try:
            self._gzipPut(item)
        except Exception as err:
            failed["exception"]=err
            failed["items"].append(item)

    def _get(self):
        item= Queue._get(self)
        self.num_got+=1
        return item

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
        with self.not_empty:
            while True:
                if self._qsize():
                    item= self._get()
                    self.not_full.notify()
                    return item
                else:
                    self.gzip_w.close()
                    try:
                        item= self._gzipGet()
                        self.not_full.notify()
                        return item
                    except EOFError:
                        pass
                    finally:
                        self.gzip_w= gzip.open(self.gzip_path,"ab")
                    self._blockCheck(block,timeout)

    def gets(self,number,block=True,timeout=None):
        """Get multiple items, will block if the optional argument 'block' is
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
        with self.not_empty:
            items=[]
            while True:
                q_size= self._qsize()
                buffer_get= min(q_size,number)
                file_get= number-buffer_get
                if buffer_get:
                    for _ in xrange(buffer_get):
                        items.append(self._get())
                    self.not_full.notify()
                if not file_get:
                    return items
                self.gzip_w.close()
                try:
                    for _ in xrange(file_get):
                        try:
                            items.append(self._gzipGet())
                        except IOError:
                            self.gzip_w.close()
                            items.append(self._gzipGet())
                    self.not_full.notify()
                    return items
                except EOFError:
                    if items:
                        self.not_full.notify()
                        return items
                finally:
                    self.gzip_w= gzip.open(self.gzip_path,"ab")
                self._blockCheck(block,timeout)


    def _blockCheck(self,block,timeout):
        """for use internally, method to handle blocking and timeouts
           for the get methods"""
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
            while not self._qsize() and not self._gzipHasItems():
                if self._isComplete():
                    raise Complete
                remaining= endtime- _time()
                if remaining <= 0.0:
                    raise Empty
                self.not_empty.wait(remaining)

    def _gzipHasItems(self):
        if self.maxsize > 0:
            self.gzip_w.close()
            item= None
            try:
                item= self._gzipGet(False)
            except EOFError:
                pass
            finally:
                self.gzip_w= gzip.open(self.gzip_path,"ab")
            if item:
                self._gzipPut(item,False)
                return True
        return False

    def _gzipPut(self,item,count=True):
        """for use internally, puts an item into the gzip file"""
        cPickle.dump(item,self.gzip_w)
        if count:
            self.num_put+=1
            self.unfinished_tasks+=1

    def _gzipGet(self,count=True):
        """for use internally, gets an item from the gzip file"""
        item= cPickle.load(self.gzip_r)
        if count:
            self.num_got+=1
        return item

    def complete(self):
        """Call once all 'putters' have finished putting work into the queue,
        This will notify any blocked 'getters' that there will be no more items
        by raising Complete exceptions after they have emptied the Queue"""
        with self.all_tasks_done:
            if self.maxsize > 0:
                self.gzip_w.close()
            self._complete=True
            self.not_empty.notifyAll()
            self.not_full.notifyAll()

    def _isComplete(self):
        """For use internally to check if the queue has been marked as complete"""
        if self._complete:
            self.all_tasks_done.notifyAll()
        return self._complete

    @property
    def contains(self):
        return self.num_put-self.num_got
