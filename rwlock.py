import threading
import collections

class _LockInfo:
    def __init__(self):
        self.readers = collections.Counter()
        self.check = threading.Condition()
        self.start = threading.RLock()

def get_rw_locks():
    """Returns a (read_lock, write_lock) pair.
    Read locks are not exclusive; write locks are.
    Both locks are re-entrant, meaning the same thread can
    lock them multiple times recursively (as long as the number of
    unlocks matches the number of locks).
    You can also promote from a read lock to a write lock.
    """
    # start lock, end lock
    info = _LockInfo()
    return ReadLock(info), WriteLock(info)

def get_ro_locks():
    """Returns a (read_lock, write_lock) pair,
    that allows for reading only.
    The read lock always grants and the write lock always
    raises an exception."""
    return PassLock(), FailLock()

class ReadLock:
    def __init__(self, lock_info):
        self._info = lock_info

    def acquire(self):
        me = threading.get_ident()
        with self._info.check:
            if self._info.readers[me]:
                # this thread is already reading
                self._info.readers[me] += 1
                return True
        with self._info.start:
            with self._info.check:
                self._info.readers[me] += 1
        return True

    def release(self):
        me = threading.get_ident()
        with self._info.check:
            self._info.readers[me] -= 1
            if sum(self._info.readers.values()) == 0:
                self._info.check.notify()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, et, ev, tb):
        self.release()

class WriteLock:
    def __init__(self, lock_info):
        self._info = lock_info

    def acquire(self):
        me = threading.get_ident()
        self._info.start.acquire()
        with self._info.check:
            while (sum(self._info.readers.values()) - 
                    self._info.readers[me]) > 0:
                self._info.check.wait()
            self._info.readers[me] += 1 # allows to obtain a read lock
        return True

    def release(self):
        me = threading.get_ident()
        if self._info.readers[me] <= 0:
            raise RuntimeError("Can't release a lock you never held!")
        with self._info.check:
            self._info.readers[me] -= 1
        self._info.start.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, et, ev, tb):
        self.release()

class PassLock:
    """Looks like a lock, but always grants access (does nothing)."""

    def acquire(self):
        return True

    def release(self):
        pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, et, ev, tb):
        self.release()

class FailLock:
    """Looks like a lock, but NEVER grants access."""

    def acquire(self):
        raise RuntimeError("Can't lock a fail lock. You are probably trying to write to a read-only resource.")

    def release(self):
        raise RuntimeError("Can't release a lock you never held!")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, et, ev, tb):
        self.release()

if __name__ == '__main__':
    # test code
    import random
    import time

    rlock, wlock = get_rw_locks()
    indent = 0
    
    class Reader(threading.Thread):
        def __init__(self, lock, num):
            super().__init__()
            self.lock = lock
            self.num = num
        def run(self):
            global indent
            print("Reader thread", self.num, "running")
            for _ in range(2):
                time.sleep(random.random())
                with self.lock:
                    indent += 1
                    print('  '*indent, "Reader", self.num, "start read")
                    time.sleep(random.random())
                    print('  '*indent, "Reader", self.num, "end read")
                    indent -= 1

    class Writer(threading.Thread):
        def __init__(self, lock, num):
            super().__init__()
            self.lock = lock
            self.num = num
        def run(self):
            global indent
            print("Writer thread", self.num, "running")
            for _ in range(2):
                time.sleep(random.random())
                with self.lock:
                    assert indent == 0
                    print("WRITER", self.num, "start write")
                    time.sleep(random.random())
                    print("WRITER", self.num, "end write")

    threads = []
    for num in range(1,11):
        if random.randrange(2):
            threads.append(Reader(rlock, num))
        else:
            threads.append(Writer(wlock, num))
        threads[-1].start()
    
    for t in threads:
        t.join()

    print("done")
