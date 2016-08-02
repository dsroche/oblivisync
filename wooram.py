#!/usr/bin/env python3

import sys
import random
import pickle
import time
import math
import threading

from buffer import Buffer
from block import Block
from rwlock import get_rw_locks
from vtable import VTable
from superblock import new_superblock, load_superblock, save_superblock

BUF_MEASURE=False
DEBUG=False

def load_wooram(backend, blocksize=2**22, total_blocks=2**10, 
        drip_rate=3, drip_time=60, headerlen=48):
    """Greedily attempts to load a wooram object from the given backend.
    If none is found stored there already, it will be created with the given
    parameters."""
    sup = None
    try:
        sup = load_superblock(backend)
        if (blocksize != sup.blocksize or total_blocks != sup.total_blocks 
                or headerlen != sup.headerlen):
            print("WARNING: Some parameters differ from superblock and will be ignored.")
        print("Successfully loaded WoOram from superblock")
    except ValueError:
        sup = new_superblock(blocksize, total_blocks, headerlen)
    return WoOram(backend, sup, drip_rate, drip_time)

class WoOram:
    def __init__(self, backend, sup, drip_rate, drip_time):
        self.backend = backend
        self.vtable = sup.vtable
        self.blocksize = sup.blocksize
        self.headerlen = sup.headerlen
        self.N = sup.total_blocks
        self.K = drip_rate
        self.T = drip_time
        self.fbsize = sup.fbsize
        self.split_maxnum = sup.split_maxnum
        self.split_maxsize = sup.split_maxsize

        self.buf = Buffer()
        self.rlock, self.wlock = get_rw_locks()
        self.syncer = Syncer(self, self.T)

        self.active = False # is the sync thread running
        self.syncing = False # is a sync operation in progress
        self.recent = None # set of (vnode, boff) pairs for what has changed during the sync op

    def start(self):
        if self.T > 0:
            self.active = True
            self.syncer.start()
        else:
            print("NOTE: sync thread not actually started...")

    def finish(self):
        """Waits until the buffer has been cleared, then stops the syncer and returns."""
        if self.active:
            self.active = False
            print("Waiting for the sync thread to finish...", file=sys.stderr)
            self.syncer.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, et, ev, tb):
        self.finish()

    def __len__(self):
        """Returns the number of distinct items stored (i.e., # of vnodes)"""
        return len(self.vtable)

    def size(self):
        """Returns the total size (in bytes) of all items stored. (Warning: slow)"""
        with self.rlock:
            return sum(self.get_size(v) for v in self.vtable)

    def num_blocks(self, vnode):
        """Returns the number of blocks this file occupies."""
        return len(self.vtable.get_info(vnode).inodes)

    def get_size(self, vnode):
        """The number of bytes of data stored for the given object."""
        return self.vtable.get_size(vnode)

    def get_mtime(self, vnode):
        """The last modification time of the given object."""
        return self.vtable.get_mtime(vnode)

    def set_mtime(self, vnode, when=None):
        if when is None:
            when = time.time()
        self.vtable.set_mtime(vnode, when)

    def capacity(self):
        """The total space avaiable (in bytes) in the backend."""
        return self.blocksize * self.N

    def _make_block(self, b1, b2):
        """Creates a new block with the given contents on either side.
        Each should be a Block object.
        The block is padded up to self.blocksize.
        """
        # TODO make more efficiently indexable storage representation?
        block = pickle.dumps((b1.contents, b2.contents))
        assert len(block) <= self.blocksize - self.headerlen
        return block + b'\0'*(self.blocksize - len(block) - self.headerlen)

    def _get_backend(self, ind):
        """Returns a tuple of block objects stored at the given index."""
        res = []
        try:
            raw = self.backend[ind]
        except IndexError:
            # this could be a normal error, just a new repository
            res = None
        except:
            res = None
            print("WARNING: error fetching", ind, "from backend. Maybe wrong key?")
        if res is not None:
            try:
                parts = pickle.loads(raw)
            except:
                parts = None
            if type(parts) is not tuple:
                print("WARNING: error unpickling", ind, "from backend")
                res = None
        if res is not None:
            for contents in parts:
                if contents is None:
                    kind = Block.EMPTY
                elif type(contents) is dict:
                    kind = Block.SPLIT
                elif type(contents) is tuple and len(contents) == 2:
                    kind = Block.FULL
                else:
                    print("WARNING: messed up parts in", ind, "from backend")
                    res = None
                    break
                res.append(Block(self, kind, contents))
        if res is None or len(res) != 2:
            return tuple(Block(self, Block.EMPTY) for _ in range(2))
        else:
            return tuple(res)

    def _get_fresh(self, ind):
        """Gets the pair of Blocks stored at the given index,
        after removing anything that's stale."""
        res = []
        inode0 = 2*ind
        with self.rlock:
            parts = self._get_backend(ind)
            for j, blk in enumerate(parts):
                inode = inode0+j
                if blk.kind() == Block.SPLIT:
                    stale = [vnode for vnode in blk.contents 
                            if self.vtable.is_stale(vnode, inode)]
                    for vnode in stale:
                        del blk.contents[vnode]
                    if len(blk.contents) == 0:
                        # all entries in split block are stale, so it's considered an empty block
                        blk = Block(self, Block.EMPTY)
                elif blk.kind() == Block.FULL:
                    if self.vtable.is_stale(blk.contents[0], inode):
                        # full block is stale, so it's actually empty
                        blk = Block(self, Block.EMPTY)
                res.append(blk)
        return res

    def _fetch_block_inode(self, vnode, inode, split):
        """Gets the contents of the given vnode stored in backend at the given
        inode. split is a bool indicating whether it's an sblock."""
        assert 0 <= inode < 2*self.N
        parts = self._get_backend(inode//2)
        if split:
            for blk in parts:
                if blk.kind() == Block.SPLIT and vnode in blk.contents:
                    return blk.contents[vnode]
        else:
            blk = parts[inode % 2]
            if blk.kind() == Block.FULL and blk.contents[0] == vnode:
                return blk.contents[1]
        return None

    def _fetch_backend(self, vnode, boff):
        with self.rlock:
            inode, split = self.vtable.get_inodes(vnode)[boff]
            if inode < 0:
                return None
            else:
                return self._fetch_block_inode(vnode, inode, split)

    def get(self, vnode, boff):
        """Returns a bytes object for the specified data fragment.
        KeyError if vnode is invalid.
        IndexError if boff is invalid.
        None if the data is inaccessible for some other reason.
        """
        with self.rlock:
            res = self.buf.get(vnode, boff)
            if res is None:
                res = self._fetch_backend(vnode, boff)
        if DEBUG: print("wooram: get: buf[{}:{}]=>len({})".format(vnode,boff,len(res) if res else None), file=sys.stderr)
        return res

    def set(self, vnode, boff, data):
        if len(data) == 0:
            raise ValueError("can't set fragment to empty. Use resize instead.")

        with self.wlock:
            if self.syncing: self.recent.add((vnode, boff))
            self.vtable.change_inode(vnode, boff, len(data))
            self.buf.set(vnode, boff, data)

        if DEBUG: print("wooram: set: buf[{}:{}]<=len({})".format(vnode,boff,len(data) if data else None), file=sys.stderr)

    def new(self):
        return self.vtable.new()

    def delete(self, vnode):
        with self.wlock:
            size = self.num_blocks(vnode)
            if self.syncing:
                self.recent.update((vnode, boff) for boff in range(size))
            self.buf.pop((vnode, boff) for boff in range(size))
            del self.vtable[vnode]

    def resize(self, vnode, size):
        """sets the length in bytes of vnode to the given value."""
        num = math.ceil(size / self.fbsize)
        lbsize = size - self.fbsize*(num-1)
        with self.wlock:
            info = self.vtable.get_info(vnode)
            curnum = len(info.inodes)
            curlbs = info.lbsize
            if num < curnum:
                # truncating
                self.vtable.trunc_inodes(vnode, num)
                if lbsize < self.fbsize:
                    data = self.get(vnode, num-1)[:lbsize]
                    self.set(vnode, num-1, data)
            elif num > curnum:
                # growing
                if curlbs < self.fbsize:
                    # need to pad last block with null bytes
                    data = self.get(vnode, curnum-1) + b'\0'*(self.fbsize - curlbs)
                    assert len(data) == self.fbsize
                    self.set(vnode, curnum-1, data)
                for boff in range(curnum, num-1):
                    self.set(vnode, boff, b'\0'*self.fbsize)
                self.set(vnode, num-1, b'\0'*lbsize)
            elif lbsize != curlbs:
                data = self.get(vnode, num-1)
                if lbsize < curlbs:
                    # truncating last block
                    self.set(vnode, num-1, data[:lbsize])
                else:
                    # growing last block
                    self.set(vnode, num-1, data + b'\0'*(lbsize-curlbs))

    def sync(self):
        evict_ind = random.sample(range(1,self.N), self.K)

        with self.wlock:
            if self.syncing:
                print("WARNING: SYNC OVERLAP!!")
                print("You should decrease the drip_rate or increase the drip_time.")
                print("This sync attempt is aborting. Your privacy may be compromised.")
                return
            self.syncing = True
            self.recent = set()

        with self.rlock:
            evict_blocks = [self._get_fresh(ind) for ind in evict_ind]
            avail = self.buf.available()

        # compute available space, pre-compacting sblocks when possible
        for blist in evict_blocks:
            if all(b.kind() == Block.SPLIT for b in blist):
                # two sblocks. can they fit into one?
                if sum(b.size() for b in blist) <= self.split_maxsize:
                    # yes!
                    blist[0].contents.update(blist[1].contents)
                    blist[1] = Block(self, Block.EMPTY)

        blocks = [b for blist in evict_blocks for b in blist]
        assert len(blocks) == 2*self.K

        # pack items from the buffer
        to_pop = []
        for vnode, boff, data in avail:
            assert len(data) > 0
            # try every block, sorting smallest first so best-fit
            blocks.sort(key=lambda b: b.space_avail())
            for b in blocks:
                if b.add_if(vnode, boff, data):
                    break

        # write back blocks to backend
        for ind, (b1, b2) in zip(evict_ind, evict_blocks):
            self.backend[ind] = self._make_block(b1,b2)

        with self.wlock:
            # update vtable for what was added
            for i, blist in enumerate(evict_blocks):
                inode0 = 2*evict_ind[i]
                for j in range(2):
                    for (vnode, boff) in blist[j].added():
                        if (vnode,boff) not in self.recent:
                            self.vtable.set_inode(vnode, boff, inode0+j)
                            to_pop.append((vnode, boff))

        with self.rlock:
            save_superblock(self.backend, 
                    self.vtable, self.blocksize, self.N, self.headerlen)

        with self.wlock:
            # now that all is set, remove added items from buffer
            self.buf.pop(to_pop)
            self.recent = None
            self.syncing = False


class Syncer(threading.Thread):
    def __init__(self, woo, T):
        super().__init__()
        self.woo = woo
        self.T = T

    def run(self):
        prev_start = time.time()
        while True:
            elapsed = time.time() - prev_start 
            if elapsed > self.T:
                print("WARNING: SYNC TOOK TOO LONG!", file=sys.stderr)
                print("You should decrease the drip_rate or increase the drip_time.", file=sys.stderr)
                print("The elapsed time was {}, which is {} greater than drip time of {}"
                        .format(elapsed, elapsed-self.T, self.T), file=sys.stderr)
                elapsed = self.T
            time.sleep(self.T - elapsed)
            prev_start = time.time()
            with self.woo.rlock:
                if (not self.woo.active and not self.woo.buf 
                        and not self.woo.vtable.has_shadow()):
                    return
            if BUF_MEASURE: print("{} {}".format(len(self.woo.buf), self.woo.buf.size()))
            if DEBUG: print("SYNC begin, buffer size is", len(self.woo.buf), file=sys.stderr)
            self.woo.sync()
            if DEBUG: print("     end SYNC with size", len(self.woo.buf), file=sys.stderr)

if __name__ == '__main__':
    # run some tests
    random.seed(1985)

    import backend
    import os
    # back = []
    backdir = "dbox"
    key = bytes(random.randrange(256) for _ in range(16))
    os.makedirs(backdir, exist_ok=True)
    if os.path.exists(os.path.join(backdir, "0")):
        os.remove(os.path.join(backdir, "0"))
    back = backend.Backend(key, backdir)
    timing = 2
    K = 20
    N = 2**10
    w = load_wooram(back, blocksize=2**16, total_blocks=N, drip_rate=K, drip_time=timing, headerlen=48)
    rounds = 20
    initial = 200
    defsize = 10
    check = {}
    count = 0

    def rblock(n=w.fbsize):
        if n > 20:
            return bytes(random.randrange(256) for _ in range(20)) + b'.'*(n-20)
        else:
            return bytes(random.randrange(256) for _ in range(n))

    def checkit(thew=None):
        global check, count, w
        if thew is None: thew = w
        totsize = 0
        for v in check:
            for i,x in enumerate(check[v]):
                assert thew.get(v,i) == x
                totsize += len(x)
        assert totsize == thew.size()
        count += 1
        print("Passed check", count)

    # create some random stuff
    print("Inserting", initial, "items...", end="")
    sys.stdout.flush()
    for _ in range(initial):
        print(".", end="")
        v = w.new()
        check[v] = [rblock() for _ in range(random.randrange(defsize))]
        if not check[v] or random.randrange(2):
            check[v].append(rblock(random.randrange(w.fbsize-1)+1))
        for i,x in enumerate(check[v]):
            w.set(v,i,x)
    print("done")

    checkit()

    # sync some of the stuff we put in, but not all
    origsize = len(w.buf)
    ns=0
    print("Performing initial syncs")
    while len(w.buf) > 2*K:
        print(".", end="")
        sys.stdout.flush()
        w.sync()
        ns += 1
    print("done")
    print("Took", ns, "syncs to reduce from size", origsize, "down to", len(w.buf))

    print("Current utilization:", w.size()/w.capacity())

    checkit()

    with w:
        if timing > 0:
            print("sync thread started...")

        # do some rounds of messin around
        for rnd in range(rounds):
            print("Round", rnd, "of random operations")

            # change the middle of something
            v = random.choice(list(check))
            lst = check[v]
            off = random.randrange(len(lst))
            lst[off] = rblock()
            w.set(v, off, lst[off])

            # change the end of something
            v = random.choice(list(check))
            lst = check[v]
            off = len(lst) - 1
            assert off >= 0
            lst[off] = rblock(random.randrange(w.fbsize-1)+1)
            w.set(v, off, lst[off])

            # truncate something
            v = random.choice(list(check))
            lst = check[v]
            newlen = random.randrange(len(lst)) + 1
            if newlen < len(lst):
                w.resize(v, newlen*w.fbsize)
                lst[newlen:] = []
                if lst and random.randrange(2):
                    # change the last part
                    off = len(lst) - 1
                    lst[off] = rblock(random.randrange(w.fbsize-1)+1)
                    w.set(v, off, lst[off])

            # grow something
            v = random.choice(list(check))
            lst = check[v]
            oldlen = len(lst)
            newlen = oldlen + random.randrange(oldlen)+1
            lst.extend(None for _ in range(oldlen, newlen))
            for off in range(oldlen-1, newlen-1):
                lst[off] = rblock()
                w.set(v, off, lst[off])
            off = newlen-1
            lst[off] = rblock(random.randrange(w.fbsize-1)+1)
            w.set(v, off, lst[off])

            # remove something
            v = random.choice(list(check))
            del check[v]
            w.delete(v)

            # add something new
            v = w.new()
            lst = [rblock() for _ in range(random.randrange(defsize))]
            lst.append(rblock(random.randrange(w.fbsize-1)+1))
            for i,d in enumerate(lst):
                w.set(v, i, d)
            check[v] = lst

            if timing > 0:
                time.sleep(random.random()*timing)
            else:
                w.sync()

        checkit()

        # sync until buffer is empty
        if timing > 0:
            while len(w.buf):
                print("current buffer length is", len(w.buf))
                time.sleep(timing*5)
        else:
            origsize = len(w.buf)
            ns=0
            while len(w.buf):
                w.sync()
                ns += 1
            print("Took", ns, "syncs to reduce from size", origsize, "down to", len(w.buf))

    if timing > 0:
        print("sync thread ended")

    print("Current utilization:", w.size()/w.capacity())

    checkit()

    w.finish()

    with load_wooram(back, drip_rate=K, drip_time=timing) as nextw:
        print("Reloaded the wooram")
        checkit(nextw)

        # do some rounds of messin around
        for rnd in range(rounds,2*rounds):
            print("Round", rnd, "of random operations")

            # change the middle of something
            v = random.choice(list(check))
            lst = check[v]
            off = random.randrange(len(lst))
            lst[off] = rblock()
            nextw.set(v, off, lst[off])

            # change the end of something
            v = random.choice(list(check))
            lst = check[v]
            off = len(lst) - 1
            assert off >= 0
            lst[off] = rblock(random.randrange(nextw.fbsize-1)+1)
            nextw.set(v, off, lst[off])

            # truncate something
            v = random.choice(list(check))
            lst = check[v]
            newlen = random.randrange(len(lst)) + 1
            if newlen < len(lst):
                nextw.resize(v, newlen*nextw.fbsize)
                lst[newlen:] = []
                if lst and random.randrange(2):
                    # change the last part
                    off = len(lst) - 1
                    lst[off] = rblock(random.randrange(nextw.fbsize-1)+1)
                    nextw.set(v, off, lst[off])

            # grow something
            v = random.choice(list(check))
            lst = check[v]
            oldlen = len(lst)
            newlen = oldlen + random.randrange(oldlen)+1
            lst.extend(None for _ in range(oldlen, newlen))
            for off in range(oldlen-1, newlen-1):
                lst[off] = rblock()
                nextw.set(v, off, lst[off])
            off = newlen-1
            lst[off] = rblock(random.randrange(nextw.fbsize-1)+1)
            nextw.set(v, off, lst[off])

            # remove something
            v = random.choice(list(check))
            del check[v]
            nextw.delete(v)

            # add something new
            v = nextw.new()
            lst = [rblock() for _ in range(random.randrange(defsize))]
            lst.append(rblock(random.randrange(nextw.fbsize-1)+1))
            for i,d in enumerate(lst):
                nextw.set(v, i, d)
            check[v] = lst

            if timing > 0:
                time.sleep(random.random()*timing)
            else:
                w.sync()


        checkit(nextw)

