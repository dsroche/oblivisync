#!/usr/bin/env python3

import sys
import random
import pickle
import time
import math
import threading

from block import Block
from vtable import VTable
from superblock import load_superblock
from rwlock import get_rw_locks

DEBUG=False

def load_rooram(backend):
    """Attempts to load a wooram object from the given backend.
    Opens in read-only mode."""
    return RoOram(backend)

class RoOram:
    def __init__(self, backend):
        self.backend = backend
        self.rlock, self.wlock = get_rw_locks()

        # self.supdate() and also get the parameters
        sup = load_superblock(self.backend)
        self.vtable = sup.vtable
        self.blocksize = sup.blocksize
        self.headerlen = sup.headerlen
        self.N = sup.total_blocks
        self.fbsize = sup.fbsize
        self.split_maxnum = sup.split_maxnum
        self.split_maxsize = sup.split_maxsize

    def supdate(self):
        """Updates vtable from superblock, if necessary."""
        # TODO check if it's actually been changed before re-reading
        with self.wlock:
            self.vtable = load_superblock(self.backend).vtable

    def start(self):
        pass

    def finish(self):
        pass

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, et, ev, tb):
        self.finish()

    def __len__(self):
        """Returns the number of distinct items stored (i.e., # of vnodes)"""
        self.supdate()
        return len(self.vtable)

    def size(self):
        """Returns the total size (in bytes) of all items stored. (Warning: slow)"""
        self.supdate()
        with self.rlock:
            return sum(self.get_size(v) for v in self.vtable)

    def num_blocks(self, vnode, update=True):
        """Returns the number of blocks this file occupies."""
        if update: self.supdate()
        return len(self.vtable.get_info(vnode).inodes)

    def get_size(self, vnode, update=True):
        """The number of bytes of data stored for the given object."""
        if update: self.supdate()
        return self.vtable.get_size(vnode)
    
    def get_mtime(self, vnode,update=True):
        """The last modification time of the given object."""
        if update: self.supdate()
        return self.vtable.get_mtime(vnode)


    
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

    def get(self, vnode, boff):
        res = None
        self.supdate()
        with self.rlock:
            inode, split = self.vtable.get_inodes(vnode)[boff]
            if inode >= 0:
                res = self._fetch_block_inode(vnode, inode, split)
        if DEBUG: print("rooram: get: buf[{}:{}]=>len({})".format(vnode,boff,len(res) if res else None), file=sys.stderr)
        return res

