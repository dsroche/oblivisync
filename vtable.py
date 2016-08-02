#!/usr/bin/env python3

import collections
import time
from rwlock import get_rw_locks

# TODO there should be a btree setup as well...

VTableData = collections.namedtuple("VTableData", ["next_free", "free", "cache"])

VTEntry = collections.namedtuple("VTEntry", ["mtime", "lbsize", "inodes"])

def create_vtable(fbsize, sbmax):
    res = VTable(fbsize, sbmax)
    res.next_free = VTable._ROOT_VNODE + 1
    res.free = set()
    res.cache = {VTable._ROOT_VNODE: VTEntry(time.time(), fbsize, [])}
    return res

def load_vtable(data, fbsize, sbmax):
    """data should be a VTableData object."""
    res = VTable(fbsize, sbmax)
    res.next_free = data.next_free
    res.free = set(data.free)
    res.cache = data.cache
    return res

class VTable:
    """Stores vnode->[inode list] mappings, as well as size and mtime."""
    _ROOT_VNODE = 1
    """Special inode values"""
    _STALE = -2

    def __init__(self, fbsize, sbmax):
        self.shadow = {}
        self.rlock, self.wlock = get_rw_locks()
        self.fbsize = fbsize
        self.sbmax = sbmax

    def save(self):
        """Returns a VTableData object"""
        # TODO check overflow and write back a btree node
        save_cache = dict(self.cache)
        save_cache.update(self.shadow)
        return VTableData(self.next_free, list(self.free), save_cache)

    def new(self):
        with self.wlock:
            if self.free:
                res = self.free.pop()
            else:
                res = self.next_free
                self.next_free += 1
            self.cache[res] = VTEntry(time.time(), self.fbsize, [])
        return res

    def has_shadow(self):
        return bool(self.shadow)

    def _unpack_inodes(self, info):
        """returns a list of (inode, issplit) tuples"""
        if info.lbsize <= self.sbmax:
            res = [(inode, False) for inode in info.inodes[:-1]]
            res.append((info.inodes[-1], True))
            return res
        else:
            return [(inode, False) for inode in info.inodes]

    def is_stale(self, vnode, inode):
        """Assuming a fragment with given vnode is found at given inode,
        is it safe to be removed?"""
        infos = []
        with self.rlock:
            try:
                infos.append(self.shadow[vnode])
            except KeyError:
                pass
            try:
                infos.append(self.cache[vnode])
            except KeyError:
                pass

        # only remove if it's not in EITHER list
        for info in infos:
            for (tin, issplit) in self._unpack_inodes(info):
                if issplit:
                    if tin//2 == inode//2:
                        return False
                elif tin == inode:
                    return False

        # TODO look in btree
        return True

    def __delitem__(self, vnode):
        with self.wlock:
            if vnode == self.next_free - 1:
                self.next_free -= 1
                while self.next_free-1 in self.free:
                    self.next_free -= 1
                    self.free.remove(self.next_free)
            else:
                self.free.add(vnode)
            del self.cache[vnode]
            try:
                del self.shadow[vnode]
            except KeyError:
                pass

    def __contains__(self, key):
        with self.rlock:
            if vnode in self.free:
                return False
            elif vnode in self.cache:
                return True
            # TODO look in btree
        return False

    def get_inodes(self, vnode):
        """Returns a list of (inode, issplit) pairs for the given vnode."""
        return self._unpack_inodes(self.get_info(vnode))
        # TODO look in btree
    
    def get_size(self, vnode):
        info = self.get_info(vnode)
        return self.fbsize * (len(info.inodes) - 1) + info.lbsize

    def get_mtime(self, vnode):
        return self.get_info(vnode).mtime

    def set_mtime(self, vnode, when):
        with self.wlock:
            oldtime, lbsize, inodes = self.get_info(vnode)
            self.cache[vnode] = VTEntry(when, lbsize, inodes)

    def trunc_inodes(self, vnode, newlen):
        """truncates the inode list to the given length"""
        now = time.time()
        with self.wlock:
            prevtime, lbsize, inodes = self.get_info(vnode)
            assert newlen < len(inodes)
            inodes = inodes[:newlen]
            if vnode in self.shadow and all(i>=0 for i in inodes):
                # totally synced; drop from shadow
                del self.shadow[vnode]
            self.cache[vnode] = VTEntry(now, self.fbsize, inodes)

    def change_inode(self, vnode, boff, size):
        """sets the given vnode list at offset boff to a value that indicates
        that item is in the buffer. Also updates the size of the given block."""
        now = time.time()
        assert boff >= 0 and size > 0
        with self.wlock:
            prevtime, lbsize, inodes = self.get_info(vnode)
            if vnode not in self.shadow:
                assert all(inode >= 0 for inode in inodes)
                self.shadow[vnode] = VTEntry(prevtime, lbsize, list(inodes))
            if boff == len(inodes):
                # appending; make sure previous block is full
                if lbsize != self.fbsize:
                    raise ValueError("Invalid block size; can't append until last block is full.")
                inodes.append(self._STALE)
                lbsize = size
            elif boff == len(inodes)-1:
                # changing last block
                inodes[-1] = self._STALE
                lbsize = size
            else:
                # changing middle block; make sure it's full
                if size != self.fbsize:
                    raise ValueError("Block {} of vnode {} is not at the end, so it must be a full block"
                            .format(boff, vnode))
                inodes[boff] = self._STALE
            self.cache[vnode] = VTEntry(now, lbsize, inodes)

    def set_inode(self, vnode, boff, inode):
        """sets the given vnode list, at offset boff, to inode.
        This doesn't change the modification time; it should be called when you are
        syncing something to the backend."""
        with self.wlock:
            mtime, lbsize, inlst = self.get_info(vnode)
            inlst[boff] = inode
            self.cache[vnode] = VTEntry(mtime, lbsize, inlst)
            if vnode in self.shadow and all(i>=0 for i in inlst):
                # totally synced; drop shadow copy
                del self.shadow[vnode]

    def get_info(self, vnode):
        with self.rlock:
            if vnode in self.free:
                raise KeyError("free'd vnode " + str(vnode))
            elif vnode in self.cache:
                return self.cache[vnode]
            # TODO read from btree
        raise KeyError("vnode not found: " + str(vnode))

    def __iter__(self):
        yield from self.cache
        # TODO read from btree

    def __len__(self):
        res = len(self.cache)
        # TODO read from btree
        return res
