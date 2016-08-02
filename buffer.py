#!/usr/bin/env python3

import collections

class Buffer:
    def __init__(self):
        self.lst = collections.OrderedDict() # (vnode, boff) -> data

    def __len__(self):
        return len(self.lst)

    def size(self):
        return sum(len(d) for d in self.lst.values())

    def get(self, vnode, boff):
        try:
            return self.lst[vnode,boff]
        except KeyError:
            return None

    def set(self, vnode, boff, data):
        try:
            self.lst.move_to_end((vnode,boff))
        except KeyError:
            pass
        self.lst[vnode, boff] = data

    def available(self):
        """Returns an list of (vnode, boff, data) tuples that
        can be popped, in FIFO order."""
        return [(v,b,d) for ((v,b),d) in self.lst.items()]

    def pop(self, items, sync=False):
        """Given a list of (vnode, boff) pairs, removes those items from the
        buffer."""
        for x in items:
            if x in self.lst:
                del self.lst[x]
