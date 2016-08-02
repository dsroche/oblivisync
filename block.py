#!/usr/bin/env python3

class Block:
    """represents space available in a single block."""

    """3 kinds of blocks"""
    EMPTY = 1
    SPLIT = 2
    FULL = 3

    def __init__(self, woo, kind, contents=None):
        self.woo = woo # the parent wooram object
        self._kind = kind # should be one of EMPTY, SPLIT, or FULL
        self.contents = contents # depends on the kind
        self._added = []

    def __eq__(self, other):
        try:
            return (self.woo is other.woo and self._kind == other._kind 
                    and self.contents == other.contents)
        except:
            return False

    def kind(self):
        return self._kind

    def size(self):
        if self._kind == self.SPLIT:
            return sum(len(dat) for dat in self.contents.values())
        elif self._kind == self.FULL:
            return len(self.contents[1])
        else:
            return 0

    def space_avail(self):
        """how many bytes can fit"""
        if self._kind == self.EMPTY:
            return self.woo.fbsize
        elif (self._kind == self.SPLIT 
                and len(self.contents) < self.woo.split_maxnum):
            return self.woo.split_maxsize - self.size()
        else:
            return 0

    def add_if(self, vnode, boff, data):
        """if it fits, add it and return True. Else return False."""
        if self._kind == self.EMPTY and len(data) > self.woo.split_maxsize:
            # has to be an fblock
            self._kind = self.FULL
            self.contents = (vnode, data)
            self._added.append((vnode,boff))
            return True
        elif len(data) <= self.space_avail():
            if self._kind == self.EMPTY:
                self._kind = self.SPLIT
                self.contents = {}
            self.contents[vnode] = data
            self._added.append((vnode,boff))
            return True
        else:
            return False

    def added(self):
        """get list of (vnode, boff) pairs for what was added"""
        return self._added

