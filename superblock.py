#!/usr/bin/env python3

import collections
import pickle
from vtable import create_vtable, load_vtable

_VERSION = 3

SuperBlock = collections.namedtuple("SuperBlock", 
        ["vtable", "blocksize", "total_blocks", "headerlen", "fbsize", "split_maxnum", "split_maxsize"])

def calc_sizes(blocksize, headerlen):
    fbsize = (blocksize - headerlen - 200) // 2
    max_splits = 2**10
    sbsize = fbsize - 10*max_splits # TODO kind of hackish, possibly innacurate
    assert all(x>0 for x in (fbsize, max_splits, sbsize))
    return fbsize, max_splits, sbsize

def new_superblock(bsize, N, headlen):
    global _VERSION
    fbsize, max_splits, sbsize = calc_sizes(bsize, headlen)
    return SuperBlock(create_vtable(fbsize, sbsize), 
            bsize, N, headlen, fbsize, max_splits, sbsize)

def save_superblock(backend, vtable, bsize, N, headlen):
    global _VERSION
    assert N >= 1 and bsize > headlen >= 0
    data = pickle.dumps((vtable.save(), bsize, N, headlen, _VERSION))
    if len(data) + headlen > bsize:
        raise ValueError("superblock is too big")
    data = data + b'\0'*(bsize-headlen-len(data))
    backend[0] = data

def load_superblock(backend):
    global _VERSION
    try:
        raw = backend[0]
    except IndexError:
        raise ValueError("backend has no superblock file")
    try:
        vtsave, bsize, N, headlen, vers = pickle.loads(raw)
        fbsize, max_splits, sbsize = calc_sizes(bsize, headlen)
        vtab = load_vtable(vtsave, fbsize, sbsize)
    except:
        raise ValueError("couldn't unpickle superblock")
    if vers == _VERSION:
        return SuperBlock(vtab, bsize, N, headlen, fbsize, max_splits, sbsize)
    else:
        raise ValueError("superblock created from incompatible version")
