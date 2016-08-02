#!/usr/bin/env python3

import collections
import time

def LRUlist(defcache=-1):
    """This is a class decorator that adds a least recently used cache
    on top of your existing list-like class.

    Besides the normal collection stuff (notably __getitem__ and __setitem__),
    the decorated class may provide the following additional method:
    _is_stale(key, timestamp) -> bool
    to indicate when it is no longer OK to use the cached value.
    The timestamp pased to _is_stale will be the time when that key
    was last looked up.
    If not provided, the default _is_stale always returns False.

    The argument to the decorator is the default cache size. A keyword
    argument 'cache_size' will also be added to the constructor.
    """
    def decorate(cls):

        class Cached(cls):
            def __init__(self, *args, **kwargs):
                if 'cache_size' in kwargs:
                    self.__max_cache = kwargs['cache_size']
                    del kwargs['cache_size']
                else:
                    self.__max_cache = None
                
                super().__init__(*args, **kwargs)
                
                if self.__max_cache is None:
                    self.__max_cache = defcache
                self.__cache = collections.OrderedDict()

                if not hasattr(self, '_is_stale'):
                    # default _is_stale never expires anything
                    self._is_stale = lambda key, timestamp: False

            @property
            def cache(self):
                return self.__cache

            @property
            def cache_size(self):
                return self.__max_cache

            @cache_size.setter
            def cache_size(self, newmax):
                self.__max_cache = newmax
                self.__maybe_evict()

            def __maybe_evict(self):
                """Check size of cache and possibly evict whatever was least recently
                used."""
                if self.__max_cache > 0:
                    while len(self.__cache) > self.__max_cache:
                        self.__cache.popitem(False)

            def __getitem__(self, key):
                try:
                    res, timestamp = self.__cache[key]
                    stale = self._is_stale(key, timestamp)
                except KeyError:
                    stale = True
                if stale:
                    res = super().__getitem__(key)
                    self.__cache[key] = (res, time.time())
                    self.__maybe_evict()
                self.__cache.move_to_end(key)
                return res

            def __setitem__(self, key, val):
                super().__setitem__(key, val)
                self.__cache[key] = (val, time.time())
                self.__maybe_evict()

            def __contains__(self, key):
                return key in self.__cache or super().__contains__(key)

            def __delitem__(self, key):
                try:
                    del self.__cache[key]
                except KeyError:
                    pass
                super().__delitem__(key)

            def clear(self):
                super().clear()
                self.__cache.clear()

            def pop(self, key=None, *args):
                if key is None:
                    key = super().__len__() - 1
                res = super().pop(key, *args)
                try:
                    del self.__cache[key]
                except KeyError:
                    pass

            def append(self, val):
                super().append(val)
                self.__cache[super().__len__()-1] = (val, time.time())
                self.__maybe_evict()

            def extend(self, iterable):
                saved = list(iterable)
                startind = super().__len__()
                super().extend(saved)
                now = time.time()
                self.__cache.update(((startind+i),(val,now)) for (i,val) in enumerate(saved))
                self.__maybe_evict()

            def insert(self, ind, val):
                super().insert(ind, val)
                torem = [key for key in self.__cache if key >= ind]
                for key in torem:
                    del self.__cache[key]
                self.__cache[key] = (val, time.time())
                self.__maybe_evict()

            def remove(self, val):
                ind = super().index(val)
                del self[ind]

        Cached.__name__ == cls.__name__
        Cached.__module__ = cls.__module__
        Cached.__doc__ = cls.__doc__

        return Cached
    
    return decorate

