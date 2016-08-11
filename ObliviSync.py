#!/usr/bin/env python3
from __future__ import print_function, absolute_import, division

import hashlib
import getpass
import binascii
import logging

from collections import defaultdict
from errno import ENOENT, ENODATA, EROFS, EACCES, EIO, EBUSY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from os import O_WRONLY, O_RDWR, O_APPEND

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from backend import Backend

import pickle

# if not hasattr(__builtins__, 'bytes'):
#     bytes = str


import wooram,rooram

WoOram = wooram.WoOram
load_wooram = wooram.load_wooram

RoOram = rooram.RoOram
load_rooram = rooram.load_rooram



import sys

DEBUG=False
DEBUG_FILE=sys.stderr
wooram.DEBUG=False
drip_rate=3
drip_time=3

USAGE = """{} [OPTIONS] <backend> <mountpoint> 
<backend>   \t : where backend files are stored
<mountpoint>\t : where the fuse client mounts

OPTIONS
    -h      \t: print this help screen
    -r      \t: Read-Only mount   (dflt: rw mount)
    -k      \t: set the drip rate (dflt: 3)
    -t      \t: set the drip time (dflt: 3)

    -v      \t: verbose output
    -d file \t: set verbose output to file (dflt: stderr) (use - for stdout)
""".format(sys.argv[0])

import getopt

def parse_args():
    global DEBUG,DEBUG_FILE, drip_rate, drip_time

    opt,args = getopt.getopt(sys.argv[1:], "hvd:rk:t:")

    readwrite=True
    for o,v in opt:
        if o =="-h":
            print(USAGE)
            exit(0)
        if o == "-v":
            DEBUG = True
            wooram.DEBUG= True
        if o == "-d":
            if v != "-":
                DEBUG_FILE = open(v,"w")
            else:
                DEBUG_FILE = sys.stdout
        if o == "-r":
            readwrite=False
        if o == "-k":
            drip_rate=int(v)
        if o == "-t":
            drip_time= int(v)

    if len(args) < 2:
        print(USAGE)
        exit(1)
    
    key = getkey(args[0])
        
    return args[0],args[1],key,readwrite

class ObliviSyncRW(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, backdir='dbox', key=b'0123456789abcdef',
                 drip_time=3,drip_rate=3,blocksize=2**22,total_blocks=2**10):
        
        #load wooram get directory table and the wooram
        self.woo = load_wooram(Backend(key, backdir),
                               drip_time=3,drip_rate=3,
                               blocksize=blocksize,total_blocks=total_blocks)
        self.woo.start() # start the syncer`
        
        #store blocksize
        self.bs = self.woo.fbsize
        

        self.direntry = self.__read_direntry()
        self.data = {} # (content, counter, dirty)
        self.fd = 0
        
        if self.direntry == None: # first mount
            if DEBUG: print("init: creating dir entry", file=DEBUG_FILE)
            now = time()
            header = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                              st_mtime=now, st_atime=now, st_nlink=2, attrs={"vnode":1})

            self.direntry = {"/":header}
            
            #write the new directory entry
            self.__write_direntry()


    def __del__(self):
        #called on filesystem destrcution/unmount?
        self.__write_direntry()
        self.woo.finish() #wait for finish to sync
        if DEBUG: print("finished",file=DEBUG_FILE)
        del self


    def __get_vnode(self,path):
        if path in self.direntry:
            return self.direntry[path]['attrs']['vnode']
        return None

    
    def __read_file(self,vnode):
        #what if woo.get() returns None?
        return b''.join(self.woo.get(vnode,i) for i in range(self.woo.num_blocks(vnode)))

    def __write_file(self,vnode,data,dirty):

        #update the mtime if any blocks are dirty
        if any(map(bool,dirty)):
            self.woo.set_mtime(vnode,time())

        for i in range(0, self.__blocks(len(data))):
            #write back anything that is dirty
            if dirty[i] == 1:
                self.woo.set(vnode,i,data[i*self.bs:(i+1)*self.bs])
                if DEBUG: print("__write_back: vnode: {} : block: {} len: {} DIRTY".format(vnode,i,len(data[i*self.bs: (i+1)*self.bs])),file=DEBUG_FILE)
            else:
                if DEBUG: print("__write_back: vnode: {} : block: {} len: {} CLEAN".format(vnode,i,len(data[i*self.bs: (i+1)*self.bs])),file=DEBUG_FILE)


    def __read_direntry(self):
        data = self.__read_file(1)
        if len(data) == 0: return None
        direntry = pickle.loads(data)
        return direntry

    def __write_direntry(self):
        data = pickle.dumps(self.direntry)
        self.__write_file(1, data,tuple(1 for _ in range(self.__blocks(len(data)))))
        
    def __blocks(self, size):
        if size == 0: return 0
        return size//self.bs + (1 if size%self.bs else 0)

    def chmod(self, path, mode):
        self.direntry[path]['st_mode'] &= 0o770000
        self.direntry[path]['st_mode'] |= mode
        self.woo.set_mtime(self.__get_vnode(path),time())
        self.__write_direntry()#write changes
        return 0


    def chown(self, path, uid, gid):
        #not really implemented ...
        self.direntry[path]['st_uid'] = uid
        self.direntry[path]['st_gid'] = gid

    
    def create(self, path, mode):
                
        now = time()
        vnode = self.woo.new()
        header = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                      st_ctime=time(),
                      st_atime=now, attrs={"vnode":vnode})
        self.woo.set_mtime(vnode,now)

        self.direntry[path]=header
        
        self.__write_direntry()#write now the changes occur in backend/woo
    
        self.data[path] = dict(contents=bytes(), counter=1, dirty=[1])
        

        if DEBUG: print("creat: {} : dirty: {} header: {}".format(path,
                                                      self.data[path]["dirty"],
                                                        self.direntry[path]),file=DEBUG_FILE)

        self.fd += 1
        return self.fd

    def __get_header(self,path):
        

        if path in self.direntry:
            vnode = self.__get_vnode(path)
            if path in self.data:
                size = len(self.data[path]["contents"])
                mtime = time()
            else:
                size = self.woo.get_size(vnode)
                mtime = self.woo.get_mtime(vnode)
        
            return dict(tuple(self.direntry[path].items())+(('st_size',size),(('st_mtime',mtime))))


        return None

    def getattr(self, path, fh=None):
        if DEBUG: print("getattr: path={}".format(path),file=DEBUG_FILE)
        header = self.__get_header(path)
        if header == None:
            raise FuseOSError(ENOENT)
        if DEBUG: print("       : header={}".format(header),file=DEBUG_FILE)
        return header

    def getxattr(self, path, name, position=0):
        if DEBUG: print("getxattr: path={} name={}".format(path,name),file=DEBUG_FILE)
        header = self.__get_header(path)
        attrs = header.get('attrs',{})
        try:
            return attrs[name]
        except KeyError:
            raise FuseOSError(ENODATA)

    def listxattr(self, path):
        if DEBUG: print("listxattr: path={}".format(path),file=DEBUG_FILE)
        header = self.__get_header(path)
        attrs = header.get('attrs',{})
        return [k for k in attrs.keys() if k != 'vnode'] #vnode could be removed by another application!

    # def mkdir(self, path, mode):
    #     self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
    #                             st_size=0, st_ctime=time(), st_mtime=time(),
    #                             st_atime=time())

    #     self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        if DEBUG: print("open: path={} flags={}".format(path,flags),file=DEBUG_FILE)
        self.fd += 1

        #read directory entry
        vnode = self.__get_vnode(path)
        odict = self.data.setdefault(path,{})
        
        #set counter and load blocks if need be
        odict.setdefault("counter",0)
        if odict["counter"] == 0:

            
            data = self.__read_file(vnode)
            dirty = [0 for _ in range(self.__blocks(len(data)))]
            odict['contents']=data
            odict['dirty'] = dirty

            # This is from the ro side, but may not be needed here
            # #still syncing, something off?
            # if len(odict['contents']) != self.direntry[path]['st_size']:
            #     raise FuseOSError(EIO)
            
        odict["counter"]+=1 #increment counter

        if DEBUG: print(" open: {} : dirty: {} header: {}".format(path,self.data[path]["dirty"],
                                                        self.direntry[path]),file=DEBUG_FILE)
                  
        #set dirty flag based on flags

        return self.fd

    def read(self, path, size, offset, fh):
        if DEBUG: print("read: path={} size={} offset={}  fh={}".format(path,size,offset,fh),file=DEBUG_FILE)
        return self.data[path]["contents"][offset:offset + size]

    def readdir(self, path, fh):
        if DEBUG: print("readdir: path={}".format(path),file=DEBUG_FILE)
        #self.__write_direnty()#write direnty for any updates?
        #self.direntry = self.__read_direntry() # reload in case of remote updates?
        return ['.', '..'] + [x[1:] for x in self.direntry if x != '/']

    def readlink(self, path):
        if DEBUG: print("readlink: path={}".format(path),file=DEBUG_FILE)
        return self.data[path]['contents']

    def release(self, path, fh):
        if DEBUG: print("release: path={} : fh: {}".format(path,fh),file=DEBUG_FILE)

        if DEBUG: print("       : counter={} dirty={} blocks={} header={}".format(  self.data[path]["counter"],
                                                                          self.data[path]["dirty"],
                                                                          self.__blocks(len(self.data[path]["contents"])),
                                                                          self.direntry[path]),file=DEBUG_FILE)
        
        self.data[path]["counter"] -= 1
        
        if self.data[path]["counter"] <= 0:
            data = self.data[path]["contents"]
            dirty = self.data[path]["dirty"]
            self.data.pop(path)


            self.__write_file(self.__get_vnode(path),data,dirty)

                    
    def removexattr(self, path, name):
        if DEBUG: print("removexattr: path={} name={}".format(path,name),file=DEBUG_FILE)
                
        attrs = self.direntry[path].get('attrs',{})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENODATA
        

    def rename(self, old, new):
        if DEBUG: print("rename: old={} new={}".format(old,new),file=DEBUG_FILE)
        self.direntry[new] = self.direntry.pop(old)
        self.__write_direntry()#write on renames?
        
    # def rmdir(self, path):
    #     self.files.pop(path)
    #     self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        if DEBUG: print("setxattr: path={} name={} value={} options={}".format(path,name,value,options),file=DEBUG_FILE)
        header = self.__get_header(path)
        attrs = header.setdefault('attrs', {})
        attrs[name] = value
        
    def statfs(self, path):
        if DEBUG: print("statfs: path={}".format(path),file=DEBUG_FILE)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    # def symlink(self, target, source):
    #     self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
    #                               st_size=len(source))

    #     self.data[target] = source

    def truncate(self, path, length, fh=None):
        if DEBUG: print("truncate: path={} length={}".format(path,length),file=DEBUG_FILE)
              
        self.data[path]["contents"] = self.data[path]["contents"][:length]
                
        #slice up dirty to similar length
        self.data[path]["dirty"] = self.data[path]["dirty"][:self.__blocks(length)]

        #if truncated into a middle of block, last block is dirty
        if length % self.bs:
            self.data[path]["dirty"][-1] = 1 

        self.woo.resize(self.__get_vnode(path),length)
        if DEBUG: print("resiz: {} : new_bsize: {}".format(path,len(self.data[path]["dirty"])),file=DEBUG_FILE)

        

    def unlink(self, path):
        if DEBUG: print("unlink: path={}".format(path),file=DEBUG_FILE)
        #read directory entry get vnode
        vnode = self.__get_vnode(path)

        #delete in wooram
        self.woo.delete(vnode)

        #remove from cache
        self.direntry.pop(path)
        self.__write_direntry() #write on remove?
        
    def utimens(self, path, times=None):
        if DEBUG: print("utimens: path={} times={}".format(path,times),file=DEBUG_FILE)
        now = time()
        atime, mtime = times if times else (now, now)
        #don't change access time ...
        self.woo.set_mtime(self.__get_vnode(path),mtime)



    def write(self, path, data, offset, fh):
        if DEBUG: print("write: path={} len(data)={} offset={} fh={}".format(path,len(data), offset, fh),file=DEBUG_FILE)
        
        old_contents = self.data[path]["contents"]

        new_contents = self.data[path]["contents"][:offset] + data
        
        #everything that changed is dirty
        old_dirty = self.data[path]["dirty"]
        new_dirty = []
        for i in range(0, self.__blocks(len(new_contents))):

            #originally empty, always dirty
            if len(old_contents) == 0:
                new_dirty.append(1)
            elif i*self.bs < len(old_contents):
                if new_contents[i*self.bs:(i+1)*self.bs] != old_contents[i*self.bs:(i+1)*self.bs]:
                    new_dirty.append(1)
                else:
                    new_dirty.append(0)
            else:
                new_dirty.append(1)


        self.data[path]["contents"] = new_contents
        
        old_dirty.extend([0]*(len(new_dirty)-len(old_dirty)))
        self.data[path]["dirty"] = [1 if any(d) else 0 for d in zip(new_dirty,old_dirty)]

        if DEBUG: print("     : OLD DIRTY: {} NEW DIRTY: {}".format(old_dirty,new_dirty),file=DEBUG_FILE)
        if DEBUG: print("     : FINISH: dirty: {}  size: {}".format(self.data[path]["dirty"],
                                                          len(self.data[path]['contents'])),file=DEBUG_FILE)

        return len(data)


class ObliviSyncRO(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, backdir='dbox', key=b'0123456789abcdef', thresh=3):
        
        #load wooram get directory table and the wooram
        self.woo = load_rooram(Backend(key, backdir))
        
        #store blocksize
        self.bs = self.woo.fbsize

        self.__thresh_dir = thresh #should be sit to drip rate
        self.__last_dir = 0
        self.direntry = self.__read_direntry()
        self.data = {} # (content, counter, dirty)
        self.fd = 0

        if DEBUG: print(self.direntry,file=DEBUG_FILE)
        
        if self.direntry == None: # first mount
            raise FuseOSError(EROFS)

    def __del__(self):
        #called on filesystem destrcution/unmount?
        if DEBUG: print("finished",file=DEBUG_FILE)
        del self


    def __get_vnode(self,path):
        if path in self.direntry:
            return self.direntry[path]['attrs']['vnode']
        return None

    
    def __read_file(self,vnode):
        #what if woo.get() returns None?
        try:
            return b''.join(self.woo.get(vnode,i) for i in range(self.woo.num_blocks(vnode,update=False)))
        except:
            return b'' #empty if we get a none?

    def __write_file(self,vnode,data,dirty): raise FuseOSError(EROFS)

    def __read_direntry(self):

        #see if we need to update
        now = time()
        if not now-self.__last_dir > self.__thresh_dir:
            return self.direntry

        if DEBUG: print("header: reading header from backend: diff: {}".format(now-self.__last_dir),file=DEBUG_FILE)
        data = self.__read_file(1)
        if len(data) == 0: return None
        direntry = pickle.loads(data)
        self.__last_dir = now
        
        #load in mtime and size without further updates
        for path in direntry:
            vnode = direntry[path]["attrs"]["vnode"]
            direntry[path]["st_mtime"] = self.woo.get_mtime(vnode,update=False)
            direntry[path]["st_size"] = self.woo.get_size(vnode,update=False)
            
            
        return direntry

                

    # def __write_direntry(self):
    #     data = pickle.dumps(self.direntry)
    #     self.__write_file(1, data,tuple(1 for _ in range(self.__blocks(len(data)))))
        
    def __blocks(self, size):
        if size == 0: return 0
        return size//self.bs + (1 if size%self.bs else 0)


    def __get_header(self,path):

        #if already open, must be in entry
        if path in self.data:
            return self.direntry[path]

        #otherwise try and see if we need to update
        self.direntry = self.__read_direntry()

        if not self.direntry: return None #error condition when
                                          #reading in the middle of an
                                          #update and get empty string

        #return the entry or none
        return self.direntry[path] if path in self.direntry else None

    def chmod(self, path, mode): raise FuseOSError(EROFS)
    def chown(self, path, uid, gid): raise FuseOSError(EROFS)
    def create(self, path, mode): raise FuseOSError(EROFS)


    def getattr(self, path, fh=None):
        if DEBUG: print("getattr: path={}".format(path),file=DEBUG_FILE)
        header = self.__get_header(path)
        if header == None:
            raise FuseOSError(ENOENT)
        if DEBUG: print("       : header={}".format(header),file=DEBUG_FILE)
        return header

    def getxattr(self, path, name, position=0):
        if DEBUG: print("getxattr: path={} name={}".format(path,name),file=DEBUG_FILE)
        header = self.__get_header(path)
        attrs = header.get('attrs',{})
        try:
            return attrs[name]
        except KeyError:
            raise FuseOSError(ENODATA)

    def listxattr(self, path):
        if DEBUG: print("listxattr: path={}".format(path),file=DEBUG_FILE)
        header = self.__get_header(path)
        attrs = header.get('attrs',{})
        return [k for k in attrs.keys() if k != 'vnode'] #vnode could be removed by another application!

    # def mkdir(self, path, mode):
    #     self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
    #                             st_size=0, st_ctime=time(), st_mtime=time(),
    #                             st_atime=time())

    #     self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        if DEBUG: print("open: path={} flags={}".format(path,flags),file=DEBUG_FILE)
        self.fd += 1

        if flags & (O_WRONLY | O_RDWR | O_APPEND): raise FuseOSError(EROFS)

        #read directory entry
        self.direntry = self.__read_direntry()
        vnode = self.__get_vnode(path)
        odict = self.data.setdefault(path,{})
        
        #set counter and load blocks if need be
        odict.setdefault("counter",0)
        if odict["counter"] == 0:

            
            data = self.__read_file(vnode)
            dirty = [0 for _ in range(self.__blocks(len(data)))]
            odict['contents']=data
            odict['dirty'] = dirty

            #still syncing, something off?
            if len(odict['contents']) != self.direntry[path]['st_size']:
                raise FuseOSError(EIO)
            
        odict["counter"]+=1 #increment counter

        if DEBUG: print(" open: {} : dirty: {} header: {}".format(path,self.data[path]["dirty"],
                                                        self.direntry[path]),file=DEBUG_FILE)
                  
        #set dirty flag based on flags

        return self.fd

    def read(self, path, size, offset, fh):
        if DEBUG: print("read: path={} size={} offset={}  fh={}".format(path,size,offset,fh),file=DEBUG_FILE)
        return self.data[path]["contents"][offset:offset + size]

    def readdir(self, path, fh):
        if DEBUG: print("readdir: path={}".format(path),file=DEBUG_FILE)
        self.direntry = self.__read_direntry()
        return ['.', '..'] + [x[1:] for x in self.direntry if x != '/']

    def readlink(self, path):
        if DEBUG: print("readlink: path={}".format(path),file=DEBUG_FILE)
        return self.data[path]['contents']

    def release(self, path, fh):
        if DEBUG: print("release: path={} : fh: {}".format(path,fh),file=DEBUG_FILE)

        if DEBUG: print("       : counter={} dirty={} blocks={} header={}".format(  self.data[path]["counter"],
                                                                          self.data[path]["dirty"],
                                                                          self.__blocks(len(self.data[path]["contents"])),
                                                                          self.direntry[path]),file=DEBUG_FILE)
        
        self.data[path]["counter"] -= 1
        
        if self.data[path]["counter"] <= 0:
            self.data.pop(path)

                    
    def removexattr(self, path, name): raise FuseOSError(EROFS)
    def rename(self, old, new): raise FuseOSError(EROFS)        
    def rmdir(self, path): raise FuseOSError(EROFS)        
    def setxattr(self, path, name, value, options, position=0): raise FuseOSError(EROFS)        
        
    def statfs(self, path):
        if DEBUG: print("statfs: path={}".format(path),file=DEBUG_FILE)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source): raise FuseOSError(EROFS)        
    def truncate(self, path, length, fh=None): raise FuseOSError(EROFS)        
    def unlink(self, path): raise FuseOSError(EROFS)        
        
    def utimens(self, path, times=None):
        if DEBUG: print("utimens: path={} times={}".format(path,times),file=DEBUG_FILE)
        now = time()
        atime, mtime = times if times else (now, now)
        #don't change access time ...

    def write(self, path, data, offset, fh):  raise FuseOSError(EROFS)        

def getkey(dirname):
    pwstring = getpass.getpass("Enter passphrase for directory {}: ".format(dirname))
    hasher = hashlib.new('sha256')
    hasher.update(bytes(pwstring, 'utf8'))
    key = hasher.digest()[:16]
    return key

if __name__ == '__main__':

    backdir,mountdir,key,readwrite = parse_args()
    if readwrite:
        fuse = FUSE(ObliviSyncRW(backdir, key,drip_rate=drip_rate,drip_time=drip_time), mountdir, foreground=True)
    else:
        fuse = FUSE(ObliviSyncRO(backdir, key), mountdir, foreground=True)
