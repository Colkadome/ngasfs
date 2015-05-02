#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT, ENOATTR
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from sqlobject import *
import os

if not hasattr(__builtins__, 'bytes'):
    bytes = str

from tables import *

"""
A SQL-based filesystem.
"""
class FS(LoggingMixIn, Operations):

    def __init__(self, db_name):
        initDB(db_name)
        self.fd = 0
        self.verbose = True     # print all activities

    def chmod(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** chmod", path, mode]))
        f = getFileFromPath(path)
        f.st_mode &= 0770000
        f.st_mode |= mode
        return 0

    def chown(self, path, uid, gid):
        if self.verbose:
            print ' '.join(map(str, ["*** chown", path, uid, gid]))
        f = getFileFromPath(path)
        f.st_uid = uid
        f.st_gid = gid

    def create(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** create", path, mode]))
        now = time()
        File(name=getFileName(path), path=getFilePath(path), st_mode=(S_IFREG | mode), st_nlink=1,
                st_size=0, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc=None, attrs={}, is_downloaded=False)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if self.verbose:
            print ' '.join(map(str, ["*** getattr", path, fh]))
        f = getFileFromPath(path)
        attrs = {}
        attrs["st_mode"] = f.st_mode
        attrs["st_nlink"] = f.st_nlink
        attrs["st_size"] = f.st_size
        attrs["st_ctime"] = f.st_ctime
        attrs["st_mtime"] = f.st_mtime
        attrs["st_atime"] = f.st_atime
        attrs["st_uid"] = f.st_uid
        attrs["st_gid"] = f.st_gid
        return attrs

    def getxattr(self, path, name, position=0):
        if self.verbose:
            print ' '.join(map(str, ["*** getxattr", path, name, position]))
        try:
            return getFileFromPath(path).attrs[name]
        except KeyError:
            raise FuseOSError(ENOATTR)

    def listxattr(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** listxattr", path]))
        return getFileFromPath(path).attrs.keys()

    def mkdir(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** mkdir", path, mode]))
        now = time()
        File(name=getFileName(path), path=getFilePath(path), st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc=None, attrs={}, is_downloaded=False)
        getParentFromPath(path).st_nlink += 1

    def open(self, path, flags):
        if self.verbose:
            print ' '.join(map(str, ["*** open", path, flags]))
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** read", path, size, offset, fh]))
        f = getFileFromPath(path)
        f._check_download() # check for data!

        i_start = int(offset / BLOCK_SIZE)
        i_end = int((offset + size) / BLOCK_SIZE)

        data = ""
        for d in Data.selectBy(file_id=f.id).filter(Data.q.series>=i_start).filter(Data.q.series<=i_end).orderBy("series"):
            s = 0
            e = BLOCK_SIZE
            if d.series == i_start:
                s = offset % BLOCK_SIZE
            if d.series == i_end:
                e = (offset + size) % BLOCK_SIZE
            data += d.data[s:e]

        return data

    def readdir(self, path, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** readdir", path, fh]))
        res = []
        for f in File.selectBy(path=path):
            if f.name:
                res.append(f.name)
        return ['.', '..'] + res

    def readlink(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** readlink", path]))
        return getFileFromPath(path).data # FIX THIS

    def removexattr(self, path, name):
        if self.verbose:
            print ' '.join(map(str, ["*** removexattr", path, name]))
        try:
            del getFileFromPath(path).attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        if self.verbose:
            print ' '.join(map(str, ["*** rename", old, new]))
        f = getFileFromPath(old)
        # download file
        f._check_download()
        f.server_loc = None
        # rename file
        f.name = getFileName(new)
        f.path = getFilePath(new)

    def rmdir(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** rmdir", path]))
        if File.select(File.q.path.startswith(path)).count():
            print "--- " + path + " has contents, was not removed."
        else:
            getFileFromPath(path).destroySelf()
            getParentFromPath(path).st_nlink -= 1

    def setxattr(self, path, name, value, options, position=0):
        if self.verbose:
            print ' '.join(map(str, ["*** setxattr", path, name, value, options, position]))
        # Ignore options
        getFileFromPath(path).attrs[name] = value

    def statfs(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** statfs", path]))
        return dict(f_bsize=65536, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        if self.verbose:
            print ' '.join(map(str, ["*** symlink", target, source]))
        f = getFileFromPath(target)
        f.st_mode = (S_IFLNK | 0777)
        f.st_nlink = 1
        f.st_size = len(source)
        f.data = source # FIX THIS

    def truncate(self, path, length, fh=None):
        if self.verbose:
            print ' '.join(map(str, ["*** truncate", path, length, fh]))
        f = getFileFromPath(path)
        f.data = f.data[:length] # FIX THIS
        f.st_size = length

    def unlink(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** unlink", path]))
        f = getFileFromPath(path)
        Data.deleteBy(file_id=f.id)
        f.destroySelf()

    def utimens(self, path, times=None):
        if self.verbose:
            print ' '.join(map(str, ["*** utimens", path, times]))
        now = time()
        atime, mtime = times if times else (now, now)
        f = getFileFromPath(path)
        f.st_atime = atime
        f.st_mtime = mtime

    def write(self, path, data, offset, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** write", path, offset, fh]))
        f = getFileFromPath(path)

        size = len(data)
        i_start = int(offset / BLOCK_SIZE)
        i_end = int((offset + size) / BLOCK_SIZE)
        write = 0

        entries = list(Data.selectBy(file_id=f.id).filter(Data.q.series>=i_start).filter(Data.q.series<=i_end).orderBy("series"))
        e_no = 0
        for i in range(i_start, i_end+1):
            s = 0
            e = BLOCK_SIZE
            if i==i_start:
                s = offset % BLOCK_SIZE
            if i==i_end:
                e = (offset + size) % BLOCK_SIZE
            to_write = e - s
            if e_no < len(entries):
                entries[e_no].data = entries[e_no].data[:s] + data[write:write+to_write] + entries[e_no].data[e:]
                e_no += 1
            else:
                Data(file_id=f.id, series=i, data=data[write:write+to_write])
            write += to_write

        f.st_size = max(f.st_size, offset + size)
        return size


if __name__ == '__main__':
    if len(argv) < 3:
        print('usage: %s <mountpoint> <db_name>' % argv[0])
        exit(1)

    foreground = False # DOES NOT WORK if file needs to be downloaded. ??
    if "-f" in argv:
        foreground = True

    debug = False
    if "-d" in argv:
        debug = True

    #sys.stdout = open("out.txt", "a", 0)
    #sys.stderr = open("err.txt", "a", 0)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(FS(argv[2]), argv[1], foreground=foreground, debug=debug) #daemon_timeout = 10000, entry_timeout = 10000, attr_timeout = 10000)