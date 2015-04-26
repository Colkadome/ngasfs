#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT, ENOATTR
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from sqlobject import *
import ntpath
from urllib2 import urlopen, URLError, HTTPError
import sys
import os

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class File(SQLObject):
    """
    File SQL column.
    """
    path = UnicodeCol(notNone=True)
    st_mode = IntCol(notNone=False)
    st_nlink = IntCol(notNone=False)
    st_size = IntCol(notNone=False)
    st_ctime = FloatCol(notNone=False)
    st_mtime = FloatCol(notNone=False)
    st_atime = FloatCol(notNone=False)
    st_uid = IntCol(notNone=False)
    st_gid = IntCol(notNone=False)
    server_loc = StringCol(notNone=False)
    attrs = PickleCol(notNone=False)
    data = BLOBCol(notNone=False)   # possibly set to True (value = "")
    path_index = DatabaseIndex("path")

    def _check_download(self):
        if not self.data and self.server_loc:
            print "=== DOWNLOADING " + self.path
            url = self.server_loc + "RETRIEVE?file_id=" + ntpath.basename(self.path)
            con = urlopen(url)
            self.data = con.read()


class FS(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, db_name):

        # check for the '.sqlite' extension on db_path
        if not db_name.endswith('.sqlite'):
            db_name = db_name + ".sqlite"

        # connect to SQL database
        connection_string = 'sqlite:' + os.path.realpath(db_name) # uses full path to stop daemon error
        connection = connectionForURI(connection_string)
        sqlhub.processConnection = connection

        # create tables
        File.createTable(ifNotExists=True)

        # create root entry (if not exists)
        now = time()
        if File.select().count() == 0:
            File(path="/", st_mode=(S_IFDIR | 0755), st_nlink=2,
                st_size=0, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc=None, attrs={}, data="")
            # TEST FILE
            File(path="/pic.png", st_mode=(S_IFREG | 0755), st_nlink=1,
                st_size=395403, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc="http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/",
                attrs={}, data="")

        self.fd = 0
        self.verbose = True     # print all activities

    def chmod(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** chmod", path, mode]))
        f = File.selectBy(path = path).getOne()
        f.st_mode &= 0770000
        f.st_mode |= mode
        return 0

    def chown(self, path, uid, gid):
        if self.verbose:
            print ' '.join(map(str, ["*** chown", path, uid, gid]))
        f = File.selectBy(path = path).getOne()
        f.st_uid = uid
        f.st_gid = gid

    def create(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** create", path, mode]))
        now = time()
        File(path=path, st_mode=(S_IFREG | mode), st_nlink=1,
                st_size=0, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc=None, attrs={}, data="")

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if self.verbose:
            print ' '.join(map(str, ["*** getattr", path, fh]))
        try:
            f = File.selectBy(path = path).getOne()
            attrs = {}
            if f.st_mode != None:
                attrs["st_mode"] = f.st_mode
            if f.st_nlink != None:
                attrs["st_nlink"] = f.st_nlink
            if f.st_size != None:
                attrs["st_size"] = f.st_size
            if f.st_ctime != None:
                attrs["st_ctime"] = f.st_ctime
            if f.st_mtime != None:
                attrs["st_mtime"] = f.st_mtime
            if f.st_atime != None:
                attrs["st_atime"] = f.st_atime
            if f.st_uid != None:
                attrs["st_uid"] = f.st_uid
            if f.st_gid != None:
                attrs["st_gid"] = f.st_gid
            return attrs
        except SQLObjectNotFound:
            raise FuseOSError(ENOENT)

    def getxattr(self, path, name, position=0):
        if self.verbose:
            print ' '.join(map(str, ["*** getxattr", path, name, position]))
        try:
            return File.selectBy(path=path).getOne().attrs[name]
        except KeyError:
            raise FuseOSError(ENOATTR)

    def listxattr(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** listxattr", path]))
        return File.selectBy(path=path).getOne().attrs.keys()

    def mkdir(self, path, mode):
        if self.verbose:
            print ' '.join(map(str, ["*** mkdir", path, mode]))
        now = time()
        File(path=path, st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=now, st_mtime=now,
                st_atime=now, st_uid=0, st_gid=0,
                server_loc=None, attrs={}, data="")
        File.selectBy(path="/").getOne().st_nlink += 1

    def open(self, path, flags):
        if self.verbose:
            print ' '.join(map(str, ["*** open", path, flags]))
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** read", path, size, offset, fh]))
        f = File.selectBy(path=path).getOne()
        f._check_download()
        return f.data[offset:offset + size]

    def readdir(self, path, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** readdir", path, fh]))
        res = []
        for f in File.select():
            if f.path != path and ntpath.dirname(f.path) == path:
                res.append(ntpath.basename(f.path))
        return ['.', '..'] + res

    def readlink(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** readlink", path]))
        return File.selectBy(path=path).getOne().data

    def removexattr(self, path, name):
        if self.verbose:
            print ' '.join(map(str, ["*** removexattr", path, name]))
        try:
            del File.selectBy(path=path).getOne().attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        if self.verbose:
            print ' '.join(map(str, ["*** rename", old, new]))
        File.selectBy(path=old).getOne().path = new

    def rmdir(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** rmdir", path]))
        File.selectBy(path=path).getOne().destroySelf()
        File.selectBy(path="/").getOne().st_nlink -= 1

    def setxattr(self, path, name, value, options, position=0):
        if self.verbose:
            print ' '.join(map(str, ["*** setxattr", path, name, value, options, position]))
        # Ignore options
        File.selectBy(path=path).getOne().attrs[name] = value

    def statfs(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** statfs", path]))
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        if self.verbose:
            print ' '.join(map(str, ["*** symlink", target, source]))
        f = File.selectBy(path=target).getOne()
        f.st_mode = (S_IFLNK | 0777)
        f.st_nlink = 1
        f.st_size = len(source)
        f.data = source

    def truncate(self, path, length, fh=None):
        if self.verbose:
            print ' '.join(map(str, ["*** truncate", path, length, fh]))
        f = File.selectBy(path=path).getOne()
        f.data = f.data[:length]
        f.st_size = length

    def unlink(self, path):
        if self.verbose:
            print ' '.join(map(str, ["*** unlink", path]))
        File.selectBy(path=path).getOne().destroySelf()

    def utimens(self, path, times=None):
        if self.verbose:
            print ' '.join(map(str, ["*** utimens", path, times]))
        now = time()
        atime, mtime = times if times else (now, now)
        f = File.selectBy(path=path).getOne()
        f.st_atime = atime
        f.st_mtime = mtime

    def write(self, path, data, offset, fh):
        if self.verbose:
            print ' '.join(map(str, ["*** write", path, offset, fh]))
        f = File.selectBy(path=path).getOne()
        f.data = f.data[:offset] + data
        f.st_size = len(f.data)
        return len(data)


if __name__ == '__main__':
    if len(argv) < 3:
        print('usage: %s <mountpoint> <db_name>' % argv[0])
        exit(1)

    foreground = False # DOES NOT WORK if file needs to be downloaded. ??
    if "-f" in argv:
        foreground = True

    sys.stdout = open("out.txt", "a", 0)
    sys.stderr = open("err.txt", "a", 0)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(FS(argv[2]), argv[1], foreground=foreground)