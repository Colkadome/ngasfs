#!/usr/bin/env python

from errno import ENOENT, ENOATTR
from time import time
from stat import S_IFDIR, S_IFREG

from fuse import FuseOSError

from sqlobject import *
from urllib2 import urlopen, URLError, HTTPError
import os

# globals
BLOCK_SIZE = 65536
FS_SPECIFIC_PATH = "FS_files"

"""
File column in SQL database.
"""
class File(SQLObject):
    name = UnicodeCol(notNone=True) # basename (eg. "file1.txt")
    path = UnicodeCol(notNone=True) # the path to the file's directiory (e. "m/n/o")
    st_mode = IntCol(notNone=True)
    st_nlink = IntCol(notNone=True)
    st_size = IntCol(notNone=True)
    st_ctime = FloatCol(notNone=True)
    st_mtime = FloatCol(notNone=True)
    st_atime = FloatCol(notNone=True)
    st_uid = IntCol(notNone=True)
    st_gid = IntCol(notNone=True)
    attrs = PickleCol(notNone=False)
    server_loc = StringCol(notNone=False)
    is_downloaded = BoolCol(notNone=True)   # possibly set to True (value = "")
    path_index = DatabaseIndex("path")

    # DOES NOT WORK WITH DAEMON. The download might have to be done in a separate process
    def _check_download(self):
        """
        >>> dum = _check_download()
        >>> print dum
        >>> True
        """
        if not self.is_downloaded and self.server_loc:
            print "--- DOWNLOADING to " + self._path()
            url = self.server_loc + "RETRIEVE?file_id=" + self.name
            try:
                con = urlopen(url)
                i = 0
                while True:
                    buffer = con.read(BLOCK_SIZE)
                    if not buffer:
                        break
                    Data(file_id = self.id, series = i, data = buffer)
                    i += 1
                self.is_downloaded = True
            except HTTPError, e:
                print "HTTP Error:", e.code, url
            except URLError, e:
                print "URL Error:", e.reason, url

    def _path(self):
        if self.path == "/":
            return "/" + self.name
        return self.path + "/" + self.name


class Data(SQLObject):
    file_id = ForeignKey("File", notNone=True)
    series = IntCol(notNone=True)
    data = BLOBCol(notNone=False)

"""
Functions for handling paths
"""

def getFileName(path):
    return os.path.basename(path)

def getFilePath(path):
    return os.path.dirname(path)

"""
Functions to help get SQL entries
"""

def getParentFromPath(path):
    par = getFilePath(path)
    try:
        return File.selectBy(path=getFilePath(par), name=getFileName(par)).getOne()
    except SQLObjectNotFound:
        raise FuseOSError(ENOENT)

def getFileFromPath(path):
    try:
        return File.selectBy(path=getFilePath(path), name=getFileName(path)).getOne()
    except SQLObjectNotFound:
        raise FuseOSError(ENOENT)

"""
Function to init the database
"""
def initDB(db_name):
    # check for the '.sqlite' extension on db_path
    if not db_name.endswith('.sqlite'):
        db_name = db_name + ".sqlite"

    # connect to SQL database
    connection_string = 'sqlite:' + os.path.realpath(db_name) # uses full path to stop daemon error
    connection = connectionForURI(connection_string)
    sqlhub.processConnection = connection

    # create tables
    File.createTable(ifNotExists=True)
    Data.createTable(ifNotExists=True)

    # create root entry (if not exists)
    if File.select().count() == 0:
        now = time()
        File(name="", path="/", st_mode=(S_IFDIR | 0755), st_nlink=3,
            st_size=0, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc=None, attrs={}, is_downloaded=False)
        File(name=FS_SPECIFIC_PATH, path="/", st_mode=(S_IFDIR | 0755), st_nlink=2,
            st_size=0, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc=None, attrs={}, is_downloaded=False)
        # TEST FILE
        """
        File(name="pic.png", path="/", st_mode=(S_IFREG | 0755), st_nlink=1,
            st_size=395403, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc="http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/",
            attrs={}, is_downloaded=False)
        """
    return connection
