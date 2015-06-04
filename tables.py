#!/usr/bin/env python

from errno import ENOENT

from time import time
from stat import S_IFDIR, S_IFREG

from fuse import FuseOSError

from sqlobject import *
from sqlobject.sqlbuilder import *
from urllib2 import urlopen, URLError, HTTPError
import os
from stat import S_ISDIR

# globals
BLOCK_SIZE = 65536
FS_SPECIFIC_PATH = "local_files"

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
    on_local = BoolCol(notNone=True)   # possibly set to True (value = "")
    path_index = DatabaseIndex("path")

    # DOES NOT WORK WITH DAEMON. The download might have to be done in a separate process
    def _check_download(self):
        """
        >>> dum = _check_download()
        >>> print dum
        >>> True
        """
        if not self.on_local and self.server_loc:
            print "--- DOWNLOADING " + self._path()
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
                self.on_local = True
            except HTTPError, e:
                print "HTTP Error:", e.code, url
            except URLError, e:
                print "URL Error:", e.reason, url

    def _is_FS_file(self):
        return self.path.startswith("/" + FS_SPECIFIC_PATH)

    def _isDir(self):
        return S_ISDIR(self.st_mode)

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
Function to clean database.
Deletes all data from files that exist on a server.
Sets on_local to False for all files on server.
"""
def cleanFS(fsName):
    con = initFS(fsName)
    con.Data.deleteMany(where=IN(con.Data.q.file_id, Select(con.File.q.id, con.File.q.server_loc!=None)))
    con.query(con.sqlrepr(Update('file', values={'on_local':False}, where='server_loc IS NOT NULL')))   # set on_local to False
    con.query("VACUUM")
    con.close()

"""
Function to check if FS exists
"""
def fsExists(fsName):
    if not fsName.endswith('.sqlite'):
        fsName = fsName + ".sqlite"
    return os.path.isfile(fsName)

"""
Function to init a FS
"""
def initFS(fsName):
    # check for the '.sqlite' extension on fsName
    if not fsName.endswith('.sqlite'):
        fsName = fsName + ".sqlite"

    # connect to SQL database
    connection_string = 'sqlite:' + os.path.realpath(fsName) # uses full path to stop daemon error
    con = connectionForURI(connection_string)
    sqlhub.processConnection = con

    # create tables
    con.File.createTable(ifNotExists=True)
    con.Data.createTable(ifNotExists=True)

    # create root entry (if not exists)
    if con.File.select().count() == 0:
        now = time()
        con.File(name="", path="/", st_mode=(S_IFDIR | 0755), st_nlink=3,
            st_size=0, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc=None, attrs={}, on_local=True)
        con.File(name=FS_SPECIFIC_PATH, path="/", st_mode=(S_IFDIR | 0755), st_nlink=2,
            st_size=0, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc=None, attrs={}, on_local=True)
        # TEST FILE
        """
        File(name="pic.png", path="/", st_mode=(S_IFREG | 0755), st_nlink=1,
            st_size=395403, st_ctime=now, st_mtime=now,
            st_atime=now, st_uid=0, st_gid=0,
            server_loc="http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/",
            attrs={}, on_local=False)
        """
    return con
