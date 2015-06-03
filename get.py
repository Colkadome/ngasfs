import sys
import atpy
from time import mktime
import datetime
import os

import numpy
from sqlobject import *
from urllib2 import urlopen, URLError, HTTPError
import argparse

from tables import *

"""
getServerList()
-----------------------
Returns a numpy list of files, sorted by date created.
The returned list is JSON serializable!

ARGS:
sLoc        - server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
                String should contain the trailing '/'.
patterns    - array of patterns to match NGAS files to.
                Files are matched using SQL query: "SELECT file WHERE file_id LIKE pattern"
RETURN:
numpy array of files, sorted by descending creation date
"""
def getServerList(sLoc, patterns):

    # get entries from all patterns
    T = atpy.Table(sLoc + 'QUERY?query=files_like&format=list&like=' + patterns[0], type='ascii')[3:]   # get everything past result 3
    for pattern in patterns[1:]:
        T = numpy.append(T, atpy.Table(sLoc + 'QUERY?query=files_like&format=list&like=' + pattern, type='ascii')[3:])
    T = numpy.unique(T) # remove duplicate entries

    # sort by name, then date
    T.sort(order=['col3', 'col14'])
    T = T[::-1]

    # remove all files except for latest versions (might not need this?)
    for i in reversed(range(len(T))[1:]):
        if T[i][2] == T[i-1][2]:
            T = numpy.delete(T, i, 0)

    return T.tolist()   # must be standard array for JSON serialization

"""
getFilesFromList()
-----------------------
Add files to FS with a numpy list.

ARGS:
fsName      - path to FS database sqlite3 file.
T           - The numpy list of file info, sorted by descending version
RETURN:
upload count
"""
def getFilesFromList(sLoc, fsName, T, verbose=True):

    # connect to DB
    con = initFS(fsName)

    # create set of all filenames already on FS
    ignore = set()
    for entry in con.File.select():
        ignore.add(entry.name)

    # define various columns
    name = 2
    cTime = 13
    iTime = 8
    size = 5
    version = 3

    # add entries that are not in DB, or have not been added previously.
    uploadCount = 0
    for t in T:
        if t[name] not in ignore:
            if not t[name].endswith(".sqlite"):
                print "Adding: " + t[name] + " [" + t[version] + "]"
                ignore.add(t[name])
                uploadCount += 1

                # get time attributes
                cTimeInt = mktime(datetime.datetime.strptime(t[cTime].replace("T", " "), "%Y-%m-%d %H:%M:%S.%f").timetuple())
                iTimeInt = mktime(datetime.datetime.strptime(t[iTime].replace("T", " "), "%Y-%m-%d %H:%M:%S.%f").timetuple())

                # add file to SQL database
                con.File(name=t[name], path="/", st_mode=33060, st_nlink=1,
                    st_size=int(t[size]), st_ctime=cTimeInt, st_mtime=iTimeInt,
                    st_atime=iTimeInt, st_uid=0, st_gid=0,
                    server_loc=sLoc, attrs={}, on_local=False)
            else:
                print "Ignoring: " + t[name] + " [" + t[version] + "], is sqlite file"
        else:
            print "Ignoring: " + t[name] + " [" + t[version] + "]"

    # close connection
    con.close()

    # print info for the user
    if uploadCount > 0:
        print "-- " + str(uploadCount) + " file(s) successfully added to " + fsName
    else:
        print "-- No files added to " + fsName
    return uploadCount


"""
getFiles() /ngas.ddns.net
-----------------------
Add multiple files to a FS from a NGAS server.
Will ignore files with the same ID as files in FS.

ARGS:
sLoc        - server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
                String should contain the trailing '/'.
fsName      - path to FS database sqlite3 file.
patterns    - array of patterns to match NGAS files to.
                Files are matched using SQL query: "SELECT file WHERE file_id LIKE pattern"

RETURN:
upload count
"""
def getFiles(sLoc, fsName, patterns, verbose=True):
    return getFilesFromList(sLoc, fsName, getServerList(sLoc, patterns))

"""
downloadFS()
"""
def downloadFS(sLoc, fsName, verbose=True, force=False):
    # check for the '.sqlite' extension on fsName (maybe .ngas.sqlite special file?)
    if not fsName.endswith('.sqlite'):
        fsName = fsName + ".sqlite"
    return downloadFile(sLoc, fsName, verbose, force)

"""
downloadFile()
-----------------------
Downloads a single file from NGAS server.
Will download to the current working directory.
If the file already exists, the download is cancelled.

ARGS:
sLoc        - server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
                String should contain the trailing '/'.
file_id     - the ID of file to download.
options     - options.
                "-f" force download of file. If file aleady exists, it is overwritten.

RETURN:
"""
def downloadFile(sLoc, file_id, verbose=True, force=False):

    if not force:
        if os.path.isfile(file_id):
            print "WARNING: " + file_id + " already exists. Use -f to force download."
            return 1

    try:
        url = sLoc + 'RETRIEVE?file_id="'+file_id+'"'
        f = urlopen(url)
        print "Downloading " + file_id

        # Open local file for writing
        with open(file_id, "wb") as local_file:
            local_file.write(f.read())
    #handle errors
    except HTTPError, e:
        print "HTTP Error:", e.code, url
        return 2
    except URLError, e:
        print "URL Error:", e.reason, url
        return 3
    return 0

"""
Main function
"""
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Save file info from NGAS server to FS")
    parser.add_argument("sLoc", help="The server location", type=str)
    parser.add_argument("fsName", help="The path to your FS", type=str)
    parser.add_argument("pattern", help="SQL pattern to match server files", type=str)
    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")
    a = parser.parse_args()

    getFiles(a.sLoc, a.fsName, [a.pattern], a.verbose)
