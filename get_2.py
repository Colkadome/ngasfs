import sys
import atpy
from time import mktime
import datetime
import os
from pprint import pprint

import numpy
from sqlobject import *
from urllib2 import urlopen, URLError, HTTPError

from tables import *

"""
getFiles()
-----------------------
Add multiple files to a FS from a NGAS server.
Will ignore files with the same ID as files in FS.

ARGS:
sLoc        - server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
                String should contain the trailing '/'.
fs_id       - path to FS database sqlite3 file.
pattern     - pattern to match NGAS files to.
                Files are matched using SQL query: "SELECT file WHERE file_id LIKE pattern"

RETURN:
"""
def getFiles(sLoc, fs_id, pattern):

    # connect to DB
    initDB(fs_id)

    # connect to DB, create set of all filenames already on FS
    ignore = set()
    for entry in File.select():
        ignore.add(entry.name)
    
    # gather server files using pattern
    print "-- Matching " + pattern
    T = atpy.Table(sLoc + 'QUERY?query=files_like&format=list&like='+pattern,type='ascii')[3:]   # get everything past result 3
    names = T['col3']

    # add entries that are not in DB, or have not been added previously.
    uploadCount = 0
    for i in range(len(names)):
        if names[i] not in ignore:
            print "Adding " + names[i]
            ignore.add(names[i])
            uploadCount += 1

            # get file attributes
            cTime = mktime(datetime.datetime.strptime(T[i]['col14'].replace("T", " "), "%Y-%m-%d %H:%M:%S.%f").timetuple())
            iTime = mktime(datetime.datetime.strptime(T[i]['col9'].replace("T", " "), "%Y-%m-%d %H:%M:%S.%f").timetuple())
            size = T[i]['col6']

            # add file to SQL database
            File(name=str(names[i]), path="/", st_mode=33060, st_nlink=1,
                st_size=size, st_ctime=cTime, st_mtime=iTime,
                st_atime=iTime, st_uid=0, st_gid=0,
                server_loc=sLoc, attrs={}, is_downloaded=False)
        else:
            print "Ignored " + names[i] + ", already in FS."

    # print info for the user
    if uploadCount > 0:
        print "-- " + str(uploadCount) + " file(s) successfully added to " + fs_id
    else:
        print "-- All files exist on " + fs_id

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
def downloadFile(sLoc, file_id, *options):

    if "-f" not in options:
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
    except URLError, e:
        print "URL Error:", e.reason, url

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "USAGE: get.py <server_loc> <fs_id> <pattern>"
    getFiles(sys.argv[1],sys.argv[2],sys.argv[3])