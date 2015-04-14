import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os
from pprint import pprint

import numpy
import ntpath
from urllib2 import urlopen, URLError, HTTPError

# add file(s) from NGAS to the FS
def getFiles(sLoc, fs_id, patterns):

    # check if database exists
    con = None
    if ntpath.isfile(fs_id):
        con = lite.connect(fs_id)
    else:
        print "ERROR: Database doesn't exist!"
        return 1

    # connect to DB, create list of all filenames already on FS
    db_fileName = []
    cur = con.cursor()
    cur.execute("SELECT DISTINCT filename FROM dentry")
    for entry in cur.fetchall():
        db_fileName.append(entry[0])

    # filenames that should be ignored
    ignore = set(db_fileName)

    # convert patterns to a list
    if not isinstance(patterns, list):
        patterns = [patterns]

    # execute some code using con (CODE FROM FILLDB)
    cur.execute("SELECT * FROM dentry ORDER BY parent_id DESC")
    parent_id = cur.fetchone() #Get highest existng parent_id
    if parent_id == None:
        parent_id = 1 #If None then set to 1
    else:
        parent_id = parent_id[1]
    cur.execute("SELECT * FROM inode ORDER BY id DESC")
    inodeID = cur.fetchone()[0] #Get highest inode id
    cur.execute("SELECT * FROM inode ORDER BY inode_num DESC")
    inode_num = cur.fetchone()[1] #Get highest inode_num
    cur.execute("SELECT * FROM dentry ORDER BY id DESC")
    dentryID = cur.fetchone() #Get highest dentry id
    if dentryID == None:
        dentryID = 0 #If None then set to 0
    else:
        dentryID = dentryID[0]
    cur.execute("SELECT * FROM data_list ORDER BY id DESC")
    data_listID = cur.fetchone() #Get highests data_list id
    if data_listID == None:
        data_listID = 0 #If None then set to 0
    else:
        data_listID = data_listID[0]

    # add filenames from server (and remove duplicate names)
    uploadCount = 0
    for p in patterns:
        # gather new files using pattern
        print "-- Matching " + p
        T = atpy.Table(sLoc + 'QUERY?query=files_like&format=list&like='+p,type='ascii')[3:]   # get everything past result 3

        # iterate through names.
        # add entries that are not in DB, or have not been added previously.
        names = T['col3']
        for i in range(len(names)):
            if names[i] not in ignore:
                print "Adding " + names[i]
                uploadCount += 1
                
                cTime = T[i]['col14'].replace("T", " ")
                cTime = time.mktime(datetime.datetime.strptime(cTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
                iTime = T[i]['col9'].replace("T", " ")
                iTime = time.mktime(datetime.datetime.strptime(iTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
                iString = "INSERT INTO inode VALUES({0}, {1}, 1, 1000, 1000, {2}, {2}, {3}, {4}, 33060, 0)".format(inodeID+i+1, inode_num+i+1, iTime, cTime, T[i]['col6'])
                deString = "INSERT INTO dentry VALUES({0}, {1}, '{2}', {3}, '{4}')".format(dentryID+i+1, parent_id, T[i]['col3'], inode_num+i+1, sLoc)
                cur.execute(iString)
                cur.execute(deString)
            else:
                print "Ignored " + names[i] + ", already in FS."
        # new names are now ignored
        ignore = ignore.union(set(names))

    # commit database changes
    con.commit()
    con.close()

    # print info for the user
    if uploadCount > 0:
        print "-- " + str(uploadCount) + " file(s) successfully added to " + fs_id
    else:
        print "-- All files exist on " + fs_id

# Download a FS
def getFS(sLoc, fs_id):
    try:
        url = sLoc + 'RETRIEVE?file_id="'+fs_id+'"'
        f = urlopen(url)
        print "Downloading " + fs_id

        # Open local file for writing
        with open(fs_id, "wb") as local_file:
            local_file.write(f.read())
    #handle errors
    except HTTPError, e:
        print "HTTP Error:", e.code, url
    except URLError, e:
        print "URL Error:", e.reason, url

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "USAGE: get.py <server_loc> <fs_id> <patterns>"
    getFiles(sys.argv[1],sys.argv[2],sys.argv[3])