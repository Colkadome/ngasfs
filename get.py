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
def getFile(sLoc, fs_id, patterns):

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

    # add filenames from server (and remove duplicate names)
    T = numpy.array([], dtype=[('col1', 'S32'), ('col2', 'S37'), ('col3', 'S18'),
        ('col4', 'S19'), ('col5', 'S24'), ('col6', 'S20'),
        ('col7', 'S28'), ('col8', 'S22'), ('col9', 'S23'),
        ('col10', 'S17'), ('col11', 'S19'), ('col12', 'S22'),
        ('col13', 'S22'), ('col14', 'S24'), ('col15', 'S18')])
    for p in patterns:
        # gather new files using pattern
        print "-- Matching " + p
        temp = atpy.Table(sLoc + 'QUERY?query=files_like&format=list&like='+p,type='ascii')[3:]   # get everything past result 3

        # iterate through names.
        # add entries that are not in DB, or have not been added previously.
        names = temp['col3']
        for i in range(len(names)):
            if names[i] not in ignore:
                print "Adding " + names[i]
                T = numpy.append(T, temp[i])
            else:
                print "Ignored " + names[i] + ", already in FS."
        # new names are now ignored
        ignore = ignore.union(set(names))

    if len(T) > 0 :

        # gather file properties
        s_fileName = T['col3']
        s_size = T['col6']
        s_ctime = T['col14']
        s_itime = T['col9']

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

        # iterate through T and add files to FS
        for i in range(len(T)):

            # fill database with data from server (FILLDB CODE)
            cTime = s_ctime[i].replace("T", " ")
            cTime = time.mktime(datetime.datetime.strptime(cTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
            iTime = s_itime[i].replace("T", " ")
            iTime = time.mktime(datetime.datetime.strptime(iTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
            iString = "INSERT INTO inode VALUES({0}, {1}, 1, 1000, 1000, {2}, {2}, {3}, {4}, 33060, 0)".format(inodeID+i+1, inode_num+i+1, iTime, cTime, s_size[i])
            deString = "INSERT INTO dentry VALUES({0}, {1}, '{2}', {3}, {4})".format(dentryID+i+1, parent_id, s_fileName[i], inode_num+i+1, "'"+sLoc+"'")
            cur.execute(iString)
            cur.execute(deString)

        # print stuff for user
        print "-- " + str(len(T)) + " file(s) added to " + fs_id
    else:
        print "-- No files added to " + fs_id

    # commit and close connection to DB
    con.commit()
    con.close()

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
        print "USAGE: post.py <server_loc> <fs_id>"
    getFS(sys.argv[1],sys.argv[2])