import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os
import glob2

from ngamsPClient import ngamsPClient
from urlparse import urlparse
import numpy

"""
getListIndex()
-----------------------
Iterates through a list, looking at a particular column (col).
If a value (val) is found, returns the row index in the list.

ARGS:
list 		- t
col 		- the column to look through
val 		- the value to look for

RETURN:
index of val, if found. Else -1.
"""
def getListIndex(list,col,val):
	for i in range(len(list)):
		if list[i][col] == val:
			return i
	return -1

"""
pathEntryMap()
-----------------------
Takes the FS database and returns a map of
file paths to file info in the database.

ARGS:
mountDir 	- the directory the files are in
con 		- a connection to the sqlite3 database

RETURN:
map, where key = file_path and value = file info.
"""
def pathEntryMap(mountDir, con):
	# get database entries
	cur = con.cursor()
	cur.execute("SELECT max(inode.id), filename, parent_id, server_loc FROM dentry, inode WHERE dentry.inode_num = inode.inode_num GROUP BY filename, parent_id")
	result = cur.fetchall()
	# iterate through entries
	maps = {}
	for entry in result:
		path = entry[1]
		curr = entry	# current entry
		# move through parent nodes until root is reached
		while 1:
			pId = curr[2] # get parent ID
			if pId==1:	# if the parent is mount directory (id = 1), exit loop
				break
			curr = result[getListIndex(result,0,pId)]	# find parent entry, and move to it
			path = curr[1] + "/" + path	# append parent's name to path
		path = mountDir + "/" + path # make full path
		maps[path] = {'filename':entry[1],'parent_id':entry[2],'server_loc':entry[3]}	# add path to mapping
	return maps

"""
postFS()
-----------------------
Posts a FS database, as well as the files, to a NGAS server.
Will delete the raw_data and data_list tables from the FS database
when upload is successful.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
fs_id 		- path to FS database sqlite3 file.
mountDir 	- path to mount location of FS.
options		- options.
				"-f" force upload of files.
				"-v" print server's response for each upload.

RETURN:
"""
def postFS(sLoc, fs_id, mountDir, *options):

	# check if mount dir exists
	if not os.path.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check for the '.sqlite' extension on fs_id
	if not fs_id.endswith('.sqlite'):
		fs_id = fs_id + ".sqlite"

	# check if database exists
	con = None
	if os.path.isfile(fs_id):
		con = lite.connect(fs_id)
	else:
		print "ERROR: Database doesn't exist!"
		return 1

	# upload files in mount
	postFiles(sLoc, fs_id, mountDir, "**", options)

	# Strip database of raw data
	cur = con.cursor()
	cur.execute("DELETE FROM raw_data")
	cur.execute("DELETE FROM data_list")
	cur.execute("VACUUM") # removes free space left behind from DELETE commands
	con.commit()

	# upload database file
	print "Uploading: " + fs_id
	postFile(sLoc, fs_id)

	# close sqlite3 db connection
	con.close()

"""
postFile()
-----------------------
Posts a single file to a NGAS server.
Uploads even if the file exists on NGAS already.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
path 		- relative path to the file.
options		- options
				"-v" print server's response.

RETURN:
"""
def postFile(sLoc, path, *options):

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# upload file using ngamsPClient
	client = ngamsPClient.ngamsPClient(host, port)	# should the function be passed this instead?
	status = client.archive(path, mimeType="application/octet-stream")

	# print response (if verbose)
	if "-v" in options:
		print status.getMessage()

"""
postFiles()
-----------------------
Posts multiple files from a FS mount to a NGAS server.
Will select certain files to upload based on <patterns>.
Will only upload files that do not already exist on the server.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
fs_id 		- path to FS database sqlite3 file.
mountDir 	- the directory the files are in
patterns	- array of patterns sent to glob2 that specifies what files should be uploaded.
				E.g. "file*.txt" - selects all files that start with 'file' and end with '.txt'.
				"**/*.txt" - selects all text files recursively.
options		- options
				"-f" force upload of files.
				"-v" print server's response for each upload.

RETURN:

NOTE:
currently does not check if local files share an ID with a file on the server.
If a local file has the same ID as a server file, the file is still uploaded.

USE CASES:
+ upload file1.txt, where file1.txt does not exist on server, but exists in FS.
- should upload file1.txt and change the server_loc column in the FS to the server it was uploaded to.

+ upload file1.txt, where file1.txt has an entry under server_loc in the FS.
- file1.txt is ignored, as it already exists on a server.

+ upload file1.txt, where the file has no server_loc entry, but the file exists on the server.
- uploads file1.txt anyway, because its likely not the same file.
"""
def postFiles(sLoc, fs_id, mountDir, patterns, *options):	#ADD option for forced upload

	# get flags
	forceUpload = "-f" in options

	# check if mount dir exists
	if not os.path.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check for the '.sqlite' extension on fs_id
	if not fs_id.endswith('.sqlite'):
		fs_id = fs_id + ".sqlite"

	# check if database exists
	con = None
	if os.path.isfile(fs_id):
		con = lite.connect(fs_id)
	else:
		print "ERROR: Database doesn't exist!"
		return 1

	# map filepaths to info about the files
	pathEntry = pathEntryMap(mountDir, con)

	# convert <patterns> to a list
	if not isinstance(patterns, list):
		patterns = [patterns]

	# construct set of paths captured by <patterns>
	paths = set()
	for pattern in patterns:
		tempPaths = glob2.glob(mountDir + "/" + pattern)
		paths = paths.union(set(tempPaths))

	# iterate through paths, and upload files
	cur = con.cursor()
	uploadCount = 0
	for path in paths:
		if os.path.isfile(path):

			# get entry details (this seems kind of dumb, maybe the sql database should contain path?)
			entry = pathEntry[path]

			# check whether the file should be uploaded
			upload = False
			if forceUpload:
				upload = True
			else:
				# if file is not from a server, upload it
				if entry['server_loc']==None:
					upload = True

			# upload the file
			if upload:
				print "Uploading: " + path
				uploadCount += 1
				postFile(sLoc, path, options)
				# update server_loc in DB
				cur.execute("UPDATE dentry SET server_loc='"+sLoc+"' WHERE filename='"+entry['filename']+"' AND parent_id="+str(entry['parent_id']))
				con.commit()
			else:
				print "Ignoring: " + path
	con.close()

	# print info for the user
	if uploadCount > 0:
		print "-- " + str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "-- All files exist on server."


if __name__ == "__main__":

	if len(sys.argv) < 5:
		print "USAGE: post.py <server_loc> <fs_id> <mount_dir> <patterns> [options]"
	postFiles(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])