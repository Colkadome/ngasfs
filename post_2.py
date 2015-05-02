import sys
import atpy
import time
import datetime
import os
import glob2

from urlparse import urlparse
from ngamsPClient import ngamsPClient
import numpy

from sqlobject import *
from tables import *

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
NOTES:
this function may only need to use the SQL file. It might be tricky to upload with blocks though.
"""
def postFS(sLoc, fs_id, mountDir, *options):

	# check if mount dir exists
	if not os.path.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check for the '.sqlite' extension on fs_id
	if not fs_id.endswith('.sqlite'):
		fs_id = fs_id + ".sqlite"

	# init DB
	connection = initDB(fs_id)

	# upload files in mount
	postFiles(sLoc, fs_id, mountDir, "**", options)

	# Strip database of data
	for f in File.select():
		f.is_downloaded = False
	Data.deleteMany(None)
	connection.queryAll("VACUUM")

	# upload database file
	print "Uploading: " + fs_id
	postFile(sLoc, fs_id, options)

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
	initDB(fs_id)

	# convert <patterns> to a list
	if not isinstance(patterns, list):
		patterns = [patterns]

	# construct set of paths captured by <patterns>
	paths = set()
	for pattern in patterns:
		tempPaths = glob2.glob(mountDir + "/" + pattern)
		paths = paths.union(set(tempPaths))

	# iterate through paths, and upload files
	uploadCount = 0
	for path in paths:
		if os.path.isfile(path):

			# get the SQL entry
			path_sql = path.split(mountDir, 1)[1] # strip mountDir from path
			f = getFileFromPath(path_sql)

			if f.server_loc==None or forceUpload:
				# upload the file
				print "Uploading: " + path_sql
				uploadCount += 1
				postFile(sLoc, path, options)
				# update tables for file
				f.server_loc = sLoc
				Data.deleteBy(file_id=f.id)
			else:
				print "Ignoring: " + path_sql

	# print info for the user
	if uploadCount > 0:
		print "-- " + str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "-- All files exist on server."


if __name__ == "__main__":

	if len(sys.argv) < 5:
		print "USAGE: post.py <server_loc> <fs_id> <mount_dir> <patterns> [options]"
	postFiles(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])

	#if len(sys.argv) < 5:
	#	print "USAGE: post.py <server_loc> <fs_id> <mount_dir> [options]"
	#postFS(sys.argv[1], sys.argv[2], sys.argv[3])
