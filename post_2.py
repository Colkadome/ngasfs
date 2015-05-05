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
from sqlobject.sqlbuilder import *
from tables import *
import httplib

"""
postFS()
-----------------------
Posts a FS database, as well as the files, to a NGAS server.
Will delete the raw_data and data_list tables from the FS database
when upload is successful.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
dbPath 		- path to FS database sqlite3 file.
options		- options.
				"-f" force upload of files.
				"-v" print server's response for each upload.

RETURN:
NOTES:
this function may only need to use the SQL file. It might be tricky to upload with blocks though.
"""
def postFS(sLoc, dbPath, *options):

	# init DB
	connection = initDB(dbPath)
	# upload files in mount
	postFiles(sLoc, dbPath, "%", options)
	# clean data (data is deleted in postFiles)
	connection.queryAll("VACUUM")
	# upload database file
	print "Uploading: " + dbPath
	if postFile_path(sLoc, dbPath, options) != 200:
		print "WARNING: " + dbPath + " was not uploaded!"

"""
postFiles()
-----------------------
Posts multiple files from a FS mount to a NGAS server.
Will select certain files to upload based on <patterns>.
Will only upload files that do not already exist on the server.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
dbPath 		- path to FS database sqlite3 file.
pattern    	- a pattern to match files in the database.
                Files are matched using SQL query: "SELECT file WHERE file_id LIKE pattern"
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
def postFiles(sLoc, dbPath, pattern, *options):	#ADD option for forced upload

	# get flags
	forceUpload = "-f" in options

	# check if database exists
	initDB(dbPath)

	# iterate through matched files, and upload them
	uploadCount = 0
	print "-- Matching " + pattern
	for f in File.select(LIKE(File.q.name, pattern) & (File.q.id > 1)):
		if f.server_loc==None or forceUpload:
			# upload the file
			print "Uploading: " + f.path + "/" + f.name
			uploadCount += 1
			if postFile_DB(sLoc, f.id, dbPath) == 200:
				f.server_loc = sLoc
				f.is_downloaded = False
				Data.deleteBy(file_id=f.id)
			else:
				print "WARNING: " + f.path + "/" + f.name + " was not uploaded!"
		else:
			print "Ignoring: " + f.path + "/" + f.name

	# print info for the user
	if uploadCount > 0:
		print "-- " + str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "-- No files were uploaded/found."

"""
postFile_DB()
-----------------------
Posts a single file to a NGAS server using a database file.
Uploads even if the file exists on NGAS already.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
file_id 	- id of file in database
options		- options
				"-v" print server's response.

RETURN:
status of server's response
"""
def postFile_DB(sLoc, file_id, dbPath, *options):

	# get file from DB
	initDB(dbPath)
	f = File.get(file_id)

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# connect to URL
	conn = httplib.HTTPConnection(host+":"+str(port))
	conn.connect()
	conn.putrequest('POST', '/ARCHIVE')
	conn.putheader('Content-type', "application/octet-stream")
	conn.putheader('Content-disposition', 'attachment; filename="'+f.name+'";')
	conn.putheader('Content-length', str(f.st_size))
	conn.putheader('Host', host)
	conn.endheaders()

	# send data
	for d in Data.selectBy(file_id=f.id).orderBy("series"):
 		conn.send(d.data)

 	# get response
	r = conn.getresponse()
	print r.status, r.reason
	return r.status

"""
postFile_path()
-----------------------
Posts a single file to a NGAS server using a path.
Uploads even if the file exists on NGAS already.

ARGS:
sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
				String should contain the trailing '/'.
filePath 		- relative path to the file.
options		- options
				"-v" print server's response.

RETURN:
status of server's response
"""
def postFile_path(sLoc, filePath, *options):

	# check if file exists
	if not os.path.isfile(filePath):
		print "ERROR: " + filePath + " does not exist!"
		return 1

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# connect to URL
	conn = httplib.HTTPConnection(host+":"+str(port))
	conn.connect()
	conn.putrequest('POST', '/ARCHIVE')
	conn.putheader('Content-type', "application/octet-stream")
	conn.putheader('Content-disposition', 'attachment; filename="'+os.path.basename(filePath)+'";')
	conn.putheader('Content-length', str(os.path.getsize(filePath)))
	conn.putheader('Host', host)
	conn.endheaders()

	# send data
	with open(filePath, 'rb') as f:
 		conn.send(f.read(BLOCK_SIZE))

 	# get response
	r = conn.getresponse()
	print r.status, r.reason
	return r.status


if __name__ == "__main__":

	if len(sys.argv) < 4:
		print "USAGE: post.py <server_loc> <fs_id> <pattern> [options]"
		exit(1)
	postFiles(sys.argv[1],sys.argv[2],sys.argv[3])

	#if len(sys.argv) < 5:
	#	print "USAGE: post.py <server_loc> <fs_id> [options]"
	#	exit(1)
	#postFS(sys.argv[1], sys.argv[2])
