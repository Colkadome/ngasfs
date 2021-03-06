import sys
import os

from urlparse import urlparse
from sqlobject import *
from sqlobject.sqlbuilder import *
import httplib
import argparse
from stat import S_IFDIR, S_ISDIR

from tables import *

def postFS(sLoc, fsName, verbose=True, force=False):
	"""
	postFS()
	-----------------------
	Posts a FS database, as well as the files, to a NGAS server.
	Will delete the raw_data and data_list tables from the FS database
	when upload is successful.
	
	ARGS:
	sLoc 		- server address string (e.g. "http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/")
					String should contain the trailing '/'.
	fsName 		- path to FS database sqlite3 file.
	options		- options.
					"-f" force upload of files.
					"-v" print server's response for each upload.
	
	RETURN:
	NOTES:
	this function may only need to use the SQL file. It might be tricky to upload with blocks though.
	"""

	# check for the '.sqlite' extension on fsName
	if not fsName.endswith('.sqlite'):
		fsName = fsName + ".sqlite"

	# upload files in mount and clean FS
	postFiles(sLoc, fsName, ["%"], verbose, force)
	cleanFS(fsName)

	# upload SQL file
	print "Uploading: " + fsName
	status = postFile_path(sLoc, fsName, verbose)
	if status != 200:
		print "WARNING: " + fsName + " was not uploaded!"
	return status

def postFiles(sLoc, fsName, patterns, verbose=True, force=False):
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
	patterns    - array of patterns to match files in the database.
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
	L = getFSList(fsName, patterns)
	L_ids = []
	for l in L:
		L_ids.append(l["id"])
	postFilesWithList(sLoc, fsName, L_ids, verbose, force)

def postFilesWithList(sLoc, fsName, L_ids, verbose=True, force=False):
	"""
	postFilesWithList()
	-----------------------
	Posts files using a list of file IDs.
	"""

	# connect to fs
	con = initFS(fsName)

	# iterate through list
	uploadCount = 0
	for f_id in L_ids:
		f = con.File.get(f_id) 
		f.expire()	# make sure cache is current
		if f.server_loc==None or force:
			if f.on_local:
				# print stuff
				print "Uploading: " + f._path()
				# upload the file
				status = postFile_FS(sLoc, fsName, f.id)
				if status == 200:
					uploadCount += 1
					f.server_loc = sLoc
				else:
					print "WARNING: " + f._path() + " was not uploaded!"
			else:
				print "Ignoring: " + f._path() + ", not on local."
		else:
			print "Ignoring: " + f._path() + ", exists on server." #checksum check?

	# close connection
	con.close()

	# print info for the user
	if uploadCount > 0:
		print "-- " + str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "-- No files were uploaded."
	return uploadCount

def getFSList(fsName, patterns):
	"""
	getFSList()
	-----------------------
	Gets a list of file information from the FS.
	Not all files returned are suitable for upload, but are files
	the user will want to know about.
	The returned list is JSON serializable!
	"""

	# check if database exists
	con = initFS(fsName)

	# define set of file IDs to ignore
	ignore = set()

	# iterate through patterns
	L = list()
	for pattern in patterns:
		for f in con.File.select(LIKE(con.File.q.name, pattern)).orderBy('name'):
			if not f._isDir() and not f._is_FS_file() and f.id not in ignore:
				# get relevant file information
				L.append({"id":f.id, "name":f.name,
					"st_size":f.st_size, "st_mtime":f.st_mtime,
					"server_loc":f.server_loc})
				ignore.add(f.id)

	# close FS connection and return
	con.close()
	return L



def getMimeType(fileName):
	"""
	getMimeType()
	-----------------------
	Determines mime-type from a file name by
	using the file extension.
	"""
	ext = os.path.splitext(fileName)[1]
	if ext == "test":
		return "application/x-nglog"
	return "application/octet-stream"

def postFile_FS(sLoc, fsName, file_id, verbose=True):
	"""
	postFile_FS()
	-----------------------
	Posts a single file to a NGAS server using a FS file.
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

	# get file from DB
	conFS = initFS(fsName)
	f = conFS.File.get(file_id)

	# check if file is directory
	if S_ISDIR(f.st_mode):
		print "ERROR: " + f._path() + " is directory!"
		return 1

	# check if file has local data
	if not f.on_local:
		print "ERROR: " + f._path() + " has no local data!"
		return 1

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# connect to URL
	conn = httplib.HTTPConnection(host+":"+str(port))
	conn.connect()
	conn.putrequest('POST', '/ARCHIVE')
	conn.putheader('Content-type', getMimeType(f.name)) # check type
	conn.putheader('Content-disposition', 'attachment; filename="'+f.name+'";')
	conn.putheader('Content-length', str(f.st_size))
	conn.putheader('Host', host)
	conn.endheaders()

	# send data
	for d in Data.selectBy(file_id=f.id, connection=conFS).orderBy("series"):
 		conn.send(d.data)

 	# close FS connection
 	conFS.close()

 	# get response
	r = conn.getresponse()
	print r.status, r.reason
	return r.status

def postFile_path(sLoc, filePath, verbose=True):
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

	# check if file exists
	if not os.path.isfile(filePath):
		print "ERROR: " + filePath + " is directory, or does not exist!"
		return 1

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# connect to URL
	conn = httplib.HTTPConnection(host+":"+str(port))
	conn.connect()
	conn.putrequest('POST', '/ARCHIVE')
	conn.putheader('Content-type', getMimeType(filePath))
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

"""
Main function
"""
if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="Post files to NGAS server")
	parser.add_argument("sLoc", help="The server location", type=str)
	parser.add_argument("fsName", help="The path to your FS", type=str)
	parser.add_argument("pattern", help="SQL pattern to match FS files", type=str)
	parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")
	parser.add_argument("-f", "--force", help="Force upload of files that already exist on server", action="store_true")
	a = parser.parse_args()

	postFiles(a.sLoc, a.fsName, [a.pattern], a.verbose, a.force)
	#postFS(a.sLoc, a.fsName, a.verbose, a.force, a.keep)
