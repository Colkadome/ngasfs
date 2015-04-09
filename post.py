import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os
import glob2
import ntpath

import requests
#from ngamsPClient import ngamsPClient

def getListIndex(list,col,val):
	for i in range(len(list)):
		if list[i][col] == val:
			return i
	return -1

def getServerFromPath(path, cursor):
	return 0

def postFS(sLoc, fs_path, mountDir):

	# define ngamsPClient class, for uploading files
	#nClient = ngamsPClient(host=SERVER_LOCATION)

	# check if mount dir exists
	if not ntpath.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check if database exists
	con = None
	if ntpath.isfile(fs_path):
		con = lite.connect(fs_path)
	else:
		print "ERROR: Database doesn't exist!"
		return 1

	# For each distinct filename in dentry, get a dentry-inode pair
	cur = con.cursor()
	cur.execute("SELECT max(inode.id), filename, parent_id, server_loc FROM dentry, inode WHERE dentry.inode_num = inode.inode_num GROUP BY filename, parent_id")
	result = cur.fetchall()

	# iterate through entries
	uploadCount = 0
	for entry in result:

		# check if the file did NOT come from NGAS
		if entry[3] == None:

			# recursively search through entry's parents to get path
			path = entry[1]
			curr = entry	# current entry
			while 1:
				pId = curr[2] # get parent ID
				if pId==1:	# if the parent is mount directory (id = 1), exit loop
					break
				curr = result[getListIndex(result,0,pId)]	# find parent entry, and move to it
				path = curr[1] + "/" + path	# append parent's name to path
			path = mountDir + "/" + path

			# upload file to NGAS
			postFile(sLoc, path)
			uploadCount += 1

			# commit sqlite3 database changes
			cur.execute("UPDATE dentry SET server_loc='"+sLoc+"' WHERE filename='"+entry[1]+"' AND parent_id="+str(entry[2]))
			con.commit()

	# print info for the user
	if uploadCount > 0:
		print "-- " + str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "-- All files exist on server."

	# strip database file
	cur.execute("DELETE FROM raw_data")
	cur.execute("DELETE FROM data_list")
	con.commit()
	# upload database file
	postFile(sLoc, fs_path)
	# close sqlite3 db connection
	con.close()

# Posts file(s) to a server.
# ARGUMENTS:
# sLoc = server address
# patterns = array or string, containing pattern(s) for glob2 to match.
def postFile(sLoc, fs_id, patterns):	#ADD option for forced upload

	# convert patterns to a list
    if not isinstance(patterns, list):
        patterns = [patterns]

	# construct array containing distinct paths.
	paths = []
	for pattern in patterns:
		tempPaths = glob2.glob(pattern)
		paths = paths + list(set(tempPaths)-set(paths))

	# iterate through paths, and upload files
	for path in paths:
		if ntpath.isfile(path):

			# check if file is already on the server
			# CHECK HERE

			print "Uploading: " + path
			filename = ntpath.basename(path)
			# send POST request to upload file, and print response.
			response = requests.post(sLoc + "ARCHIVE", headers={"Content-type":"application/octet-stream",
				"Content-Disposition":"filename="+filename},
				files={filename: open(path, mode='rb')})
			#print response.text


if __name__ == "__main__":

	if len(sys.argv) < 4:
		print "USAGE: post.py <server_loc> <fs_path> <mount_dir>"
	postFS(sys.argv[1],sys.argv[2],sys.argv[3])