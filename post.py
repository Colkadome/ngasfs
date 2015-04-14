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

def postFS(sLoc, fs_id, mountDir, *options):

	# check if mount dir exists
	if not ntpath.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check if database exists
	con = None
	if ntpath.isfile(fs_id):
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
	con.commit()

	# upload database file
	print "Uploading: " + fs_id
	postFile(sLoc, fs_id)

	# close sqlite3 db connection
	con.close()

# posts a single file to the server with no checking
def postFile(sLoc, path, *options):
	# get options
	verbose = "-v" in options

	# send POST request to upload file, and get response.
	filename = ntpath.basename(path)
	response = requests.post(sLoc + "ARCHIVE", headers={"Content-type":"application/octet-stream",
		"Content-Disposition":"filename="+filename},
		files={filename: open(path, mode='rb')})

	# print response (if verbose)
	if verbose:
		print response.text

# Posts file(s) to a server.
# ARGUMENTS:
# sLoc = server address
# patterns = array or string, containing pattern(s) for glob2 to match.
def postFiles(sLoc, fs_id, mountDir, patterns, *options):	#ADD option for forced upload

	# get flags
	forceUpload = "-f" in options

	# check if mount dir exists
	if not ntpath.isdir(mountDir):
		print "ERROR: Directory " + mountDir + " does not exist!"
		return 1

	# check if database exists
	con = None
	if ntpath.isfile(fs_id):
		con = lite.connect(fs_id)
	else:
		print "ERROR: Database doesn't exist!"
		return 1
	cur = con.cursor()

	# get path to entry map
	pathEntry = pathEntryMap(mountDir, con)

	# convert patterns to a list
	if not isinstance(patterns, list):
		patterns = [patterns]

	# construct array containing distinct paths.
	paths = []
	for pattern in patterns:
		tempPaths = glob2.glob(mountDir + "/" + pattern)
		paths = paths + list(set(tempPaths)-set(paths))

	# iterate through paths, and upload files
	uploadCount = 0
	for path in paths:
		if ntpath.isfile(path):

			# get entry details
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