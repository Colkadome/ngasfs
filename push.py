import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os
import glob2
#import requests
#from ngamsPClient import ngamsPClient

# specify important locations
database = 'dumpfs.sqlite' # "GROUP"
SERVER_LOCATION = 'http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/'

def getListIndex(list,col,val):
	for i in range(len(list)):
		if list[i][col] == val:
			return i
	return -1

def main():

	# check if the number of arguments is correct
	if len(sys.argv) < 2:
		print "Error: Must specify directory to upload from!"
		exit()

	# check if mount dir exists
	mountDir = sys.argv[1]
	if not os.path.isdir(mountDir):
		print "Error: Directory " + mountDir + " does not exist!"
		exit()

	# check if database exists
	con = None
	if os.path.isfile(database):
		con = lite.connect(database)
	else:
		print "Error: Database doesn't exist!"
		exit()

    # For each distinct filename in dentry, get a dentry-inode pair
	cur = con.cursor()
	cur.execute("SELECT max(inode.id), filename, parent_id, server_loc FROM dentry, inode WHERE dentry.inode_num = inode.inode_num GROUP BY filename, parent_id")
	result = cur.fetchall()

	uploadCount = 0
	for entry in result:
		# check if the file did NOT come from NGAS
		server = entry[3]
		if server == None:

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

			# check if the path is a file
			if os.path.isfile(path):
				uploadCount += 1
				#con = ngamsPClient(host=server,port="",timeout=None)
				#mime_type = "application/octet-stream"
				#print "--------------------"
				print "Uploading: " + path

				# SET SERVER IN DENTRY, AND UPLOAD USING ngamsPClient

				#print "--------------------"
				# send POST request to upload file, and print response. (UPLOAD USING ngamsPClient.py)
				#response = con.archive(fileUri="fileUri",mime_type=mime_type)
				#response = requests.post(SERVER_LOCATION + "ARCHIVE", headers=headers, files={fname: open(fpath, mode='rb')})
				#print response.text
	# print info for the user
	if uploadCount > 0:
		print str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "All filenames exist on server."

if __name__ == "__main__":
    main()