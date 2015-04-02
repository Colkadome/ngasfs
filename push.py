import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os
import requests

# specify important locations
database = 'dumpfs.sqlite'
SERVER_LOCATION = 'http://ec2-54-152-35-198.compute-1.amazonaws.com:7777/'

def getListIndex(list,col,val):
	for i in range(len(list)):
		if list[i][col] == val:
			return i
	return 0

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

	# connect to server and get file attributes
	T = atpy.Table(SERVER_LOCATION + 'QUERY?query=files_list&format=list',type='ascii')
	fileNames = []	# NGAS file IDs
	itimes = []		# NGAS ingest times
	for s in T['col3'][3:]:		# Get every entry from the 4th and onwards (to skip NGAS's first three lines)
		fileNames.append(s)
	for t in T['col9'][3:]:
		itimes.append(t)

    # Execute code with connection
	uploadCount = 0
	with con:
		cur = con.cursor() # create cursor
		# execute sql query.
		# the query joins inode and dentry BY inode_num. parent_id references inode.id.
		cur.execute("SELECT inode.id, filename, parent_id FROM dentry, inode WHERE dentry.inode_num = inode.inode_num GROUP BY filename ORDER BY inode.id")
		result = cur.fetchall() # save sql query's result
		for entry in result:
			# recursively search through entry's parents to gather full file path
			fpath = entry[1]
			curr = entry	# current entry
			while 1:
				pId = curr[2] # get parent ID
				if pId==1:	# if the parent is mount directory (id = 1), exit loop
					break
				curr = result[getListIndex(result,0,pId)]	# find parent entry, and move to it
				fpath = curr[1] + "/" + fpath	# append parent's name to fpath
			fpath = mountDir + "/" + fpath	# append mount to path

			# check if file does not exist on the server, and is not a directory! (USE DATE MODIFIED??)
			fname = entry[1]
			if fname not in fileNames and os.path.isfile(fpath):
				uploadCount += 1
				# mime_type = "application/octet-stream" to upload any file.
				mime_type = "application/octet-stream"
				# construct header.
				headers = {"Content-type":mime_type,"Content-Disposition":'filename="'+fname+'"'}
				# print stuff for user.
				#print "--------------------"
				print "Uploading: " + fpath
				#print "--------------------"
				# send POST request to upload file, and print response.
				response = requests.post(SERVER_LOCATION + "ARCHIVE", headers=headers, files={fname: open(fpath, mode='rb')})
				#print response.text
	# print info for the user
	if uploadCount > 0:
		print str(uploadCount) + " file(s) successfully uploaded to NGAS."
	else:
		print "All filenames exist on server."

if __name__ == "__main__":
    main()