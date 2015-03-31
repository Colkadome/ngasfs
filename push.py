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

    # Execute code with connection
	with con:
		cur = con.cursor() # create cursor
		# for each filename in dentry (that is NOT a directory), upload to ARCHIVE
		for entry in cur.execute("SELECT filename FROM dentry WHERE inode_num IN (SELECT DISTINCT parent_id FROM data_list)"):
			# get arguments to send to ARCHIVE.
			# mime_type = "application/octet-stream" to upload any file.
			filename = entry[0]
			mime_type = "application/octet-stream"
			# construct header.
			headers = {"Content-type":mime_type,"Content-Disposition":'filename="'+filename+'"'}
			# print something.
			print "--------------------"
			print "Uploading: " + filename
			print "--------------------"
			# send POST request and print response.
			response = requests.post(SERVER_LOCATION + "ARCHIVE", headers=headers, files={filename: open(mountDir+"/"+filename, mode='rb')})
			print response.text

if __name__ == "__main__":
    main()