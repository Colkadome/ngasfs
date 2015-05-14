from tables import *
from get import *
from post import *
from ngasfs import *

import os

from flask import Flask
from flask import request
app = Flask(__name__)


@app.route('/create_fs', methods=['POST'])
def create_fs():
	fsName = request.form['fsName']

	# check if file already exists
	if not fsName.endswith('.sqlite'):
		fsName = fsName + ".sqlite"
	if os.path.isfile(fsName):
		return fsName + " already exists"

	# create FS
	initFS(fsName)

	# return response message
	return "Successfully created " + fsName

@app.route('/mount_fs', methods=['POST'])
def mount_fs():
	fsName = request.form['fsName']

	# check if file already exists
	if not fsName.endswith('.sqlite'):
		fsName = fsName + ".sqlite"
	if not os.path.isfile(fsName):
		return fsName + " does not exist"

	# create mountDir (SHOULD BE RANDOM NAME)
	mountDir = "M1"

	# connect to FS (DOES NOT WORK, TRY USING TORNADO!!!!)
	runFS("server_location_here", fsName, mountDir=mountDir, foreground=False)

	# return response message
	return "Successfully mounted " + fsName + " to " + mountDir

@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def catch_all(path):
	return app.send_static_file(path)

if __name__ == '__main__':
	app.run(debug=True)
	#app.run(host='0.0.0.0')