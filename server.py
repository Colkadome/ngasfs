from tables import *
from get import *
from post import *
from ngasfs import *

import os
import string
import random
from multiprocessing import Process

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, StaticFileHandler

mountedFS = {}

class IndexPageHandler(RequestHandler):
	def get(self):
		self.render("index.html")

class CreateFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if os.path.isfile(fsName):
			self.write(fsName + " already exists")
			return

		# create FS and close connection
		initFS(fsName).close()

		# return response message
		self.write("Successfully created " + fsName)

class MountFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write(fsName + " does not exist")
			return

		if fsName in mountedFS:
			mountedFS[fsName].terminate()
			del mountedFS[fsName]
			self.write("Successfully unmounted " + fsName)
		else:
			# create mountDir with random name
			mountDir = random.choice(string.letters) + random.choice(string.letters)

			# connect to FS (MUST RUN AS NEW PROCESS)
			p = Process(target=runFS, args=("sLoc", fsName, mountDir, False, True,))
			p.start()
			#p.join()

			# save process in memory
			mountedFS[fsName] = p
			
			#runFS("server_location_here", fsName, mountDir=mountDir, foreground=False)
			self.write("Successfully mounted " + fsName + " to " + mountDir)

class GetFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		patterns = self.get_body_argument("patterns")

		# get files (MAKE IT ACCEPT ARRAY, OOPS)
		p = Process(target=getFiles, args=(sLoc, fsName, patterns,))
		p.start()

		# return response message (SYNC WITH PROCESS?)
		self.write("Successfully got files")

class GetFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# download FS
		downloadFS(sLoc, fsName)

		# return response message
		self.write("Successfully downloaded " + fsName)

def make_app():

	settings = {
		"static_path": os.path.join(os.path.dirname(__file__), r"static"),
		"template_path": os.path.join(os.path.dirname(__file__), r"static")
	}

	handlers = [
		url(r"/create_fs", CreateFSHandler),
		url(r"/mount_fs", MountFSHandler),
		url(r"/get_files", GetFilesHandler),
		url(r"/get_fs", GetFSHandler),
		url(r"/", IndexPageHandler),
		url(r"/(.*)", StaticFileHandler, {'path': settings['static_path']}),
	]

	return Application(handlers, **settings)

def main():
    app = make_app()
    app.listen(8888)
    IOLoop.current().start()

if __name__ == '__main__':
	main()
