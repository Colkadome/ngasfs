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

processes = {}

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

		if fsName in processes:
			processes[fsName].terminate()
			del processes[fsName]
			self.write("Successfully unmounted " + fsName)
		else:
			# create mountDir with random name
			mountDir = random.choice(string.letters) + random.choice(string.letters)

			# connect to FS (MUST RUN AS NEW PROCESS)
			p = Process(target=runFS, args=("sLoc", fsName, mountDir, False, True,))
			p.start()
			#p.join()

			# save process in memory
			processes[fsName] = p
			
			#runFS("server_location_here", fsName, mountDir=mountDir, foreground=False)
			self.write("Successfully mounted " + fsName + " to " + mountDir)

class GetFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		patterns = self.get_body_argument("patterns").split()

		# get files
		p = Process(target=getFiles, args=(sLoc, fsName, patterns,))
		p.start()

		# return response message (SYNC WITH PROCESS?)
		self.write("Getting files...")

class GetFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# download FS
		downloadFS(sLoc, fsName)

		# return response message
		self.write("Downloading " + fsName + "...")

class PostFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		patterns = self.get_body_argument("patterns").split()

		# post files
		p = Process(target=postFiles, args=(sLoc, fsName, patterns,))
		p.start()

		# return response message (SYNC WITH PROCESS?)
		self.write("Posting files...")

class PostFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# post files
		p = Process(target=postFS, args=(sLoc, fsName,))
		p.start()

		# return response message (SYNC WITH PROCESS?)
		self.write("Posting " + fsName + "...")

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
		url(r"/post_files", PostFilesHandler),
		url(r"/post_fs", PostFSHandler),
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
