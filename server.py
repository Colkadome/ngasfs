from tables import *
from get import *
from post import *
from ngasfs import *

import os
import string
import random
from multiprocessing import Process

from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado.web import RequestHandler, Application, url, StaticFileHandler

processes = {}

# ADD MULTIPLE CLIENT SUPPORT

class CheckServerHandler(RequestHandler):
	def get(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_argument("sLoc")
		
		# get host and port
		o = urlparse(sLoc)
		host = o.hostname
		port = o.port

		# connect to URL
		conn = httplib.HTTPConnection(host+":"+str(port))
		conn.connect()
		conn.putrequest('GET', '/STATUS')
		conn.endheaders()

 		# get response
		r = conn.getresponse()
		if r.status == 200:
			print r.read()
		else:
			print r.status, r.reason
		self.write({"status":r.status})

def checkServer(sLoc):

	# get host and port
	o = urlparse(sLoc)
	host = o.hostname
	port = o.port

	# connect to URL
	conn = httplib.HTTPConnection(host+":"+str(port))
	conn.connect()
	conn.putrequest('GET', '/STATUS')
	conn.endheaders()

 	# get response
	r = conn.getresponse()
	print r.status, r.reason
	return r.status

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
		self.write("Created " + fsName)

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

		if fsName in processes and processes[fsName].is_alive():
			processes[fsName].terminate()
			#del processes[fsName]
			self.write("Unmounting " + fsName)
		else:
			# create mountDir with name of FS
			mountDir = fsName.replace('.sqlite', '')

			# check if mountDir is currently mounted
			if not os.path.ismount(mountDir):
				# run ngasfs
				p = Process(target=runFS, args=("sLoc", fsName, mountDir, False, True,))
				p.start()
				#p.join()
				processes[fsName] = p
				self.write("Mounted " + fsName + " to " + mountDir)
			else:
				self.write("ERROR: " + fsName + " is busy!")

class GetFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		patterns = self.get_body_argument("patterns").split()

		# check if fs exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write(fsName + " does not exist")
			return

		# get files
		count = getFiles(sLoc, fsName, patterns)

		# return response
		self.write(str(count) + " file(s) added to " + fsName)

class GetFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# download FS
		status = downloadFS(sLoc, fsName)
		if status==1:
			self.write("Could not download " + fsName + ", file already exists")
		elif status==2:
			self.write("Could not download " + fsName + ", HTTP error")
		elif status==3:
			self.write("Could not download " + fsName + ", URL error")
		else:
			self.write("Downloaded " + fsName)

class PostFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		patterns = self.get_body_argument("patterns").split()
		force = int(self.get_body_argument("force"))

		# check if fs exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write(fsName + " does not exist")
			return

		# post files
		count = postFiles(sLoc, fsName, patterns, True, force)

		# return response
		self.write(str(count) + " file(s) added to " + sLoc)

class PostFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write(fsName + " does not exist")
			return

		# post files
		status = postFS(sLoc, fsName)
		if status!=200:
			self.write(str(status) + ", Could not upload " + fsName)
		else:
			self.write("Uploaded " + fsName)

def make_app():

	settings = {
		"static_path": os.path.join(os.path.dirname(__file__), r"static"),
		"template_path": os.path.join(os.path.dirname(__file__), r"static")
	}

	handlers = [
		url(r"/create_fs", CreateFSHandler),
		url(r"/mount_fs", MountFSHandler),
		url(r"/check_server", CheckServerHandler),
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
    server = HTTPServer(app)
    server.listen(8888)
    IOLoop.current().start()

if __name__ == '__main__':
	main()
