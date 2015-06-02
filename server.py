from tables import *
from get import *
from post import *
from ngasfs import *

import os
import string
import random
from multiprocessing import Process
import json

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
		self.write({"status":r.status, "statusText":"Server is OK!"})

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
			self.write({"status":0, "statusText":fsName + " exists"})
			return

		# create FS and close connection
		initFS(fsName).close()

		# return response message
		self.write({"status":0, "statusText":"Created " + fsName})

class CleanFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
			return

		# clean FS
		cleanFS(fsName)

		# return response message
		self.write({"status":0, "statusText":"Cleaned " + fsName})

class MountFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		# ADD A CHECK_ARGUMENT FUNCTION
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write({"status":2, "statusText":fsName + " does not exist"})
			return

		if fsName in processes and processes[fsName].is_alive():
			processes[fsName].terminate()
			#del processes[fsName]
			self.write({"status":1, "statusText":"Unmounting " + fsName})
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
				self.write({"status":0, "statusText":"Mounted " + fsName + " to " + mountDir})
			else:
				self.write({"status":2, "statusText":"ERROR: " + fsName + " is busy!"})

class GetFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")
		sLoc = self.get_body_argument("sLoc")
		T = json.loads(self.get_body_argument("T"))	# JSON used for objects

		# check if fs exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
			return

		# add files
		count = getFilesFromList(sLoc, fsName, T)

		# return response
		self.write({"status":0, "statusText":str(count) + " file(s) added to " + fsName})

class GetFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# download FS
		status = downloadFS(sLoc, fsName)
		if status==1:
			self.write({"status":1, "statusText":"Could not download " + fsName + ", file already exists"})
		elif status==2:
			self.write({"status":2, "statusText":"Could not download " + fsName + ", HTTP error"})
		elif status==3:
			self.write({"status":3, "statusText":"Could not download " + fsName + ", URL error"})
		else:
			self.write({"status":0, "statusText":"Downloaded " + fsName})

class PostFilesHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")
		force = int(self.get_body_argument("force"))
		ids = json.loads(self.get_body_argument("ids"))	# JSON used for objects

		# check if fs exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
			return

		# post files
		count = postFilesWithList(sLoc, fsName, ids, True, force)

		# return response
		self.write({"status":0, "statusText":str(count) + " file(s) added to " + sLoc})

class PostFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		# check if file already exists
		if not fsName.endswith('.sqlite'):
			fsName = fsName + ".sqlite"
		if not os.path.isfile(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
			return

		# post files
		status = postFS(sLoc, fsName)
		if status!=200:
			self.write({"status":status, "statusText":str(status) + ", Could not upload " + fsName})
		else:
			self.write({"status":0, "statusText":"Uploaded " + fsName})

class SearchFSHandler(RequestHandler):
	def get(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_argument("fsName")
		patterns = self.get_argument("patterns").split()

		L = getFSList(fsName, patterns)
		self.write({"status":0, "statusText":"Found File System Files", "L":L})


class SearchServerHandler(RequestHandler):
	def get(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_argument("sLoc")
		patterns = self.get_argument("patterns").split()

		T = getServerList(sLoc, patterns)
		self.write({"status":0, "statusText":"Found Server Files", "T":T})
		

def make_app():

	settings = {
		"static_path": os.path.join(os.path.dirname(__file__), r"static"),
		"template_path": os.path.join(os.path.dirname(__file__), r"static")
	}

	handlers = [
		url(r"/create_fs", CreateFSHandler),
		url(r"/mount_fs", MountFSHandler),
		url(r"/clean_fs", CleanFSHandler),
		url(r"/check_server", CheckServerHandler),
		url(r"/search_fs", SearchFSHandler),
		url(r"/search_server", SearchServerHandler),
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
