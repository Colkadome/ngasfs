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
		if fsExists(fsName):
			self.write({"status":0, "statusText":fsName + " exists"})
		else:
			initFS(fsName).close()	# create FS and close connection
			self.write({"status":0, "statusText":"Created " + fsName})

class CleanFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")
		if not fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
		else:
			cleanFS(fsName)
			self.write({"status":0, "statusText":"Cleaned " + fsName})

class MountFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		fsName = self.get_body_argument("fsName")
		if not fsExists(fsName):
			self.write({"status":2, "statusText":fsName + " does not exist"})
		else:
			# check if the process exists, and is alive.
			if fsName in processes and processes[fsName].is_alive():
				processes[fsName].terminate()
				#del processes[fsName] # the process might not terminate, so the entry is kept
				self.write({"status":1, "statusText":"Unmounting " + fsName})
			else:
				mountDir = fsName.replace('.sqlite', '') # create mountDir with name of FS
				# check if mountDir is not currently mounted
				if not os.path.ismount(mountDir):
					p = Process(target=runFS, args=("sLoc", fsName, mountDir, False, True,)) # run ngasfs
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

		if not fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
		else:
			count = getFilesFromList(sLoc, fsName, T)
			self.write({"status":0, "statusText":str(count) + " file(s) added to " + fsName})

class GetFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		if fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " already exists"})
		else:
			status = downloadFS(sLoc, fsName) # download FS
			if status==2:
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
		if not fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
		else:
			count = postFilesWithList(sLoc, fsName, ids, True, force)
			self.write({"status":0, "statusText":str(count) + " file(s) added to " + sLoc})

class PostFSHandler(RequestHandler):
	def post(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_body_argument("sLoc")
		fsName = self.get_body_argument("fsName")

		if not fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
		else:
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

		if not fsExists(fsName):
			self.write({"status":1, "statusText":fsName + " does not exist"})
		else:
			L = getFSList(fsName, patterns)
			self.write({"status":0, "statusText":"Found "+str(len(L))+" File System File(s)", "L":L})


class SearchServerHandler(RequestHandler):
	def get(self):
		self.set_header("Content-Type", "text/plain")
		sLoc = self.get_argument("sLoc")
		patterns = self.get_argument("patterns").split()

		T = getServerList(sLoc, patterns)
		self.write({"status":0, "statusText":"Found "+str(len(T))+" Server File(s)", "T":T})
		

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
