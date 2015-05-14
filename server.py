from tables import *
from get import *
from post import *
from ngasfs import *

import os

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, StaticFileHandler

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

		# create FS
		initFS(fsName)

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

		# create mountDir (SHOULD BE RANDOM NAME)
		mountDir = "M1"

		# connect to FS (MUST RUN AS NEW PROCESS)
		runFS("server_location_here", fsName, mountDir=mountDir, foreground=False)

		# return response message
		self.write("Successfully mounted " + fsName + " to " + mountDir)

def make_app():

	settings = {
		"static_path": os.path.join(os.path.dirname(__file__), r"static"),
		"template_path": os.path.join(os.path.dirname(__file__), r"static")
	}

	handlers = [
		url(r"/create_fs", CreateFSHandler),
		url(r"/mount_fs", MountFSHandler),
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
