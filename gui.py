from Tkinter import *
from post import *
from get import *

class App():

	def __init__(self, master):
		
		frame = Frame(master)
		frame.pack(fill=BOTH, expand=1)

		# Grid layout

		frame.columnconfigure(0, weight=0)
		frame.columnconfigure(1, weight=1)
		frame.rowconfigure(0, weight=0)
		frame.rowconfigure(1, weight=0)
		frame.rowconfigure(2, weight=0)
		frame.rowconfigure(3, weight=0)
		frame.rowconfigure(4, weight=0)
		frame.rowconfigure(5, weight=0)
		frame.rowconfigure(6, weight=1)

		# Elements

		self.server_loc_label = Label(frame, text="Server Location:")
		self.server_loc_input = Entry(frame)

		self.fs_id_label = Label(frame, text="FS id:")
		self.fs_id_input = Entry(frame)

		self.mountDir_label = Label(frame, text="Mount directory:")
		self.mountDir_input = Entry(frame)

		self.create_fs_button = Button(frame, text="Create FS", command=self.run_ngasfs)
		self.download_fs_button = Button(frame, text="Download FS", command=self.run_downloadFile)
		self.upload_fs_button = Button(frame, text="Upload FS", command=self.run_postFS)

		self.patterns_label = Label(frame, text="File Patterns:")
		self.patterns_input = Entry(frame)

		self.get_files_button = Button(frame, text="Get Files", command=self.run_getFiles)
		self.post_files_button = Button(frame, text="Post Files", command=self.run_postFiles)

		self.console_text = Text(frame)
		self.console_text.config(state=DISABLED)

		# Element locations

		self.server_loc_label.grid(row=0, column=0, padx=4, sticky=W)
		self.server_loc_input.grid(row=0, column=1, columnspan=2, sticky=E+W)

		self.fs_id_label.grid(row=1, column=0, padx=4, sticky=W)
		self.fs_id_input.grid(row=1, column=1, columnspan=2, sticky=E+W)

		self.mountDir_label.grid(row=2, column=0, padx=4, sticky=W)
		self.mountDir_input.grid(row=2, column=1, columnspan=2, sticky=E+W)

		self.create_fs_button.grid(row=3, column=0, sticky=W)
		self.download_fs_button.grid(row=3, column=1, sticky=W)
		self.upload_fs_button.grid(row=3, column=1, sticky=E)	# fix this

		self.patterns_label.grid(row=4, column=0, padx=4, sticky=W)
		self.patterns_input.grid(row=4, column=1, columnspan=2, sticky=E+W)

		self.get_files_button.grid(row=5, column=0, sticky=W)
		self.post_files_button.grid(row=5, column=1, sticky=W)

		self.console_text.grid(row=6, column=0, padx=4, rowspan=1, columnspan=2, sticky=N+S+E+W)

		# Further Init

		self.print_to_console("Console:")    

	def print_to_console(self, text):
		self.console_text.config(state=NORMAL)
		self.console_text.insert(END, text + "\n")
		self.console_text.config(state=DISABLED)

	def get_server_loc(self):
		server_loc = self.server_loc_input.get()
		if server_loc == "":
			self.print_to_console("Please specify Server Location")
			return 0
		return server_loc

	def get_fs_id(self):
		fs_id = self.fs_id_input.get()
		if fs_id == "":
			self.print_to_console("Please specify FS id")
			return 0
		# check for the '.sqlite' extension on fs_id
		if not fs_id.endswith('.sqlite'):
			fs_id = fs_id + ".sqlite"
		return fs_id

	def get_mountDir(self):
		mountDir = self.mountDir_input.get()
		if mountDir == "":
			self.print_to_console("Please specify Mount directory")
			return 0
		return mountDir

	def get_patterns(self):
		patterns = self.patterns_input.get()
		if patterns == "":
			self.print_to_console("Please specify File patterns")
			return 0
		return patterns.split()

	def run_ngasfs(self):
		print "run ngasfs here"

	def run_getFiles(self):

		server_loc = self.get_server_loc()
		if not server_loc:
			return 0

		fs_id = self.get_fs_id()
		if not server_loc:
			return 0

		patterns = self.get_patterns()
		if not patterns:
			return 0

		self.print_to_console("Getting files from " + server_loc)
		getFiles(server_loc, fs_id, patterns)

	def run_downloadFile(self):

		server_loc = self.get_server_loc()
		if not server_loc:
			return 0

		fs_id = self.get_fs_id()
		if not server_loc:
			return 0

		self.print_to_console("Downloading " + fs_id + " from " + server_loc)
		downloadFile(server_loc, fs_id)

	def run_postFiles(self):

		server_loc = self.get_server_loc()
		if not server_loc:
			return 0

		fs_id = self.get_fs_id()
		if not server_loc:
			return 0

		mountDir = self.get_mountDir()
		if not mountDir:
			return 0

		patterns = self.get_patterns()
		if not patterns:
			return 0

		self.print_to_console("Posting files to " + server_loc)
		postFiles(server_loc, fs_id, mountDir, patterns)

	def run_postFS(self):

		server_loc = self.get_server_loc()
		if not server_loc:
			return 0

		fs_id = self.get_fs_id()
		if not server_loc:
			return 0

		mountDir = self.get_mountDir()
		if not mountDir:
			return 0

		self.print_to_console("Uploading " + fs_id + " to " + server_loc)
		postFS(server_loc, fs_id, mountDir)

def main():
	root = Tk()
	root.geometry("300x200+50+50")
	root.title("ngasfs")
	app = App(root)
	root.mainloop()

if __name__ == "__main__":
    main()