from Tkinter import *
from post import *
from get import *

class App:

	def __init__(self, master):

		# FRAME 1 #
		###########

		frame1 = Frame(master)
		frame1.pack()

		self.button = Button(frame1, text="QUIT", fg="red", command=master.quit)
		self.button.pack(side=LEFT)

		self.hi_there = Button(frame1, text="Hello", command=self.say_hi)
		self.hi_there.pack(side=LEFT)

		# FRAME 2 #
		###########

		frame2 = Frame(master)
		frame2.pack()

		# GET
		self.get_label = Label(frame2, text="GET FILES")
		self.get_label.pack(side=LEFT)

		# Server Location
		self.server_loc_label = Label(frame2, text="Server Location:")
		self.server_loc_label.pack(side=LEFT)

		self.server_loc = Entry(frame2)
		self.server_loc.pack(side=LEFT)

		# FS path
		self.fs_id_label = Label(frame2, text="FS path:")
		self.fs_id_label.pack(side=LEFT)

		self.fs_id = Entry(frame2)
		self.fs_id.pack(side=LEFT)

		# GET button
		self.get_files = Button(frame2, text="GET", command=self.run_getFiles)
		self.get_files.pack(side=LEFT)

		# FRAME 3 #
		###########

		frame3 = Frame(master)
		frame3.pack()

		# console
		self.console_text = Text(frame3)
		self.print_to_console("Console:")
		self.console_text.pack()

	def say_hi(self):
		self.print_to_console(self.server_loc.get())

	def print_to_console(self, text):
		self.console_text.config(state=NORMAL)
		self.console_text.insert(END, text + "\n")
		self.console_text.config(state=DISABLED)

	def run_getFiles(self):
		getFiles(self.server_loc.get(), self.fs_id.get(), "poo")

	def run_getFS(self):
		getFS(self.server_loc.get(), self.fs_id.get())

	def run_postFiles(self):
		postFiles(self.server_loc.get(), self.fs_id.get(), "mountDir", "patterns", "options")

	def run_postFS(self):
		postFS(self.server_loc.get(), self.fs_id.get(), "mountDir", "options")

def main():
	root = Tk()

	app = App(root)

	root.mainloop()

if __name__ == "__main__":
    main()