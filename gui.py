from Tkinter import *
from post import *
from get import *

class App():

	def __init__(self, master):
		
		frame = Frame(master)
		frame.pack()

		# Grid layout

		frame.columnconfigure(0, weight=0)
		frame.columnconfigure(1, weight=1)
		frame.rowconfigure(0, weight=0)
		frame.rowconfigure(1, weight=0)
		frame.rowconfigure(2, weight=1)

		# Elements

		self.server_loc_label = Label(frame, text="Server Location:")
		self.server_loc_input = Entry(frame)

		self.fs_id_label = Label(frame, text="FS file path:")
		self.fs_id_input = Entry(frame)

		self.console_text = Text(frame)
		self.console_text.config(state=DISABLED)

		# Element locations

		self.server_loc_label.grid(row=0, column=0, padx=4, sticky=W)
		self.server_loc_input.grid(row=0, column=1, sticky=E+W)

		self.fs_id_label.grid(row=1, column=0, padx=4, sticky=W)
		self.fs_id_input.grid(row=1, column=1, sticky=E+W)

		self.console_text.grid(row=2, column=0, padx=4, rowspan=1, columnspan=2, sticky=N+S+E+W)

		# Further Init

		self.print_to_console("hello!")        

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
	root.geometry("300x200+50+50")
	root.title("ngasfs")
	app = App(root)
	root.mainloop()

if __name__ == "__main__":
    main()