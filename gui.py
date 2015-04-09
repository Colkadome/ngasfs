from Tkinter import *

class App:

	def __init__(self, master):

		frame1 = Frame(master)
		frame1.pack()

		self.button = Button(frame1, text="QUIT", fg="red", command=master.quit)
		self.button.pack(side=LEFT)

		self.hi_there = Button(frame1, text="Hello", command=self.say_hi)
		self.hi_there.pack(side=LEFT)

		frame2 = Frame(master)
		frame2.pack()

		self.server_loc_label = Label(frame2, text="Server Location")
		self.server_loc_label.pack(side=LEFT)

		self.server_loc = Entry(frame2)
		self.server_loc.pack(side=LEFT)

		frame3 = Frame(master)
		frame3.pack()

		self.console_text = Text()
		self.print_to_console("Console:")
		self.console_text.pack()

	def say_hi(self):
		print self.server_loc.get()

	def print_to_console(self, text):
		self.console_text.config(state=NORMAL)
		self.console_text.insert(END, text + "\n")
		self.console_text.config(state=DISABLED)

def main():
	root = Tk()

	app = App(root)

	root.mainloop()

if __name__ == "__main__":
    main()