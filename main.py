import tkinter as tk

from gui import OsuDownloaderGUI


def main():
    root = tk.Tk()
    app = OsuDownloaderGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
