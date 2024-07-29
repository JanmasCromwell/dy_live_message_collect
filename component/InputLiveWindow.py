from tkinter import Toplevel, Label, Entry, Button, StringVar, filedialog, Radiobutton, IntVar

from component.LiveListenWindow import LiveListenWindow


class InputLiveWindow(Toplevel):
    userDictFile = ''

    def __init__(self, master=None, **kw):
        super().__init__(master=master, **kw)
        self.master = master
        self.title('直播间信息')

    def show(self):
        self.geometry(
            f"+%d+%d" % (
                (self.master.screenWidth - self.master.mainWidth / 4) // 2, (self.master.screenHeight - 40) / 2))
        self.liveIdStringVar = StringVar()
        Label(self, text='直播间ID:').grid(row=0, column=0, padx=(10, 0), pady=10)  # 0行0列
        Entry(self, width=20, textvariable=self.liveIdStringVar).grid(row=0, column=1, columnspan=5, padx=(0, 10),
                                                                      ipadx=60)  # 0行1列，跨2列
        Label(self, text='上传用户热词词典').grid(row=1, column=0, padx=(10, 0))
        self.chooseButton = Button(self, text="选择文件", command=self.chooseFile)
        self.chooseButton.grid(row=1, column=1, columnspan=5, padx=(0, 10))
        Label(self, text='直播结束后的操作').grid(row=2, column=0, padx=(10, 0))
        self.opera = IntVar()
        Radiobutton(self, text='什么都不做', value=0, variable=self.opera).grid(row=2, column=1, padx=(0, 10))
        Radiobutton(self, text='保存到excel', value=1, variable=self.opera).grid(row=2, column=2, padx=(0, 10))
        Radiobutton(self, text='保存到数据库', value=2, variable=self.opera).grid(row=2, column=3, padx=(0, 10))
        Button(self, text="确定", command=self.submit).grid(row=3, column=0, pady=10, ipadx=30)
        Button(self, text="取消", command=self.destroy).grid(row=3, column=3, ipadx=30)

    def submit(self):
        master = self.master
        userDictFile = self.userDictFile
        liveId = self.liveIdStringVar.get()
        self.master.inputWindow = None
        # 创建直播间监听窗口
        child = LiveListenWindow(master=master)
        child.show(userDictFile=userDictFile, liveId=liveId, opera=self.opera.get())
        self.wait_window(child)
        self.destroy()

    def chooseFile(self):
        self.userDictFile = filedialog.askopenfilename()
        self.chooseButton.config(text=self.userDictFile)
        self.focus_force()
