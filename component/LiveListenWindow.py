import json
import queue
from threading import Thread
from tkinter import Toplevel, messagebox, Button
from tkinter.ttk import Treeview
from util.Pool import getRedisConn
from util.Spider import Spider


class LiveListenWindow(Toplevel):

    def __init__(self, master=None, **kwargs):
        super().__init__(master=master, **kwargs)
        self.master = master
        # 324980718774
        self.geometry('%dx%d+%d+%d' % (
            master.mainWidth, master.mainHeight, (master.screenWidth - master.mainWidth) // 2 + 30,
            (master.screenHeight - master.mainHeight) // 2 + 30))
        self.title('直播间弹幕采集')
        self.focus_get()
        Button(self, text='采集结束', command=self.close).pack()
        self.threadList = {}
        self.queueList = {}

    def show(self, liveId='', userDictFile='', opera=0):
        self.liveId = liveId
        self.spidering = True
        self.opera = opera
        self.protocol('WM_DELETE_WINDOW', self.close)
        self.focus_set()
        columns = ('内容', '弹幕id', '时间')
        self.treeview = Treeview(self, columns=columns, height=self.master.mainHeight // 20)
        self.treeview.pack(fill='x', expand=True)
        for i in range(len(columns)):
            self.treeview.heading(columns[i], text=columns[i])
            self.treeview.column(columns[i], anchor='center')
        self.queueList[liveId] = q = queue.Queue()
        spider = Spider(master=self.treeview, liveId=liveId, userDictFile=userDictFile, que=q)
        self.threadList[liveId] = Thread(target=spider.start, name="爬取弹幕信息", args=()).start()
        self.listen(q, liveId)

    def listen(self, q: queue.Queue, liveId=''):
        message = q.get()
        q.task_done()
        message = json.loads(message)
        pushKey = 'dy:message:push:{}'.format(liveId)
        redisClient = getRedisConn()
        redisClient.lpush(pushKey, message['id'])

        hashKey = 'dy:message:hash:{}:{}'.format(liveId, message['id'])
        redisClient.hset(hashKey, 'id', message['id'])
        redisClient.hset(hashKey, 'content', message['content'])
        redisClient.hset(hashKey, 'time', message['time'])

        self.treeview.insert('', q.qsize(), values=(message['content'], message['id'], message['time']))

    def warning(self):
        if messagebox.showinfo('提示', '直播已经结束'):
            self.spidering = False

    def close(self):
        for thread in self.threadList:
            thread.join()
        for queue in self.queueList:
            queue.join()
        if self.spidering:
            if not messagebox.askyesno('销毁？', '正在采集，确定要退出吗？'):
                return
        self.spidering = False
        if self.opera != 0:
            # TODO:保存数据到excel或者数据库
            pass
        self.destroy()
