import json
import queue
import time
from threading import Thread
from tkinter import Toplevel, messagebox, Button
from tkinter.ttk import Treeview
from util.Pool import getRedisConn
from util.Spider import Spider


class LiveListenWindow(Toplevel):
    def __init__(self, master=None, **kwargs):
        super().__init__(master=master, **kwargs)
        self.master = master
        self.geometry('%dx%d+%d+%d' % (
            master.mainWidth, master.mainHeight, (master.screenWidth - master.mainWidth) // 2 + 30,
            (master.screenHeight - master.mainHeight) // 2 + 30))
        self.title('直播间弹幕采集')
        self.focus_get()
        Button(self, text='采集结束', command=self.close).pack()
        self.threadList = {}
        self.queueList = {}
        self.spidering = False

    def show(self, liveId='', userDictFile='', opera=0):
        self.liveId = liveId
        self.spidering = True
        self.opera = opera
        self.protocol('WM_DELETE_WINDOW', self.close)
        self.focus_set()

        columns = ('内容', '弹幕id', '时间')
        self.treeview = Treeview(self, columns=columns, height=self.master.mainHeight // 20)
        self.treeview.pack(fill='x', expand=True)
        for col in columns:
            self.treeview.heading(col, text=col)
            self.treeview.column(col, anchor='center')

        # 创建队列和线程
        self.queueList[liveId] = q = queue.Queue()
        spider = Spider(master=self.treeview, liveId=liveId, userDictFile=userDictFile, que=q)
        self.threadList[liveId] = Thread(target=spider.start, name="爬取弹幕信息", daemon=True)
        self.threadList[liveId].start()

        # 开始监听队列
        self.listen(q, liveId)

    def listen(self, q: queue.Queue, liveId=''):
        if not self.spidering:
            return

        try:
            message = q.get(timeout=1)
            q.task_done()
            message = json.loads(message)
            messageId, messageContent, messageTime = message['id'], message['content'], message['time']
            print(f"messageId: {messageId}, messageContent: {messageContent}, messageTime: {messageTime}")
            if self.opera == 2:
                redisClient = getRedisConn()
                redisKey = f'dy:message:hash:{liveId}:{messageId}'
                if not redisClient.exists(redisKey):
                    redisClient.lpush(f'dy:message:push:{liveId}', messageId)
                    redisClient.hset(redisKey, mapping=message)

            self.treeview.insert('', len(self.treeview.get_children())-1, values=(messageContent, messageId, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(messageTime))))
        except queue.Empty:
            pass
        finally:
            self.after(100, self.listen, q, liveId)

    def warning(self):
        if messagebox.showinfo('提示', '直播已经结束'):
            self.spidering = False

    def close(self):
        self.spidering = False
        for liveId, thread in self.threadList.items():
            thread.join(timeout=1)
            if thread.is_alive():
                print(f"Warning: Thread {thread.name} for {liveId} is still running.")
        for q in self.queueList.values():
            q.join()
        self.destroy()
