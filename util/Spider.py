import datetime
import json
import random
import threading
from threading import Thread
import time
from threading import Timer
from tkinter import Tk
from tkinter.ttk import Treeview

from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver import Chrome, ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from util.Pool import getRedisConn
from queue import Queue
import logging


class Spider():
    def __init__(self, master: Treeview, liveId='', userDictFile='', que: Queue = None, thread: Thread = None):
        self.liveId = liveId
        self.userDictFile = userDictFile
        self.master = master
        self.queue = que
        self.thread = thread
        self.threadEvent = threading.Event()
        self.web = None

        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def start(self):
        if self.master.master:
            self.master.master.focus_force()
        liveId = self.liveId
        pageUrl = f"https://live.douyin.com/{liveId}"

        option = Options()
        option.add_argument("--disable-extensions")
        option.add_argument("--ignore-certificate-errors")
        option.set_capability("acceptInsecureCerts", True)

        self.web = Chrome(options=option)
        timer = Timer(1800, self.moveMouse)
        timer.start()
        self.web.get(pageUrl)
        logging.info("爬虫启动")
        time.sleep(1)
        logging.info(f"线程事件是否设置: {self.threadEvent.is_set()}")

        try:
            while not self.threadEvent.is_set():
                if not self.master.master.spidering:
                    break
                self.check_if_live_ended()
                self.process_chat_messages()
        finally:
            self.cleanup(timer)

    def check_if_live_ended(self):
        try:
            overEle = self.web.find_element(By.XPATH, '//*[@id="island_e4c6d"]/div/span')
            if overEle.text == '直播已结束':
                self.threadEvent.set()
                self.thread.join()
                self.queue.join()
                self.master.master.warning()
                return True
        except NoSuchElementException:
            pass
        return False

    def process_chat_messages(self):
        eles = self.web.find_elements(By.CLASS_NAME, 'webcast-chatroom___enter-done')
        if eles:
            for ele in eles:
                try:
                    es = ele.find_element(By.CLASS_NAME, 'webcast-chatroom___content-with-emoji-text')
                    if es:
                        id = ele.get_attribute('data-id')
                        message = json.dumps({
                            "content": es.text,
                            "time": time.time(),
                            "id": id,
                            "liveId": self.liveId
                        })
                        self.queue.put(message)
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    logging.error(f"异常发生: {e}")

    def moveMouse(self):
        action = ActionChains(self.web)
        width = Tk().winfo_width()
        height = Tk().winfo_height()
        action.move_by_offset(random.randint(1, width), random.randint(1, height)).perform()

    def cleanup(self, timer):
        if self.web:
            self.web.quit()
        timer.cancel()
        logging.info("清理完成")
