#!/usr/bin/.env python
# -*- coding: utf-8 -*-

import datetime
import random
import time
from threading import Timer
from tkinter import Tk
from tkinter.ttk import Treeview

from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver import Chrome, ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from util.Pool import getRedisConn


class Spider():
    def __init__(self, master: Treeview, liveId='', userDictFile=''):
        self.liveId = liveId
        self.userDictFile = userDictFile
        self.master = master

    def start(self, liveId=''):
        if self.master.master:
            self.master.focus_force()
        if not liveId:
            liveId = self.liveId
        pageUrl = "https://live.douyin.com/{}".format(liveId)

        option = Options()
        option.add_argument("--disable-extensions")  # 给option对象添加无头参数
        option.add_argument("--ignore-certificate-errors")  # 忽略证书异常错误
        option.set_capability("acceptInsecureCerts", True)

        self.web = Chrome(options=option)
        # self.web.minimize_window()  # 窗口最小化
        self.web.get(pageUrl)
        # 定时器 半个小时动一下鼠标
        timer = Timer(1800, function=self.moveMouse, args=())
        timer.start()
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "启动了")
        time.sleep(1)
        while self.master.master.spidering:
            try:
                overEle = self.web.find_element(By.XPATH,
                                                '//*[@id="_douyin_live_scroll_container_"]/div/div/div/div[2]/div[2]/div[2]/div')
                if overEle.text == '直播已结束':
                    self.master.master.warning()
                    timer.cancel()
                    break
            except NoSuchElementException:
                print('未获取到直播结束')
                # 弹窗
                pass
            eles = self.web.find_elements(By.CLASS_NAME, 'webcast-chatroom___enter-done')
            if len(eles) > 0:
                for ele in eles:
                    redisConn = getRedisConn()
                    try:
                        es = ele.find_element(By.CLASS_NAME, 'webcast-chatroom___content-with-emoji-text')
                        id = ele.get_attribute('data-id')
                        hashKey = 'dy:message:hash:{}:{}'.format(liveId, id)
                        if not es or redisConn.exists(hashKey):
                            continue
                        pushKey = 'dy:message:push:{}'.format(liveId)

                        redisConn.lpush(pushKey, id)
                        redisConn.hset(hashKey, 'id', id)
                        redisConn.hset(hashKey, 'content', es.text)
                        redisConn.hset(hashKey, 'time', int(time.time()))

                        currentTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        # print(currentTime, id, es.text)
                        self.master.insert('', '0', values=(currentTime, id, es.text))
                        # 分词检测
                        # self.master.master.update()
                    except NoSuchElementException:
                        pass
                    except StaleElementReferenceException:
                        pass

    def moveMouse(self):
        action = ActionChains(self.web)
        action.move_by_offset(random.randint(1, Tk().winfo_width()), random.randint(1, Tk().winfo_height())).perform()
