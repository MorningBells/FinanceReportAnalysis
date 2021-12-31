# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import random

import requests, json, time
from bs4 import BeautifulSoup
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
import pymongo


class Xinalang():
    def __init__(self):
        self.queue = Queue()
        self.info = []
        self.json = []
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mydb = self.myclient["stock"]
        self.balanceSheetDb = self.mydb["financial_report_source_balanceSheet"]
        self.profitStatementDb = self.mydb["financial_report_source_profitStatement"]
        self.cashFlowDb = self.mydb["financial_report_source_cashFlow"]

    def req(self, ninfo):
        try:
            self.balanceSheet(ninfo)
            time.sleep(2)
            self.profitStatement(ninfo)
            time.sleep(3)
            self.cashFlow(ninfo)
        except TimeoutError:
            print("超时")
        except:
            print("其他错误")
            info = json.loads(ninfo)
            print(info["SECNAME"], info["year"])


    def balanceSheet(self, ninfo):
        info = json.loads(ninfo)
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
            info["SECCODE"], info["year"])
        data = self.getSourceData(url)
        my_dict = {**info, **data}
        print(info["SECNAME"], info["year"])
        self.balanceSheetDb.insert_one(my_dict)

    def profitStatement(self, ninfo):
        info = json.loads(ninfo)
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
            info["SECCODE"], info["year"])
        data = self.getSourceData(url)
        my_dict = {**info, **data}
        print(info["SECNAME"], info["year"])
        self.profitStatementDb.insert_one(my_dict)

    def cashFlow(self, ninfo):
        info = json.loads(ninfo)
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
            info["SECCODE"], info["year"])
        data = self.getSourceData(url)
        my_dict = {**info, **data}
        print(info["SECNAME"], info["year"])
        self.cashFlowDb.insert_one(my_dict)

    def getSourceData(self, url):
        headers = {}
        response = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(response.content.decode("gb2312"), "lxml")
        '''报表日期'''
        trs = soup.select("tbody tr")
        data = {}
        for tr in trs:
            tds = tr.select("td")
            if tds != []:
                try:
                    value = tds[1].text
                    if value == "--":
                        value = "0.00"
                    data[tds[0].text] = value
                except:
                    pass
        return data

    def scheduler(self):
        # year_list=[2014,2015,2016,2017,2018]
        year_list = [2020]

        with open("stockCode.txt", encoding="utf8") as f:
            lines = f.readlines()

        slice = random.sample(lines, 1)

        for line in slice:
            info = json.loads(line)
            for year in year_list:
                info["year"] = year
                info_str = json.dumps(info)
                print(json.loads(info_str))

                self.queue.put(info_str)

        pool = ThreadPoolExecutor(max_workers=8)
        while not self.queue.empty():
            pool.submit(self.req, self.queue.get())
        pool.shutdown()

        print("剩下：" + str(len(self.info)))
        # while len(self.info)>0:
        #     self.req(self.info.pop())

        # self.write_json()

    def write_json(self):
        try:
            for j in self.json:
                with open('data.json', 'a') as f:
                    json.dump(j, f)
        except:
            print("写入出错！！")
            pass


if __name__ == '__main__':
    start_time = time.time()

    X = Xinalang()
    X.scheduler()

    print("总耗时：{}秒".format(time.time() - start_time))
