# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import random

import requests,json,time
from bs4 import BeautifulSoup
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor

class Xinalang():
    def __init__(self):
        self.queue=Queue()
        self.info=[]
        self.json=[]

    def req(self,ninfo):
        try:
            info=json.loads(ninfo)
            scode=info["SECCODE"]
            year=info["year"]
            # print(scode,year)
            data_=info
            url0='http://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(scode,year)
            url1='http://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(scode,year)
            url2='http://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(scode,year)
            url_list=[]
            url_list.extend([url0,url1,url2])
            data_year=[]
            for url in url_list:
                headers={}
                response=requests.get(url,headers=headers,timeout=5)
                soup=BeautifulSoup(response.content.decode("gb2312"),"lxml")

                '''报表日期'''
                trs = soup.select("tbody tr")
                data={}
                for tr in trs:
                    tds=tr.select("td")
                    if tds != []:
                        # print(tds)
                        try:
                            value = tds[1].text
                            if value == "--":
                                value = "0.00"
                            data[tds[0].text] = value
                        except:
                            pass



                data_year.append(data)

            data_["data"]=data_year
            print(info["SECNAME"],info["year"])
            self.json.append(json.dumps(data_))
        except TimeoutError:
            print("超时")
            self.info.append(ninfo)
        except:
            print("其他错误")
            info = json.loads(ninfo)
            print(info["SECNAME"], info["year"])

    def scheduler(self):
        year_list=[2014,2015,2016,2017,2018]

        with open("stockCode.txt",encoding="utf8") as f:
            lines=f.readlines()

        slice = random.sample(lines, 2)

        for line in slice:
            info=json.loads(line)
            for year in year_list:
                info["year"]=year
                info_str=json.dumps(info)
                print(json.loads(info_str))

                self.queue.put(info_str)

        pool=ThreadPoolExecutor(max_workers=8)
        while not self.queue.empty():
            pool.submit(self.req, self.queue.get())
        pool.shutdown()

        print("剩下："+str(len(self.info)))
        while len(self.info)>0:

            self.req(self.info.pop())

        self.write_json()

    def write_json(self):
        try:
            for j in self.json:
                with open('data.json', 'a') as f:
                    json.dump(j, f)
        except:
            print("写入出错！！")
            pass



if __name__ == '__main__':


    start_time=time.time()

    X = Xinalang()
    X.scheduler()

    print("总耗时：{}秒".format(time.time()-start_time))


