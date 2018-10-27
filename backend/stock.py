import tushare
import json
from time import sleep
from os import getcwd
import os.path


import time

"""
code,代码
name,名称
industry,所属行业
area,地区
pe,市盈率
outstanding,流通股本(亿)
totals,总股本(亿)
totalAssets,总资产(万)
liquidAssets,流动资产
fixedAssets,固定资产
reserved,公积金
reservedPerShare,每股公积金
esp,每股收益
bvps,每股净资
pb,市净率
timeToMarket,上市日期
undp,未分利润
perundp, 每股未分配
rev,收入同比(%)
profit,利润同比(%)
gpr,毛利率(%)
npr,净利润率(%)
holders,股东人数
"""


def save_stock_list():
    stocks = tushare.get_stock_basics()
    all = {}
    idx = 0
    for i, r in stocks.iterrows():
        d = r.to_dict()
        d["code"] = i
        all[idx] = d
        idx += 1

    print("all stocks[{}]".format(idx - 1))
    with open("stocks.json", "w") as f:
        json.dump(all, f)


def get_data_info(path, code, name):
    data = {}
    if name[0:1]=="*":
        name = name[1:]
    file = os.path.join(path,"{}_{}.json".format(code, name))
    print("get {} into {}".format(name, file))
    kdata = tushare.get_hist_data(code)
    if kdata is None:
        return False
    idx = 0
    for i, v in kdata.iterrows():
        d = v.to_dict()
        d["date"] = i
        data[idx] = d
        idx += 1
    with open(file, "w") as f:
        json.dump(data, f)
    return True


def get_all_data():
    save_stock_list()
    with open("stocks.json") as f:
        data = json.load(f)
    failed = []
    cnt = 0
    path = getcwd()
    print("path={}".format(path))
    # path = "{}/{}".format(path, "data")
    path = os.path.join(path,"data")
    print("path={}".format(path))
    for k, d in data.items():
        all = {}
        idx = 0
        print("get {} datas".format(cnt))
        if cnt % 10 == 0:
            sleep(1)
        cnt += 1
        flag = get_data_info(path, d["code"], d["name"])
        if flag is False:
            failed.append({"code": d["code"], "name": d["name"]})
            print("get {} {} faild".format(d["code"], d["name"]))
            sleep(10)

    print("get [{}] faild".format(len(failed))) if len(failed) > 0 else print(
        "get all data successed")

    cnt = 10
    while len(failed) > 0 and cnt > 0:
        failed = [
            x for x in failed
            if get_data_info(path, x["code"], x["name"]) is False
        ]
        cnt -= 1
        print(failed)


get_all_data()
