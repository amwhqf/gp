import json
from pandas import DataFrame, Series
from os import getcwd, access, F_OK
import time
import tushare
import talib

def get_ema(df, n):
    size = len(df)
    for i in range(size - 1, -1, -1):
        if i == size - 1:
            df.ix[i, "ema"] = df.ix[i, "close"]
        else:
            df.ix[i, "ema"] = float("%.2f"%((2 * df.ix[i, "close"] +
                               (n - 1) * df.ix[i + 1, "ema"]) / (n + 1)))
    ema = list(df["ema"])
    return ema

def get_sma(df,colunm='close',n=5,m=1):
    size = len(df)
    if n<= m:
        return None
    for i in range(size - 1, -1, -1):
        if i == size - 1:
            df.ix[i, "sma"] = df.ix[i, colunm]
        else:
            df.ix[i, "sma"] = float("%.2f"%((m * df.ix[i, colunm] +
                               (n - m) * df.ix[i + 1, "sma"]) / n))
    sma=list(df["sma"])
    # print(df[["date","close","sma"]])
    return sma

def get_rsv(df,i,n):
    close =float("%.2f"%(df.ix[i, "close"]))
    x = int(i + n)-1
    # if i == 0:
    #     print(x)
    #     print(df.ix[i:x,['date','low']])

    if x >=len(df)-1:
        x = len(df)-1
        # print(i, x)
    llv = float("%.2f"%(min(df.ix[i:x,'low'])))
    hhv = float("%.2f"%(max(df.ix[i:x,"high"])))
    if close -llv == 0:
        rsv = 0
    else:
        rsv = float("%.2f"%((close-llv)/(hhv - llv)*100))
    return rsv

def get_kdj(df,n=9,M1=3,M2=3):
    '''
    RSV:=(CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100;
    K:SMA(RSV,M1,1);
    D:SMA(K,M2,1);
    J:3*K-2*D;
    :param df:
    :param n:
    :param M1:
    :param M2:
    :return:K,D,J
    '''
    size = len(df)
    rsv = list()
    for i in range(size):
        rsv.append(get_rsv(df,i,n))
    df['rsv']=Series(rsv)
    df['k']=get_sma(df,colunm='rsv',n=3,m=1)
    df['d']=get_sma(df,colunm='k',n=3,m=1)
    # df['j']=3*df['k']-2*df['d']
    df['j']=df.apply(lambda x: float("%.2f"%(3*x['k'])) - float("%.2f"%(2 * x['d'])), axis=1)
    return df

def get_macd(df, short=12, long=26, M=9):
    a = get_ema(df, short)
    b = get_ema(df, long)
    df["diff"] = Series(a) - Series(b)
    size = len(df)
    for i in range(size - 1, -1, -1):
        if i == size - 1:
            df.ix[i, "dea"] = float("%.2f"%(df.ix[i, "diff"]))
        else:
            df.ix[i, "dea"] = float("%.2f"% ((2 * df.ix[i, "diff"] +
                               (M - 1) * df.ix[i + 1, "dea"]) / (M + 1)))
    df["macd"] = 2 * (df["diff"] - df["dea"])
    return df


def analysis_five(data):
    five = data.nlargest(5, "date")
    grow = five[five.close - five.open > 0]
    if grow.iloc[:, 0].size == 5:
        return True
    return False


def analysis_20_avg(data):
    tw = data.nlargest(20, "date")
    last = tw.iloc[0].close
    avg = tw.close.mean()
    if last > avg and tw.iloc[0]["diff"] > 0 and tw.iloc[0][
            "dea"] > 0 and tw.iloc[0]["macd"] > 5:
        return True
    return False


def analysis():
    with open("stocks.json", "r") as f:
        stocks = json.load(f)
    l5 = []
    la = []
    cnt = 0
    tol = len(stocks.keys())
    for infos in stocks.values():
        path = getcwd()
        path = "{}/data/{}_{}.json".format(path, infos["code"], infos["name"])
        cnt += 1
        if access(path, F_OK) is False:
            print("{} {} 数据缺失！".format(infos["code"], infos["name"]))
            continue
        print("分析{} {} ....进度{}/{}".format(infos["code"], infos["name"], cnt,
                                           tol))
        with open(path, "r") as f:
            info = json.load(f)
            for k, v in info.items():
                ts = time.strptime(v["date"], "%Y-%m-%d")
                v["date"] = time.mktime(ts)
            v = info.values()
            tmp = sorted(v, key=lambda x: x["date"], reverse=True)
            sort_info = {}
            for i in range(len(tmp)):
                sort_info[i] = tmp[i]
            data = DataFrame.from_dict(sort_info, orient="index")
            get_macd(data)

            if analysis_five(data) is True:
                l5.append({"code": infos["code"], "name": infos["name"]})
            if analysis_20_avg(data) is True:
                la.append({"code": infos["code"], "name": infos["name"]})
    print("5日连涨股票有{}支".format(len(l5)))
    print("最后交易日收盘价高于20日平均价的有{}支".format(len(la)))


# analysis()
# with open("E:\项目\python_project\gp\dat\\002460_赣锋锂业.json", "r") as f:
#     info = json.load(f)
#     for k, v in info.items():
#         ts = time.strptime(v["date"], "%Y-%m-%d")
#         v["date"] = time.mktime(ts)
#     v = info.values()
#     tmp = sorted(v, key=lambda x: x["date"], reverse=True)
#     sort_info = {}
#     for i in range(len(tmp)):
#         sort_info[i] = tmp[i]
#     data = DataFrame.from_dict(sort_info, orient="index")
#     dt = get_macd(data)
#     print(dt)

df = tushare.get_h_data('002903',start='2017-10-13')
df.reset_index(inplace=True)
print()
# print(df)
# print(df)

# dw = tushare.get_k_data('002903')
# close = dw.close.values
# dw['macd'], dw['macdsignal'], dw['macdhist'] = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
# print(dw)
# df['sma']=get_sma(df,n=3,m=1)
df=get_kdj(df,n=9,M1=3,M2=3)
print(df.ix[:,['date','close','high','low','rsv','k','d','j']])


