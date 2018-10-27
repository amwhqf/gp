# -*- coding: utf-8 -*-
"""
-------------------------------------------------
 File Name：  test
 Description :
 Author :  amw
 date：   2018/10/9
-------------------------------------------------
 Change Activity:
     2018/10/9:
-------------------------------------------------
"""
import tushare
import pandas as pd
import json
from time import sleep
from os import getcwd
import multiprocessing
import time
import os.path
import pymysql
from sqlalchemy  import create_engine
import datetime

ts_token='32bcbe7714fe65894202c39c6fa166b93d4c1d310d1e46da7f4a5a8e'
tushare.set_token(ts_token)
pro = tushare.pro_api()
yconnect = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/py_db?charset=utf8')
mysql_connect = pymysql.connect(host="127.0.0.1",user="root",password="123456",db="py_db",port=3306)

def hi():
    enddate = ''
    startdate ='20180101'
    sql = '''
        select max(cal_date) as cal_date  from py_db.gp_trade_date;
      '''
    qur_rslt = pd.read_sql_query(sql, yconnect)
    max_trade_date = qur_rslt.loc[0,'cal_date']
    time_now =datetime.datetime.now().strftime('%Y%m%d')

    if max_trade_date <str(time_now):
        startdate=max_trade_date
        enddate=time_now
    elif max_trade_date == time_now:
        return True
    else:
        enddate=max_trade_date
    print(startdate,enddate)
    df = pro.trade_cal(start_date=startdate,end_date=enddate)
    if df is not None:
        try:
            pd.io.sql.to_sql(df, 'gp_trade_date', yconnect, schema='py_db', if_exists='append',index=df['cal_date'])
        except Exception as e:
            print(e)
            return False
        print("交易日更新完成")
        return True
    return False

# def get_EMA(cps, days):
#     emas = cps.copy()  # 创造一个和cps一样大小的集合
#     for i in range(len(cps)):
#         if i == 0:
#             emas[i] = cps[i]
#         if i > 0:
#             emas[i] = ((days - 1) * emas[i - 1] + 2 * cps[i]) / (days + 1)
#     return emas

def get_his_data():
    # df = pro.adj_factor(ts_code='002460.SZ', trade_date='20181016')
    df = pro.daily(trade_date='20181022')
    # 0  002460.SZ   20180528       6.228
    # 0  002460.SZ   20180529       9.398
    # df = tushare.pro_bar(pro_api=pro, ts_code='',adj=None,  start_date='20181022', end_date='20181024')
    # try:
    #     pd.io.sql.to_sql(df, 'gp_trade_daily_none', yconnect, schema='py_db', if_exists='append',
    #                  index=df[['ts_code', 'trade_date']])
    # except Exception as e:
    #     print(e)
    # tushare.pro
    #df = tushare.get_h_data(code='000001',autype='qfq',start='1999-01-01',end='1999-12-31',pause=1)
    # index = df.index
    # df['code']=pd.Series('000001',index=index)
    print("")
    print(df)
    # pd.io.sql.to_sql(df, 'gp_trade_daily_qfq_day', yconnect, schema='py_db', if_exists='append', index=df['date','code'])
    return True

print(tushare.__version__)
get_his_data()

# df_factor = pro.adj_factor(ts_code='', trade_date='20181022')
# print(len(df_factor))
#
# df = tushare.get_hist_data(code='000001')
# close=list()
# for x in df['close']:
#     close.append(x)
#
# dif = pd.Series(get_EMA(close,12))-pd.Series(get_EMA(close,26))
# print(dif)

# df_factor = pro.adj_factor(ts_code='000001.SZ', start_date='19000101',end_date='20181008')
# print(df_factor)


# df = tushare.get_h_data(code='000001',autype='qfq',start='1990-01-01',end='1992-12-31',pause=3)
# print("")
# print(df)

# start_date = datetime.datetime.strptime('19000101', '%Y%m%d')
# end_date = datetime.datetime.strptime('20000101', '%Y%m%d')
# print((end_date-start_date).days)
# if start_date-end_date > datetime.timedelta(1000) :
#     print("aaa")

# a = float("%.2f"%(69.03*6.228/9.398))
# print(a)