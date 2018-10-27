# -*- coding: utf-8 -*-
"""
-------------------------------------------------
 File Name：  getinfo
 Description :
 Author :  amw
 date：   2018/10/16
-------------------------------------------------
 Change Activity:
     2018/10/16:
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

def get_gp_stock_basic():

    strsql ='''
        select * from py_db.gp_stock_basic
        where data_dt = (select max(data_dt) from py_db.gp_stock_basic)   
        order by ts_code
    '''
    #and substr(ts_code,1,6)>='603203'
    df = pd.read_sql_query(strsql,yconnect)
    return df

def get_max_cal_date():
    '''

    :return:
    '''
    str_sql = '''
        select max(cal_date) from py_db.gp_trade_date
        where is_open ='1'
    '''
    print(str_sql)
    #打开游标
    cur = mysql_connect.cursor()
    try:
        sta = cur.execute(str_sql)
        print(sta)
        print("获取最大开盘日期:", sta, "条数据完成")
        data = cur.fetchone()
    except Exception as e:
        print(e)
    finally:
        mysql_connect.commit()
        cur.close()
    if len(data)>0:
        return str(data[0])
    return None;


def get_stock_daily_qfq(code,adj='qfq'):
    params={'code':code,}
    df = pd.read_sql_query("select a.*,b.adj_factor from py_db.gp_trade_daily_none a inner join py_db.gp_adj_factor b \
             on a.ts_code = b.ts_code \
             and a.trade_date = b.trade_date \
             where a.ts_code = %(code)s \
            order by a.trade_date desc;", yconnect,params=params)
    # and a.trade_date <= str_to_date(%(dates)s,%(formats)s) \
    #          'formats':r'%Y%m%d'
    #qfq因子
    if len(df)==0:
        print("........")
        return None
    qfq_yz = df.ix[0, 'adj_factor']
    df['trade_date'] = df.apply(lambda x: datetime.datetime.strftime(x['trade_date'], '%Y%m%d'), axis=1)
    if adj=='qfq':
        df['open'] = df.apply(lambda x :float("%.2f"%(x['open']*x['adj_factor']/ qfq_yz)),axis=1)
        df['high'] = df.apply(lambda x: float("%.2f"%(x['high']*x['adj_factor']/ qfq_yz)),axis=1)
        df['low'] = df.apply(lambda x: float("%.2f"%(x['low']*x['adj_factor']/ qfq_yz)),axis=1)
        df['close'] = df.apply(lambda x: float("%.2f"%(x['close']*x['adj_factor']/ qfq_yz )),axis=1)
        df['pre_close'] = df.apply(lambda x: float("%.2f" % (x['pre_close'] * x['adj_factor'] / qfq_yz)), axis=1)

    return df


def insert_gp_trade_daily_none_err(code,start_date, end_date, err_msg):
    cnt = -1
    if err_msg is None:
        err_msg=''
    # err_msg = err_msg.replace("'","\'")
    # err_msg = err_msg.replace('"', "\"")
    insert_sql = '''
        insert into py_db.gp_trade_daily_none_err values ('%s','%s','%s','%s')
    '''%(code,start_date,end_date,err_msg)
    print(insert_sql)
    cur = mysql_connect.cursor()
    try:
        cnt = cur.execute(insert_sql)
        print("插入错误信息成功")
    except Exception as e:
        print(e)
        mysql_connect.rollback()
        cur.close()
        return -1

    return cnt

# if __name__ == "__main__":
#     print()

# df =get_gp_stock_basic();
# print(df)

# print(get_max_cal_date())
# df = get_stock_daily_qfq('002460.SZ',adj=None)

# size = len(df)
# for i in range(size-1):
#     if i ==0 :
#
#     else:


# print(df[df['trade_date']<'20180530'])