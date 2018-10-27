# -*- coding: utf-8 -*-
"""
-------------------------------------------------
 File Name：  down
 Description :
 Author :  amw
 date：   2018/10/8
-------------------------------------------------
 Change Activity:
     2018/10/8:
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
from  gp_scr.backend.getinfo import  *

ts_token='32bcbe7714fe65894202c39c6fa166b93d4c1d310d1e46da7f4a5a8e'
tushare.set_token(ts_token)
pro = tushare.pro_api()
yconnect = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/py_db?charset=utf8')
mysql_connect = pymysql.connect(host="127.0.0.1",user="root",password="123456",db="py_db",port=3306)


def get_cur_trade_date():
    '''
    获取数据库中最大交易日期
    :return:
    '''
    sql = '''
                select max(cal_date) as cal_date  from py_db.gp_trade_date;
              '''
    qur_rslt = pd.read_sql_query(sql, yconnect)
    if len(qur_rslt):
        max_trade_date = qur_rslt.loc[0, 'cal_date']
        return max_trade_date
    else:
        return None

def save_trade_cal():
    '''
    获取股票交易日信息，默认是上证所
    :return:
    '''
    enddate = ''
    startdate = '19900101'
    sql = '''
            select max(cal_date) as cal_date  from py_db.gp_trade_date;
          '''
    qur_rslt = pd.read_sql_query(sql, yconnect)

    max_trade_date = qur_rslt.loc[0, 'cal_date']
    print("max_trade_date=",max_trade_date)
    if max_trade_date is None:
        max_trade_date='19891231'
    time_now = datetime.datetime.now().strftime('%Y%m%d')

    if max_trade_date < str(time_now):
        tmpdate = datetime.datetime.strptime(max_trade_date,'%Y%m%d')+datetime.timedelta(days=1)
        startdate = tmpdate.strftime('%Y%m%d')
        print(startdate)
        enddate = time_now
    elif max_trade_date == time_now:
        print("无需更新")
        return True
    else:
        enddate = max_trade_date
    print(startdate, enddate)
    df = pro.trade_cal(start_date=startdate, end_date=enddate)
    print("获取",len(df),"条记录")
    if df is not None:
        try:
            pd.io.sql.to_sql(df, 'gp_trade_date', yconnect, schema='py_db', if_exists='append', index=df['cal_date'])
        except Exception as e:
            print(e)
            return False
        print("交易日更新完成")
        return True
    return False


def save_industry_classified():
    '''
    获取行业分类信息
    :return:
    '''
    cur=mysql_connect.cursor()
    try:
        sta = cur.execute("delete from py_db.gp_industry_classified")
        print(sta)
        print("删除行业分类gp_industry_classified:", sta, "条数据完成")
    except Exception as e:
        print(e)

    mysql_connect.commit()
    cur.close()
    df = tushare.get_industry_classified()
    try:
        pd.io.sql.to_sql(df, 'gp_industry_classified', yconnect, schema='py_db', if_exists='append', index=df['code'])
        print("行业分类",len(df),"更新完成")
        return len(df)
    except ValueError as e:
        print(e)
        return False
    return -1

def save_concept_classified():
    '''
    更新概念分类信息
    :return:
    '''
    cur=mysql_connect.cursor()
    try:
        sta = cur.execute("delete from py_db.gp_concept_classified")
        print(sta)
        print("删除概念分类gp_concept_classified:", sta, "条数据完成")
    except Exception as e:
        print(e)

    mysql_connect.commit()
    cur.close()
    df = tushare.get_concept_classified()
    try:
        pd.io.sql.to_sql(df, 'gp_concept_classified', yconnect, schema='py_db', if_exists='append', index=df['code'])
        print("概念分类",len(df),"更新完成")
        return len(df)
    except ValueError as e:
        print(e)
        return False
    return -1

def save_area_classified():
    '''
    获取地域分类信息
    :return:
    '''
    cur=mysql_connect.cursor()
    try:
        sta = cur.execute("delete from py_db.gp_area_classified")
        print(sta)
        print("删除地域分类gp_area_classified:", sta, "条数据完成")
    except Exception as e:
        print(e)

    mysql_connect.commit()
    cur.close()
    df = tushare.get_area_classified()
    try:
        pd.io.sql.to_sql(df, 'gp_area_classified', yconnect, schema='py_db', if_exists='append', index=df['code'])
        print("地域分类",len(df),"更新完成")
        return len(df)
    except ValueError as e:
        print(e)
        return False
    return -1


def save_gem_classified():
    '''
    更新创业板分类
    :return:
    '''
    cur=mysql_connect.cursor()
    try:
        sta = cur.execute("delete from py_db.gp_gem_classified")
        print(sta)
        print("删除创业板分类gp_gem_classified:", sta, "条数据完成")
    except Exception as e:
        print(e)

    mysql_connect.commit()
    cur.close()
    df = tushare.get_gem_classified()
    try:
        pd.io.sql.to_sql(df, 'gp_gem_classified', yconnect, schema='py_db', if_exists='append', index=df['code'])
        print("创业板分类",len(df),"更新完成")
        return len(df)
    except ValueError as e:
        print(e)
        return False
    return -1

def save_stock_list():

    '''
    获取股票列表信息
    :return:

    1.取交易日，判断股票列表信息是否已经存在
    2.如果存在，则返回成功
    3.如果不存在，则下载并插入

    '''
    stock_basic_fields = 'ts_code,symbol,name,fullname,enname,exchange_id,curr_type,list_status,list_date,delist_date,is_hs'
    sql = '''
      select count(1) as num from py_db.gp_stock_basic
      where data_dt =(select max(cal_date) as cal_date  from py_db.gp_trade_date);
     '''
    qur_rslt = pd.read_sql_query(sql, yconnect)
    if qur_rslt is None:
        return -1

    num = qur_rslt.loc[0, 'num']
    if num >0:
        print("共加载", num, "笔数据")
        return 0

    df = pro.stock_basic(fields=stock_basic_fields)
    if df is None:
        return -1

    trade_date = get_cur_trade_date()
    if trade_date is None:
        print("获取日期失败")
        return -1
    df['data_dt'] = trade_date
    print(trade_date)
    # print(df)
    try:
        pd.io.sql.to_sql(df, 'gp_stock_basic', yconnect, schema='py_db', if_exists='append',
                            index=df['ts_code'])
    except ValueError as e:
        print(e)
        return -1
    print("共加载",len(df),"笔数据")
    return len(df)


def get_data_info(path, code, name):
    data = {}
    if name[0:1]=="*":
        name = name[1:]
    file = os.path.join(path,"{}_{}.json".format(code, name))
    print("get {} into {}".format(name, file))
    kdata = tushare.get_hist_data(code,start='20130101')
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

def get_all_data_json():
    save_stock_list()
    with open("stocks.json") as f:
        data = json.load(f)
    allcnt = len(data)
    failed = []
    cnt = 0
    path = getcwd()
    print("path={}".format(path))
    # path = "{}/{}".format(path, "data")
    path = os.path.join(path, "data")
    print("path={}".format(path))

    pool = multiprocessing.Pool(processes=4)
    # rslt = server.list()
    for k, d in data.items():
        all = {}
        idx = 0
        print("get {} datas".format(cnt))

        pool.apply_async(get_data_info, (path, d["code"], d["name"]))
        # rslt.append()
        cnt += 1
        print("cnt=", cnt)



    pool.close()
    pool.join()
    print("完成下载")

    return None


def save_all_adj_factor():
    '''
    获取复权因子,按照股票代码一个个循环更新
    :return:
    '''
    df = get_gp_stock_basic()
    if len(df)==0:
        print('获取股票列表信息失败')
        return None
    err_flag = 0
    code_list = list()
    for i in range(len(df)):
        time.sleep(0.5)
        code_list.append(df.ix[i,'ts_code'])
        code_list_date=df.ix[i,'list_date']
        delist_date = str.strip(df.ix[i,'delist_date'])
        stock_date = df.ix[i,'data_dt']
        if len(delist_date) > 0 and delist_date<stock_date:
            continue
        # 删除已有数据
        cur = mysql_connect.cursor()
        try:
            sel_sql = '''
                select  a.ts_code, 
(case when a.max_date < b.cal_date then a.max_date else b.cal_date end) as start_date,
b.cal_date as end_date,
(case when a.max_date = b.cal_date  and b.cal_date is not null then '1' else '0' end) as is_up
 from 
(select ts_code,max(date_format(trade_date,'%s')) as max_date, min(trade_date) as min_date, count(1)
 from py_db.gp_adj_factor
 where ts_code='%s') A
left join (select max(cal_date) as cal_date from py_db.gp_trade_date
        where is_open ='1') b
on 1=1;
            '''%(r"%Y%m%d",code_list[i])
            #print(sel_sql)

            sta = cur.execute(sel_sql)
            data = cur.fetchone()
            # print(data)
            if data[0] is not None and data[3]=='1':
                continue
            end_date = data[2]

            if data[0] is None or len(data[0])==0:
                start_date = code_list_date
                print(code_list[i], start_date, end_date)
                del_sql = '''delete from py_db.gp_adj_factor where ts_code='%s'
                                    and trade_date >= '%s' and trade_date <= '%s'
                              ''' % (code_list[i], start_date, end_date)
            else:
                start_date= datetime.datetime.strptime(data[1],'%Y%m%d')+datetime.timedelta(days=1)
                # print(start_date)
                start_date = datetime.datetime.strftime(start_date,'%Y%m%d')
                print(code_list[i], start_date, end_date)
                del_sql = '''delete from py_db.gp_adj_factor where ts_code='%s'
                                                   and trade_date > '%s' and trade_date <= '%s'
                                             ''' % (code_list[i], start_date, end_date)




            # print(del_sql)
            sta = cur.execute(del_sql)
            print(sta)
            print("删除:", sta, "条数据完成")
        except Exception as e:
            print(e)
            print("更新",code_list[i],"复权因子失败")
            err_flag = 1
            break
        finally:
            mysql_connect.commit()
            cur.close()
        try:
            df_factor = pro.adj_factor(ts_code=code_list[i], start_date= start_date,end_date=end_date)
        except Exception as e:
            print(e)
            #插入到更新失败表中

        if len(df_factor)>0:
            print('获取',code_list[i],len(df_factor),'条复权因子')
            try:
                pd.io.sql.to_sql(df_factor, 'gp_adj_factor', yconnect, schema='py_db', if_exists='append',
                                 index=df_factor[['ts_code','trade_date']])
                print(code_list[i],"交易日更新完成")
            except Exception as e:
                print(e)
                continue

        else:
            continue
    if err_flag > 1:
        return False
    print('获取复权因子完成')
    return True

def save_all_adj_factor_by_day(data_dt):
    cnt = 0
    del_sql = '''
        delete from py_db.gp_adj_factor 
        where trade_date = str_to_date('%s','%s');
    '''%(data_dt,r'%Y%m%d')
    print(del_sql)
    cur = mysql_connect.cursor()
    try:
        print("开始连接查询信息")
        df_factor = pro.adj_factor(ts_code='', trade_date=data_dt)
        cnt = len(df_factor)
        print("返回",cnt,"条信息")
        if cnt > 0:
            sta = cur.execute(del_sql)
            print(sta)
            print("删除:", sta, "条数据完成")
            print(df_factor)
            mysql_connect.commit()
            pd.io.sql.to_sql(df_factor, 'gp_adj_factor', yconnect, schema='py_db', if_exists='append',
                         index=df_factor[['ts_code', 'trade_date']])
            mysql_connect.commit()
    except Exception as e:
        print(e)
        mysql_connect.rollback()
        cur.close()
        return -1
    finally:
        cur.close()
    print("更新",data_dt,"日复权因子",cnt,"条")
    return cnt

def save_stock_daily_none(code, start_date, end_date,flag):
    err_flag = 0
    print(code, start_date,end_date)
    cur = mysql_connect.cursor()
    if flag == 1:
        del_sql = '''delete from py_db.gp_trade_daily_none where ts_code='%s'
                                                    and trade_date >= '%s' and trade_date <= '%s'
                                              ''' % (code, start_date, end_date)
    else:
        del_sql = '''delete from py_db.gp_trade_daily_none where ts_code='%s'
                                                                   and trade_date > '%s' and trade_date <= '%s'
                                                             ''' % (code, start_date, end_date)
    try:
        sta = cur.execute(del_sql)
        # print(sta)
        if sta >0:
            print(code,"删除:", sta, "条数据完成")
    except Exception as e:
        print(e)
        print("更新", code, "不复权K线失败")
        mysql_connect.rollback()
        cur.close()
        insert_gp_trade_daily_none_err(code, start_date, end_date, str(e))
        err_flag = -1
    finally:
        mysql_connect.commit()
        cur.close()
        if err_flag == -1:
            return err_flag

    try:
        df_daily = tushare.pro_bar(pro_api=pro, ts_code=code, adj=None,
                                   start_date=start_date, end_date=end_date)
        if df_daily is None or len(df_daily)==0:
            print(code,"获取0条记录")
        if  df_daily is not None and len(df_daily) > 0:
            print('获取', code, len(df_daily), '条不复权K线')
            pd.io.sql.to_sql(df_daily, 'gp_trade_daily_none', yconnect, schema='py_db', if_exists='append',
                             index=df_daily[['ts_code', 'trade_date']])
    except Exception as e:
        print(e)
        insert_gp_trade_daily_none_err(code, start_date, end_date, str(e))
        err_flag = -1
        return err_flag
    return err_flag


def save_stock_daily_none_trade(trade_date,code =None):
    err_flag = 0
    print(trade_date)
    cur = mysql_connect.cursor()
    if code is None:
        del_sql = '''delete from py_db.gp_trade_daily_none 
            where trade_date = '%s' 
            ''' % (trade_date)
    else:
        del_sql = '''delete from py_db.gp_trade_daily_none 
          where ts_code='%s' and trade_date = '%s'
        ''' % (code, trade_date)
    print(del_sql)
    try:
        sta = cur.execute(del_sql)
        # print(sta)
        if sta >0:
            print("删除:", sta, "条数据完成")
    except Exception as e:
        print(e)
        print("更新", code, "不复权K线失败")
        mysql_connect.rollback()
        cur.close()
        insert_gp_trade_daily_none_err(code, trade_date, trade_date, str(e))
        err_flag = -1
    finally:
        mysql_connect.commit()
        cur.close()
        if err_flag == -1:
            return err_flag

    try:
        if code is None:
            df_daily = pro.daily(trade_date=trade_date)
        else:
            df_daily = pro.daily(trade_date=trade_date,ts_code =code)
        if df_daily is None or len(df_daily)==0:
            print(code,"获取0条记录")
        if  df_daily is not None and len(df_daily) > 0:
            print('获取', code, len(df_daily), '条不复权K线')
            pd.io.sql.to_sql(df_daily, 'gp_trade_daily_none', yconnect, schema='py_db', if_exists='append',
                             index=df_daily[['ts_code', 'trade_date']])
    except Exception as e:
        print(e)
        insert_gp_trade_daily_none_err(code, trade_date, trade_date, str(e))
        err_flag = -1
        return err_flag
    return err_flag

def save_stock_daily_none_all():
    '''
    获取日线K线交易
    :return:
    '''
    df_code = get_gp_stock_basic()
    if len(df_code) == 0:
        print('获取股票列表信息失败')
        return None

    code_list = [str(x) for x in df_code['ts_code']]
    code_list_date = [str(x) for x in df_code['list_date']]
    code_list_statues = [str(x) for x in df_code['list_status']]

    # print(len(code_list), len(code_list_date),len(code_list_statues))
    pool = multiprocessing.Pool(processes=6)
    for i in range(len(code_list)):
        # time.sleep(0.5)
        err_flag = 0
        # code_list.append(df_code.ix[i,'ts_code'])
        code_date=code_list_date[i]

        if str(code_list_statues[i]) =='D':
            print(code_list[i], "退市 跳过")
            continue

        # 删除已有数据

        cur = mysql_connect.cursor()
        try:
            sel_sql = '''
                        select  a.ts_code,
        (case when a.max_date < b.cal_date then a.max_date else b.cal_date end) as start_date,
        b.cal_date as end_date,
        (case when a.max_date = b.cal_date  and b.cal_date is not null then '1' else '0' end) as is_up
         from
        (select ts_code,max(date_format(trade_date,'%s')) as max_date, min(trade_date) as min_date, count(1)
         from py_db.gp_trade_daily_none
         where ts_code='%s') A
        left join (select max(cal_date) as cal_date from py_db.gp_trade_date
                where is_open ='1') b
        on 1=1;
                    ''' % (r"%Y%m%d", code_list[i])
            sta = cur.execute(sel_sql)
            data = cur.fetchone()
            if data[0] is not None and data[3] == '1':
                print(code_list[i],"跳过")
                continue
            end_date = data[2]

            if data[1] == data[2]:
                print(code_list[i], "跳过")
                continue

            if data[0] is None or len(data[0]) == 0:
                start_date = code_date
                end_date=datetime.datetime.strptime(start_date, '%Y%m%d') + datetime.timedelta(days=1500)
                end_date = datetime.datetime.strftime(end_date, '%Y%m%d')
                # print(code_list[i], start_date, end_date)
                pool.apply_async(save_stock_daily_none,(code_list[i], start_date, end_date, 1))
                # err_flag=save_stock_daily_none(code_list[i], start_date, end_date, 1)
            else:
                start_date = datetime.datetime.strptime(data[1], '%Y%m%d') + datetime.timedelta(days=1)
                # print(start_date,"|",end_date)
                end_date = datetime.datetime.strptime(end_date, '%Y%m%d')
                if (end_date-start_date).days > 1500:
                    end_date = start_date + datetime.timedelta(days=1500)
                start_date = datetime.datetime.strftime(start_date, '%Y%m%d')
                end_date = datetime.datetime.strftime(end_date, '%Y%m%d')
                # print(code_list[i], start_date, end_date)
                pool.apply_async(save_stock_daily_none, (code_list[i], start_date, end_date, 0))
                # err_flag=save_stock_daily_none(code_list[i], start_date, end_date, 0)

            if err_flag == -1:
                continue

        except Exception as e:
            print(e)
            print("更新", code_list[i], "不复权K线失败")
            cur.close()
            insert_gp_trade_daily_none_err(code_list[i],start_date,end_date,str(e))
            continue
    pool.close()
    pool.join()
    return True




if __name__ == "__main__":

    server = multiprocessing.Manager()
    date_str = datetime.datetime.now()

    # print("--------------更新股市日期--------------")
    # save_trade_cal()
    #
    # print("--------------更新股票列表信息--------------")
    # save_stock_list()
    #
    # print("--------------更新板块信息--------------")
    # save_industry_classified()
    # save_concept_classified()
    # save_area_classified()
    # save_gem_classified()

    # save_all_adj_factor_by_day(data_dt='20181026')
    # save_stock_daily_none_trade(trade_date='20181026')
    #
    # save_stock_daily_none_all()
    #save_all_adj_factor_by_day('20181024')
    # save_stock_daily_none('600705.SH','20120101','20181022',1)


    date_end = datetime.datetime.now()
    all_time = (date_end - date_str).seconds
    print("总共花费",all_time,"秒")



