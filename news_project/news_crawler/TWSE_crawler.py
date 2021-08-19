# -*- coding: utf-8 -*-
"""

Last Modified：08/13

@author: Denver Liu

"""

import pandas as pd
import json
import requests
import traceback
from datetime import datetime,timedelta
import time as t
import os
import sys
sys.path.append(r'..')
import log_manager as log

#程序一：證交所爬蟲 (停券預告表)
def twse_future():
    try:
        params = {'response':'csv'}
        r = requests.get('https://www.twse.com.tw/exchangeReport/BFI84U',params=params)
        connect_count = 0
        while r.status_code != 200:
            t.sleep(5)
            connect_count += 1
            r = requests.get('https://www.twse.com.tw/exchangeReport/BFI84U',params=params)
            if connect_count > 10:
                print('[斷線] 證交所停券預告表 連線十次皆失敗')
                break
        r.content.decode('big5')

        fileName = 'twse_future.csv'
        with open(fileName,'wb+') as f:
            f.write(r.content)

        df = pd.read_csv(fileName,encoding='ms950',header=1)
        df = df.iloc[:-4,:-1] #排除footer
        df['股票代號'] = df['股票代號'].astype(str).str.replace('=|"','')
        df.columns = ['股票代號', '股票名稱', '停券起日', '停券迄日', '原因']
        df['發言日期'] = ''
        df['REASON'] = ''
        df['主旨'] = '此為證交所停券預告表直接匯入之資料'
        df['停券起日'].fillna('',inplace=True)
        df['停券迄日'].fillna('',inplace=True)

        #處理日期資訊
        allStart,allEnd = [],[]
        for row in df.iterrows():
            row = row[1]
            tStart = row[2]
            tEnd = row[3]
            if tStart != '':
                tStart = str(int(tStart[:tStart.find('.')])+1911) + tStart[tStart.find('.'):]
                tStart = tStart.strip()
                tStart = datetime.strftime(datetime.strptime(tStart,'%Y.%m.%d'),'%Y/%m/%d')
            if tEnd != '':
                tEnd = str(int(tEnd[:tEnd.find('.')])+1911) + tEnd[tEnd.find('.'):]
                tEnd = tEnd.strip()
                tEnd = datetime.strftime(datetime.strptime(tEnd,'%Y.%m.%d'),'%Y/%m/%d')
            allStart.append(tStart)
            allEnd.append(tEnd)

        df['停券起日'] = allStart
        df['停券迄日'] = allEnd

        return df
    except:
        log.processLog(f'[TWSE_crawler] 子程序(twse_future)發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())


#程序二：證交所爬蟲 (停券歷史查詢)
def twse_history(startDate:str,endDate:str) -> pd.DataFrame:

    try:
        params = {'response':'json','startDate':startDate,'endDate':endDate,'stockNo':''}
        r = requests.get('https://www.twse.com.tw/exchangeReport/BFI84U2',params=params)
        connect_count = 0
        while r.status_code != 200:
            t.sleep(5)
            connect_count += 1
            r = requests.get('https://www.twse.com.tw/exchangeReport/BFI84U2',params=params)
            if connect_count > 10:
                break
        resp = r.content.decode('utf-8')
        d = json.loads(resp)

        allCode,allName,allStart,allEnd,allReason = [],[],[],[],[]
        for row in d['data']:
            tCode = row[0]
            tName = row[1]
            tStart= row[2]
            if tStart != '':
                tStart = str(int(tStart[:tStart.find('.')])+1911) + tStart[tStart.find('.'):]
                tStart = tStart.strip().replace('.','/')
                tStart = datetime.strftime(datetime.strptime(tStart,'%Y/%m/%d'),'%Y/%m/%d')
            tEnd = row[3]
            if tEnd != '':
                tEnd = str(int(tEnd[:tEnd.find('.')])+1911) + tEnd[tEnd.find('.'):]
                tEnd = tEnd.strip().replace('.','/')
                tEnd = datetime.strftime(datetime.strptime(tEnd,'%Y/%m/%d'),'%Y/%m/%d')
            tReason = row[4]
            allCode.append(tCode.strip())
            allName.append(tName.strip())
            allStart.append(tStart.strip().replace('.','/'))
            allEnd.append(tEnd.strip().replace('.','/'))
            allReason.append(tReason.strip())

        new = dict(zip(d['fields'],[allCode,allName,allStart,allEnd,allReason]))
        df = pd.DataFrame(new)
        df['發言日期'] = ''
        df['REASON'] = ''
        df['主旨'] = '此為證交所停券歷史公告匯入之資料'
        df.columns = ['股票代號', '股票名稱', '停券起日', '停券迄日', '原因', '發言日期', 'REASON', '主旨']
        return(df)
    except:
        log.processLog(f'[TWSE_crawler] 子程序(twse_history)發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())


## 程序三：櫃買中心爬蟲 (停券預告表) 
def tpex(startDate) -> pd.DataFrame:
    try:
        start = f"{int(startDate[:4]) - 1911}/{startDate[4:6]}/{startDate[6:]}"
        params = {'l':'zh-tw','sd':start,"ed":""}
        url2 = "https://www.tpex.org.tw/web/stock/margin_trading/term/term_result.php"
        r = requests.get(url2,params=params)
        connect_count = 0
        while r.status_code != 200:
            t.sleep(5)
            connect_count += 1
            r = requests.get(url2,params=params)
            if connect_count > 10:
                break
        resp = r.content.decode('utf-8')
        d = json.loads(resp)

        allCode,allName,allStart,allEnd,allReason = [],[],[],[],[]
        for row in d['aaData']:
            tCode = row[0]
            tName = row[1]
            tStart = row[2]
            if tStart != '':
                tStart = str(int(tStart[:tStart.find('/')])+1911) + tStart[tStart.find('/'):]
                tStart = tStart.strip()
                tStart = datetime.strftime(datetime.strptime(tStart,'%Y/%m/%d'),'%Y/%m/%d')
            tEnd = row[3]
            if tEnd != '':
                tEnd = str(int(tEnd[:tEnd.find('/')])+1911) + tEnd[tEnd.find('/'):]
                tEnd = tEnd.strip()
                tEnd = datetime.strftime(datetime.strptime(tEnd,'%Y/%m/%d'),'%Y/%m/%d')
            tReason = row[4]
            allCode.append(tCode.strip())
            allName.append(tName.strip())
            allStart.append(tStart.strip())
            allEnd.append(tEnd.strip())
            allReason.append(tReason.strip())
            
        new = dict(zip(['股票代號', '股票名稱', '停券起日(最後回補日)', '停券迄日', '原因'],[allCode,allName,allStart,allEnd,allReason]))
        df = pd.DataFrame(new)
        df['發言日期'] = ''
        df['REASON'] = ''
        df['主旨'] = '此為櫃買中心停券預告表匯入之資料'
        df.columns = ['股票代號', '股票名稱', '停券起日', '停券迄日', '原因', '發言日期', 'REASON', '主旨']
        return(df)
    except:
        log.processLog(f'[TWSE_crawler] 子程序(tpex)發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())

if __name__== '__main__':
    try:

        #路徑設定 (需手動更新的地方)
        #只要決定"根路徑"即可
        output_dir_path_dict = json.load(open(r'../set.json','r+'))
        output_dir_path = '../' + output_dir_path_dict['output_dir_path']
        output_file_path_dataCsv = os.path.join(output_dir_path,'News_Stocks','print','data.csv')

        log.processLog('【停券預告表爬蟲補充程序】 TWSE_crawler.py')
        start = t.perf_counter_ns() 

        holidayList = pd.read_csv('../predata/holiday.csv')['date'].apply(lambda x: t.strftime('%Y/%m/%d',t.strptime(x,'%Y/%m/%d'))).tolist()
        workdayList = pd.read_csv('../predata/workday.csv')['date'].apply(lambda x: t.strftime('%Y/%m/%d',t.strptime(x,'%Y/%m/%d'))).tolist()


        startDate = '20210101'
        endDate = datetime.strftime(datetime.now()-timedelta(days=1),'%Y%m%d')
        key1 = ['stat', 'title', 'fields', 'params', 'data', 'notes']
        key2 = ['STK_CODE', 'reportDateStart', 'reportDateEnd', 'iTotalRecords', 'aaData']
        df_twse_future= twse_future() #證交所-停券預告表
        df_twse_history = twse_history(startDate, endDate) #證交所 - 停券歷史查詢
        df_tpex = tpex(startDate) #櫃買中心 - 停券預告表
        df = pd.concat([df_twse_future,df_twse_history,df_tpex],axis=0)
        df.sort_values('停券起日',ignore_index=True,inplace=True)
        df = df.drop_duplicates(['股票代號', '股票名稱', '停券起日', '停券迄日','原因'])
        for i in range(len(df)):
            if ( (df.iloc[i,3] == '') & (df.iloc[i,2]=='') ):
                continue
            elif ( (df.iloc[i,3] == '') & (df.iloc[i,2]!='') ):
                time_stamp = int(t.mktime(t.strptime(df.iloc[i,2], '%Y/%m/%d')))
                count = 0
                while count < 3:
                    time_stamp += 86400
                    cache = t.strftime('%Y/%m/%d', t.localtime(time_stamp))
                    if str(cache) in holidayList:
                        pass
                    elif str(cache) in workdayList:
                        count += 1
                    elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
                        pass
                    else:
                        count += 1
                cache_list = cache.split('/')
                end_date = str(cache_list[0]) + '/' + cache_list[1] + '/' + cache_list[2]
                df.iloc[i,3] = end_date
            elif ( (df.iloc[i,3] != '') & (df.iloc[i,2]=='') ):
                time_stamp = int(t.mktime(t.strptime(df.iloc[i,3], '%Y/%m/%d')))
                count = 0
                while count < 3:
                    time_stamp -= 86400
                    cache = t.strftime('%Y/%m/%d', t.localtime(time_stamp))
                    if str(cache) in holidayList:
                        pass
                    elif str(cache) in workdayList:
                        count += 1
                    elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
                        pass
                    else:
                        count += 1
                cache_list = cache.split('/')
                end_date = str(cache_list[0]) + '/' + cache_list[1] + '/' + cache_list[2]
                df.iloc[i,2] = end_date
            else:
                continue
        df_origin = pd.read_csv(output_file_path_dataCsv)
        df_origin['股票代號'] = df_origin['股票代號'].astype(str)
        df_origin['停券起日'] = [ datetime.strftime(datetime.strptime(i,'%Y/%m/%d'),'%Y/%m/%d') for i in df_origin['停券起日'].values] 
        df_origin['停券迄日'] = [ datetime.strftime(datetime.strptime(i,'%Y/%m/%d'),'%Y/%m/%d') for i in df_origin['停券迄日'].values] 
        for row in df.iterrows():
            row = row[1]
            checkLen = len((df_origin[(df_origin['股票代號']==row[0])  & (df_origin['停券起日']==row[2]) & (df_origin['停券迄日']==row[3])]))
            if checkLen == 0:
                df_origin = df_origin.append(row)
                print(row[0],row[1],row[2],row[3],row[4]) #檢查時再用這行
                log.processLog(f'補充 {row[0]},{row[1]},{row[2]},{row[3]},{row[4]} 於data.csv')
            else:
                continue
        df_origin.to_csv(output_file_path_dataCsv,encoding='utf-8-sig',index=False)

        end = t.perf_counter_ns() 
        log.processLog(f'【結束程序】 TWSE_crawler.py - 執行時間:{(end-start)/10**9}')
        
        log.processLog('==============================================================================================')

    except:
        log.processLog(f'[TWSE_crawler] 主程序發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())
            
