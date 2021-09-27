import requests
import pandas as pd
from datetime import datetime,timedelta
import sys
sys.path.append('pre') 
import log_manager as log
import json
import os
import traceback
import time as t
import argparse
from argparse import RawTextHelpFormatter


def GetPermitCode(etfCode:str,startDate:str) -> str:
    '''
        取得下載授權碼
    '''
        
    url = "https://global.krx.co.kr/contents/COM/GenerateOTP.jspx"

    params = {
        "name":"fileDown",
        "filetype":"csv",
        "url":"GLB/05/0507/0507010302/glb0507010302",
        "gubun":"00",
        "isu_cd":etfCode,
        "pagePath":"/contents/GLB/05/0507/0507010104/GLB0507010104.jsp",
        "trd_dd":startDate,
    }

    headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
        'x-requested-with': 'XMLHttpRequest',
        'referer': 'https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp'
    }

    r = requests.request("GET",url,params=params,headers=headers)
    connect_count = 0
    while r.status_code != 200:
        t.sleep(5)
        connect_count += 1
        r = requests.request("GET",url,params=params,headers=headers)
        if connect_count > 10:
            break
    permitCode = r.text
    return permitCode

def GetFile(permitCode:str) -> requests.Response:
    '''
        取得下載檔案
    '''

    data = {'code':permitCode}
    header = {
        'origin': 'https://global.krx.co.kr',
        'referer': 'https://global.krx.co.kr/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
        'content-type': 'application/x-www-form-urlencoded'
    }
    r = requests.request('POST','https://file.krx.co.kr/download.jspx',data=data,headers=header)
    connect_count = 0
    while r.status_code != 200:
        t.sleep(5)
        connect_count += 1
        r = requests.request('POST','https://file.krx.co.kr/download.jspx',data=data,headers=header)
        if connect_count > 10:
            break
    return r 
    
def WriteFile(content:str,fileType:str,filePathName:str) -> None:
    '''
        寫入binaryData
    '''

    with open(f'{filePathName}.{fileType}','wb+') as f:
        f.write(content)
        f.close()
    
    return 


if __name__ == '__main__':
    try:
        #參數呼叫設定
        endDate = datetime.strftime(datetime.now() - timedelta(days=1),'%Y%m%d')
        parser = argparse.ArgumentParser(description='目標：下載韓國ETF成分表 \
            \n網址：https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.js\
            \nOptional you can choose crawler date.\
            \nDefault crawler date is your execute date - 1 \
            \nExamples: python3 CB_News_Crawler.py --d 2021/06/30 ', formatter_class=RawTextHelpFormatter)

        parser.add_argument('-d', '--date', action='store', dest='date', type=str,
                            help='enter endDate: YYYY/mm/dd', default=endDate)

        args = parser.parse_args()
        args.date = args.date.replace('/','')
        endDate = args.date


        log.processLog('==============================================================================================')
        log.processLog('【開始執行韓國ETF成份表爬蟲專案】 KOR_Crawler_PCF.py')
        
        #計時開始
        start = t.perf_counter_ns() 

        #路徑設定 (需手動更新的地方)
        #只要決定"根路徑"即可
        output_dir_path_dict  = json.load(open('pre/set.json','r'))
        
        #路徑設定 (不用更動這裡)
        output_dir_path = f"{output_dir_path_dict['output_dir_path']}/PCF_Data"
        
        if not os.path.exists(output_dir_path):
            log.processLog(f'建立根資料夾：{output_dir_path}')
            os.makedirs(output_dir_path)

        output_dir_date_path = f"{output_dir_path}/{endDate}"
        if not os.path.exists(output_dir_date_path):
            log.processLog(f'建立日期資料夾：{output_dir_date_path}')
            os.makedirs(output_dir_date_path)
        
        log.processLog(f'讀取前置檔案：pre/etf_code.csv')
        codeEtf = pd.read_csv('pre/etf_code.csv',encoding='utf-8-sig')

        count = 0
        rowCount = 0
        length = len(codeEtf)
        fileType = 'csv'
        
        log.processLog(f'開始爬取：韓國ETF成分網 - https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.jsp')
        log.processLog(f'=== 本次查詢日期：${endDate}')
        for row in codeEtf.iterrows():
            rowCount += 1
            row = row[1]
            permitCode = GetPermitCode(row['isu_cd'],endDate)
            fileName = row['isu_cd'].replace(' ','_')
            filePathName = f"{output_dir_date_path}/{fileName}_PCF_{endDate}"
            log.processLog(f"===== [{rowCount}]_[取認證碼]：{fileName}")
            r = GetFile(permitCode)
            log.processLog(f"===== [{rowCount}]_[檔案下載]：{fileName}")
            if r.text == 'Name,No of Shares,Par Value,Amount,Weight(%)':
                count += 1
                if count >= 10:
                    log.processLog(f'===== [{rowCount}]_[檔案無值]：此為本日出現第({count})筆無任何資料')
                    log.processLog(f'------------------------------------------------------')
                    log.processLog(f'【程序中止】 因遇十筆無資料，判斷{endDate}為韓國停市日，略過本日')
                    break
                else:
                    log.processLog(f'===== [{rowCount}]_[檔案無值]：此為本日出現第({count})筆無任何資料')
            else:
                WriteFile(r.content,fileType,filePathName)
                log.processLog(f'===== [{rowCount}]_[檔案寫入]：{filePathName}.{fileType}')
            log.processLog(f'------------------------------------------------------')
            
        #計時結束
        end = t.perf_counter_ns() 

        log.processLog(f'【結束程序】 KOR_Crawler_PCF.py - 執行時間:{(end-start)/10**9}')
        log.processLog('==============================================================================================')

    except:

        log.processLog(f'【程序錯誤】：本次運行完成{rowCount-1}筆目標,剩餘{length-rowCount}筆未爬')
        log.processLog(f'【程序錯誤】：錯誤詳情請查看errorlog')
        log.processLog('==============================================================================================')

        log.errorLog(traceback.format_exc())
    
