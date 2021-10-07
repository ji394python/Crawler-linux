import requests
import pandas as pd
from datetime import datetime,timedelta
import sys
sys.path.append('pre') 
import log_manager as log
import json
import csv
import traceback
import time
import argparse
from argparse import RawTextHelpFormatter
import os
import re

def GetPermitCode_Down(stockFullCode:str,stockName:str,stockCd:str) -> str:
        url = 'https://global.krx.co.kr/contents/COM/GenerateOTP.jspx'
        header = {'referer':'https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'x-requested-with': 'XMLHttpRequest'}
        params = {'name':'fileDown',
        'filetype':'csv',
        'url':'GLB/05/0503/0503010400/glb0503010400_01',
        'isu_cdnm':stockName,
        'isu_cd':stockFullCode,
        'isu_nm':stockName[stockName.find('/')+1:],
        'isu_srt_cd':stockCd,
        'pagePath':'/contents/GLB/05/0503/0503010400/GLB0503010400.jsp'}
        #params = {'bld':'GLB/05/0503/0503010400/glb0503010400_01','name':'form'}

        r = requests.get(url,headers=header,params=params)
        connect_count = 0
        while r.status_code != 200:
                time.sleep(5)
                connect_count += 1
                r = requests.get(url,headers=header,params=params)
                if connect_count > 10:
                        break
        permitCode = r.text
        return permitCode

def GetFile_Down(permitCode:str) -> requests.Response:

        url2 = 'https://file.krx.co.kr/download.jspx'
        header = {'referer':'https://global.krx.co.kr/',
                'origin':'https://global.krx.co.kr',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'x-requested-with': 'XMLHttpRequest'}
        new=requests.request('POST',url2,headers=header,data={"code":permitCode})
        connect_count = 0
        while new.status_code != 200:
                time.sleep(5)
                connect_count += 1
                new=requests.request('POST',url2,headers=header,data={"code":permitCode})
                if connect_count > 10:
                        break

        return new.text

def WriteFile_Down(content:str,fileName:str,csvHead:list,encode:str) -> None:
        '''
                寫入JsonData
        '''
        csvRows = [re.findall('"(.*?)"',row) for row in content.split('\n')[1:]]
        df = pd.DataFrame(csvRows,columns=csvHead)
        df.sort_values('Time',inplace=True)
        df.to_csv(fileName,encoding=encode,index=False)
    
        return '200' 




if __name__ == '__main__':

        try:

                #計時開始
                start = time.perf_counter_ns()

                #日期設定
                today = datetime.now().strftime('%Y%m%d') 

                log.processLog('==============================================================================================')
                log.processLog(f'【開始執行韓國Stock日內交易價爬蟲專案】 {os.path.basename(__file__)}')
                
                #路徑設定 (需手動更新的地方)
                #只要決定"根路徑"即可
                output_dir_path_dict  = json.load(open('pre/set.json','r'))
                
                #路徑設定 (不用更動這裡)
                output_dir_path = f"{output_dir_path_dict['output_dir_path']}/KRX/Stock"
                
                if not os.path.exists(output_dir_path):
                        log.processLog(f'建立根資料夾：{output_dir_path}')
                        os.makedirs(output_dir_path)

                output_dir_date_path = f"{output_dir_path}/{today}"
                if not os.path.exists(output_dir_date_path):
                        log.processLog(f'建立日期資料夾：{output_dir_date_path}')
                        os.makedirs(output_dir_date_path)

                #讀取前置檔案
                log.processLog(f'讀取前置檔案：pre/KR_stk_list.csv')
                codeStock = pd.read_csv('pre/KR_stk_list.csv',encoding='utf-8-sig',dtype={'Stock Abbrv Code': 'str'})
                csvHead = ['Time','Price','Change','Trading Valume (shr.)','Trading Value (KRW)']

                count = 0
                rowCount = 0
                length = len(codeStock)

                for row in codeStock.iterrows():
                        rowCount += 1
                        row = row[1]
                        requestCode = row['Stock Full Code']
                        requestAbbrvCode = row['Stock Abbrv Code']
                        requestAbbrvName = row['Stock Abbrv']

                        fileName = f"{output_dir_date_path}/{requestAbbrvCode}_Trades_{today}.csv"
                        
                        permitCode = GetPermitCode_Down(requestCode,f"A{requestAbbrvCode}/{requestAbbrvName}",f"A{requestAbbrvCode}")
                        log.processLog(f"===== [{rowCount}]_[取認證碼]：{fileName}")

                        resp = GetFile_Down(permitCode)
                        log.processLog(f"===== [{rowCount}]_[檔案下載]：{fileName}")

                        WriteFile_Down(resp,fileName,csvHead,'utf-8-sig')
                        log.processLog(f'===== [{rowCount}]_[檔案寫入]：{fileName}')
                        log.processLog(f'------------------------------------------------------')


                end = time.perf_counter_ns()
                log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)/10**9}')
                log.processLog('==============================================================================================')
        
        except:
                end = time.perf_counter_ns()
                log.processLog(f'【程序錯誤】 {os.path.basename(__file__)} - 執行時間:{(end-start)/10**9}')
                log.processLog(f'【程序錯誤】：本次運行完成{rowCount-1}筆目標,剩餘{length-rowCount}筆未爬')
                log.processLog(f'【程序錯誤】：錯誤詳情請查看errorlog')
                log.processLog('==============================================================================================')

                log.errorLog(traceback.format_exc())