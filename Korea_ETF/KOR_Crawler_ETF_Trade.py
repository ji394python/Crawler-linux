import requests
import pandas as pd
from datetime import datetime
import sys
sys.path.append('pre') 
import log_manager as log
import json
import traceback
import time
import os
import time 

def GetPermitCode() -> str:
        url = 'https://global.krx.co.kr/contents/COM/GenerateOTP.jspx'
        header = {'referer':'https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'x-requested-with': 'XMLHttpRequest'}
        # params = {'name': 'fileDown','filetype': 'csv',
        #         'url':'GLB/05/0503/0503010400/glb0503010400_01',
        #         'isu_cdnm': 'A006400/SAMSUNG SDI CO.,LTD.',
        #         'isu_cd': 'KR7006400006',
        #         'isu_srt_cd': 'A006400',
        #         'isu_nm': '삼성SDI',
        #         'pagePath': '/contents/GLB/05/0503/0503010400/GLB0503010400.jsp'}
        params = {'bld':'GLB/05/0507/0507010104/glb0507010104_02','name':'grid'}

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

def GetFile(permitCode:str,etfCode:str) -> requests.Response:

        url2 = 'https://global.krx.co.kr/contents/GLB/99/GLB99000001.jspx'
        header = {'referer':'https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp',
                'origin':'https://global.krx.co.kr',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'x-requested-with': 'XMLHttpRequest'}
        data = {'isu_cd':etfCode,
                'gubun': '00',
                'acsString': '1',
                'pagePath':'/contents/GLB/05/0507/0507010104/GLB0507010104.jsp',
                'code':permitCode
        }
        new=requests.request('POST',url2,headers=header,data=data)
        connect_count = 0
        while new.status_code != 200:
                time.sleep(5)
                connect_count += 1
                new=requests.request('POST',url2,headers=header,data=data)
                if connect_count > 10:
                        break

        return new.text

if __name__ == '__main__':
        try:    


                log.processLog('==============================================================================================')
                log.processLog(f'【開始執行韓國ETF日內交易價爬蟲專案】 {os.path.basename(__file__)}')
                
                #計時開始
                start = time.perf_counter_ns()

                #日期設定
                today = datetime.now().strftime('%Y%m%d') 

                #路徑設定 (需手動更新的地方)
                #只要決定"根路徑"即可
                output_dir_path_dict  = json.load(open('pre/set.json','r'))
                
                #路徑設定 (不用更動這裡)
                output_dir_path = f"{output_dir_path_dict['output_dir_path']}/KRX/ETF/Trade"
                
                if not os.path.exists(output_dir_path):
                        log.processLog(f'建立根資料夾：{output_dir_path}')
                        os.makedirs(output_dir_path)

                output_dir_date_path = f"{output_dir_path}/{today}"
                if not os.path.exists(output_dir_date_path):
                        log.processLog(f'建立日期資料夾：{output_dir_date_path}')
                        os.makedirs(output_dir_date_path)

                #讀取前置檔案
                log.processLog(f'讀取前置檔案：pre/etf_code.csv')
                codeEtf = pd.read_csv('pre/etf_code.csv',encoding='utf-8-sig')
                csvHead = ['Time','Price','Change','iNAV','Underlying Index','Trading Volume (share)','Trading Volume at the time (share)']
                
                count = 0
                rowCount = 0
                length = len(codeEtf)
                
                log.processLog(f'開始爬取：韓國ETF日內交易價格網 - https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp')
                log.processLog(f'=== 本次查詢日期：${today}')
                for row in codeEtf.iterrows():
                        rowCount += 1
                        row = row[1]
                        fileName = f"{output_dir_date_path}/{row['Stock Abbrv Code']}_Trades_{today}.csv"
                        permitCode = GetPermitCode()
                        log.processLog(f"===== [{rowCount}]_[取認證碼]：{fileName}")
                        resp = GetFile(permitCode,row['isu_cd'])
                        log.processLog(f"===== [{rowCount}]_[檔案下載]：{fileName}")
                        respJson = json.loads(resp)
                        if len(respJson['block1']) == 0: 
                                count += 1
                                if count >= 10:
                                        log.processLog(f'===== [{rowCount}]_[檔案無值]：此為本日出現第({count})筆無任何資料')
                                        log.processLog(f'------------------------------------------------------')
                                        log.processLog(f'【程序中止】 因遇十筆無資料，判斷{today}為韓國停市日，略過本日')
                                        break
                                else:
                                        log.processLog(f'===== [{rowCount}]_[檔案無值]：此為本日出現第({count})筆無任何資料')
                        else:
                                df = pd.DataFrame(respJson['block1'])
                                df.drop('fluc_tp_cd',axis=1,inplace=True)
                                df.columns = csvHead
                                df.to_csv(f'{fileName}',encoding='utf-8-sig',index=False)
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


