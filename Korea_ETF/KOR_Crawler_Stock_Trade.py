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
        while ( (new.status_code != 200) or (new.text == '')):
                time.sleep(5)
                connect_count += 1
                new=requests.request('POST',url2,headers=header,data={"code":permitCode})
                if connect_count > 5:
                        break

        return new.text

def WriteFile_Down(content:str,fileName:str,csvHead:list,encode:str) -> str:
        '''
                ??????JsonData
        '''
        csvRow = [ row[1:-1].split('",') for row in content.split('\n')[1:]]
        csvRows = []
        for row in csvRow:
                temp = []
                for i in row:
                        if i.find(',"') != -1:
                                temp.append('')
                                temp.append(i.replace(',"',''))
                        else:
                                temp.append(i.replace('"',''))
                csvRows.append(temp)
        df = pd.DataFrame(csvRows,columns=csvHead)
        df.sort_values('Time',inplace=True)
        df.to_csv(fileName,encoding=encode,index=False)
    
        return '200' 


def CsvShutdown(lostDate:str,csvHead:list,csvRow:list,filePath:str,msgType:str) -> str:
        if msgType == 'server':
                date = [lostDate,'no data']
                date.extend(csvRow)
        elif msgType == 'client':
                date = [lostDate,'exceed time']
                date.extend(csvRow)
        Head = ['lost_date','lost_type']
        Head.extend(csvHead)
        if not os.path.exists(filePath): #????????????????????????????????????
                csvFile = open(filePath,'w+',encoding='utf-8-sig',newline='')
                writer = csv.writer(csvFile) #?????????????????????
                writer.writerow(Head) #?????????
                writer.writerow(date)
                csvFile.close()
        else:
                csvFile = open(filePath,'a',encoding='utf-8-sig',newline='')
                writer = csv.writer(csvFile) #?????????????????????
                writer.writerow(date)
                csvFile.close()
        return '200'


if __name__ == '__main__':

        try:

                #????????????
                start = time.perf_counter_ns()

                #????????????
                today = datetime.now().strftime('%Y-%m-%d') 

                log.processLog('==============================================================================================')
                log.processLog(f'?????????????????????Stock?????????????????????????????? {os.path.basename(__file__)}')
                
                #???????????? (????????????????????????)
                #????????????"?????????"??????
                output_dir_path_dict  = json.load(open('pre/set.json','r'))
                
                #???????????? (??????????????????)
                output_dir_path = f"{output_dir_path_dict['output_dir_path']}/KRX/Stock"
                
                if not os.path.exists(output_dir_path):
                        log.processLog(f'?????????????????????{output_dir_path}')
                        os.makedirs(output_dir_path)

                output_dir_date_path = f"{output_dir_path}/{today}"
                if not os.path.exists(output_dir_date_path):
                        log.processLog(f'????????????????????????{output_dir_date_path}')
                        os.makedirs(output_dir_date_path)

                #??????????????????
                log.processLog(f'?????????????????????pre/KR_stk_list.csv')
                codeStock = pd.read_csv('pre/KR_stk_list.csv',encoding='utf-8-sig',dtype={'Stock Abbrv Code': 'str'})
                csvHead = ['Time','Price','Change','Trading Valume (shr.)','Trading Value (KRW)']

                count = 0
                rowCount = 0
                lostFile = 0
                length = len(codeStock)

                for row in codeStock.iterrows():
                        rowCount += 1
                        row = row[1]
                        requestCode = row['Stock Full Code']
                        requestAbbrvCode = row['Stock Abbrv Code']
                        requestAbbrvName = row['Stock Abbrv']

                        fileName = f"{output_dir_date_path}/{requestAbbrvCode}_Trades_{today}.csv"
                        
                        permitCode = GetPermitCode_Down(requestCode,f"A{requestAbbrvCode}/{requestAbbrvName}",f"A{requestAbbrvCode}")
                        log.processLog(f"===== [{rowCount}]_[????????????]???{fileName}")
                        
                        resp = GetFile_Down(permitCode)
                        if resp == '':
                                lostFile +=1
                                log.processLog(f'===== [{rowCount}]_[????????????]????????????${lostFile}??????????????????{output_dir_path}/noExist.csv???')
                                CsvShutdown(today,row.index.tolist(),row.values.tolist(),f"{output_dir_path}/noExist.csv",'server')
                                log.processLog(f'------------------------------------------------------')
                                continue
                        elif resp == ",Time,Price,Change,Trading Valume (shr.),Trading Value (KRW)":
                                count += 1
                                if count >= 20:
                                        log.processLog(f'===== [{rowCount}]_[????????????]????????????????????????({count})??????????????????')
                                        log.processLog(f'------------------------------------------------------')
                                        log.processLog(f'?????????????????? ?????????????????????????????????{today}?????????????????????????????????')
                                        os.system(f"rm -r -f {output_dir_date_path}")
                                        break
                                else:
                                        log.processLog(f'===== [{rowCount}]_[????????????]????????????????????????({count})??????????????????')
                        else:
                                log.processLog(f"===== [{rowCount}]_[????????????]???{fileName}")
                                
                                WriteFile_Down(resp,fileName,csvHead,'utf-8-sig')
                                log.processLog(f'===== [{rowCount}]_[????????????]???{fileName}')
                                log.processLog(f'------------------------------------------------------')
                

                end = time.perf_counter_ns()
                log.processLog(f'?????????????????? {os.path.basename(__file__)} - ????????????:{(end-start)/10**9}')
                log.processLog('==============================================================================================')
        
        except:
                end = time.perf_counter_ns()
                log.processLog(f'?????????????????? {os.path.basename(__file__)} - ????????????:{(end-start)/10**9}')
                log.processLog(f'???????????????????????????????????????{rowCount-1}?????????,??????{length-rowCount}?????????')
                log.processLog(f'??????????????????????????????????????????errorlog')
                log.processLog('==============================================================================================')

                log.errorLog(traceback.format_exc())