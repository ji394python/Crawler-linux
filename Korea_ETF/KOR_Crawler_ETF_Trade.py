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
import csv 
import re

def GetPermitCode(etfCode:str) -> str:
        url = 'https://global.krx.co.kr/contents/COM/GenerateOTP.jspx'
        header = {'referer':'https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'x-requested-with': 'XMLHttpRequest',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-TW,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'}
        params = {'name': 'fileDown','filetype': 'csv',
                'url':'GLB/05/0507/0507010104/glb0507010104_02',
                'acsString':'1',
                'gubun':'00',
                'isu_cd':etfCode,
                'pagePath': '/contents/GLB/05/0507/0507010104/GLB0507010104.jsp'}
        # params = {'bld':'GLB/05/0507/0507010104/glb0507010104_02','name':'grid'}

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

def GetFile(permitCode:str) -> requests.Response:

        url2 = 'https://file.krx.co.kr/download.jspx'
        header = {'referer':'https://global.krx.co.kr/',
                'origin':'https://global.krx.co.kr',
                'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.52',
                'content-type': 'application/x-www-form-urlencoded',
                'upgrade-insecure-requests': '1'}
        data = {
                'code':permitCode
        }
        new=requests.request('POST',url2,headers=header,data=data)
        connect_count = 0
        while ( (new.status_code != 200) or (new.text == '')):
                time.sleep(5)
                connect_count += 1
                new=requests.request('POST',url2,headers=header,data=data)
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


                log.processLog('==============================================================================================')
                log.processLog(f'?????????????????????ETF?????????????????????????????? {os.path.basename(__file__)}')
                
                #????????????
                start = time.perf_counter_ns()

                #????????????
                today = datetime.now().strftime('%Y-%m-%d') 

                #???????????? (????????????????????????)
                #????????????"?????????"??????
                output_dir_path_dict  = json.load(open('pre/set.json','r'))
                
                #???????????? (??????????????????)
                output_dir_path = f"{output_dir_path_dict['output_dir_path']}/KRX/ETF/Trade"
                
                if not os.path.exists(output_dir_path):
                        log.processLog(f'?????????????????????{output_dir_path}')
                        os.makedirs(output_dir_path)

                output_dir_date_path = f"{output_dir_path}/{today}"
                if not os.path.exists(output_dir_date_path):
                        log.processLog(f'????????????????????????{output_dir_date_path}')
                        os.makedirs(output_dir_date_path)

                #??????????????????
                log.processLog(f'?????????????????????pre/etf_code.csv')
                codeEtf = pd.read_csv('pre/etf_code.csv',encoding='utf-8-sig')
                csvHead = ['Time','Price','Change','iNAV','Underlying Index','Trading Volume (share)','Trading Volume at the time (share)']
                
                count = 0
                rowCount = 0
                lostFile = 0
                length = len(codeEtf)
                
                log.processLog(f'?????????????????????ETF????????????????????? - https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp')
                log.processLog(f'=== ?????????????????????${today}')
                for row in codeEtf.iterrows():
                        rowCount += 1
                        row = row[1]
                        fileName = f"{output_dir_date_path}/{row['Stock Abbrv Code']}_Trades_{today}.csv"
                        permitCode = GetPermitCode(row['isu_cd'])
                        log.processLog(f"===== [{rowCount}]_[????????????]???{row['isu_cd']}")
                        resp = GetFile(permitCode)
                        log.processLog(f"===== [{rowCount}]_[????????????]???{row['isu_cd']}")
                        
                        if resp == '':
                                lostFile +=1
                                log.processLog(f'===== [{rowCount}]_[????????????]????????????${lostFile}??????????????????{output_dir_path}/noExist.csv???')
                                CsvShutdown(today,row.index.tolist(),row.values.tolist(),f"{output_dir_path}/noExist.csv",'server')
                                log.processLog(f'------------------------------------------------------')
                                continue
                        elif resp == 'Time,Price,Change,iNAV,Underlying Index,Trading Volume (share),Trading Volume at the time (share)':
                                count += 1
                                if count >= 30:
                                        log.processLog(f'===== [{rowCount}]_[????????????]????????????????????????({count})??????????????????')
                                        log.processLog(f'------------------------------------------------------')
                                        log.processLog(f'?????????????????? ?????????????????????????????????{today}?????????????????????????????????')
                                        os.system(f"rm -r -f {output_dir_date_path}")
                                        break
                                else:
                                        log.processLog(f'===== [{rowCount}]_[????????????]????????????????????????({count})??????????????????')
                        else:
                                WriteFile_Down(resp,f'{fileName}',csvHead,'utf-8-sig')
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


