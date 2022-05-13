import requests
import json
import requests
import pandas as pd 
import os
import sys 
from traceback import format_exc
import log_manager as log
import time
from datetime import datetime,timedelta
import calendar

# 當月全部都爬!
# pdf_url = 'https://www.adr.db.com/drwebrebrand/dr-universe/dr_details.html?identifier=7612#/corporate-actions'
# obj['results'][0]['companyName'] #公司名稱
# obj['results'][0]['actionTypeName'] #行動種類
# obj['results'][0]['drStructure'] #Program Type
# obj['results'][0]['cusip'] #CUSIP
# obj['results'][0]['date'] #1970-01-01開始起算的毫秒  ms/86400/1000 轉成 Days
# obj['results'][0]['dateType'] #給的日期種類
# obj['results'][0]['drSymbol'] #Ticker
# obj['results'][0]['drId'] #文件網址

if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ADR_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    #計時開始
    start = time.perf_counter() 
    try:
    
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
        if not os.path.exists(f'{tail_path}'):
            os.makedirs(f'{tail_path}')

        now = datetime.now()
        this_month_start = datetime(now.year, now.month, 1)
        this_month_end = datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1])

        r = requests.session()
        count = 0
        check = False
        while True:
            h = r.get('https://www.adr.db.com/drwebrebrand/api/druniverse/actions')
            if h.status_code == 200:
                break
            if count >= 10:
                log.processLog(f'[deutsche.py] 10次連線失敗，本日未完成ADR新聞爬取')
                print(f'[deutsche.py] 10次連線失敗，本日未完成ADR新聞爬取')
                check = True
                break
            else:
                count += 1
                print(f'[deutsche.py] ADR新聞爬取取得Token第{count}次連線失敗...5秒後重試')
                log.processLog(f'[deutsche.py] ADR新聞爬取取得Token第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                
        if not check:
            log.processLog(f'[deutsche.py] ADR新聞Token取得完成...進入爬取階段')
            print(f'[deutsche.py] ADR新聞Token取得完成...進入爬取階段')
            
            payload = json.dumps({
            "page": 0,
            "size": 80,
            "query": "",
            "actionTypeId": 0,
            "regionId": 0,
            "exchange": "",
            "countryId": 0,
            "dateFrom": this_month_start.strftime('%Y-%m-%d'),
            "dateTo": this_month_end.strftime('%Y-%m-%d')
            })

            headers = {
            'origin': 'https://www.adr.db.com',
            'referer': 'https://www.adr.db.com/drwebrebrand/dr-universe/corporate_actions_type_e.html',
            'x-csrf-token':  h.headers['X-CSRF-TOKEN'],
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36 Edg/101.0.1210.32',
            'x-auth-token': '',
            'Content-Type': 'application/json;charset=UTF-8',
            'Cookie': h.headers['Set-Cookie']
            }



            df = pd.DataFrame()
            page = 0
            while True:
                url = f"https://www.adr.db.com/drwebrebrand/api/corporateactions/search?page={page}&size=80"
                while True:
                    resp = r.request("POST", url, headers=headers,data=payload)
                    if resp.status_code == 200:
                        obj = json.loads(resp.text)
                        print('[200]')
                        break
                    
                if len(obj['results']) == 0 :
                    print(f'結束：{page}')
                    break
                page += 1
                for o in obj['results']:
                    companyName = o['companyName']
                    actionTypeName = o['actionTypeName']
                    drStructure = o['drStructure']
                    cusip = f"'{o['cusip']}'"
                    ticker = o['drSymbol'] #Ticker
                    Date = (datetime.strptime('1970-01-01','%Y-%m-%d') + timedelta(days=(o['date']/86400/1000))).strftime('%Y-%m-%d')
                    dateType = o['dateType']
                    URL = f"https://www.adr.db.com/drwebrebrand/dr-universe/dr_details.html?identifier={o['drId']}#/corporate-actions"
                    row = pd.Series([companyName,actionTypeName,drStructure,cusip,ticker,Date,dateType,URL])
                    row_df = pd.DataFrame([row])
                    df = pd.concat([df,row_df], ignore_index=True)
                
                
            page = 0
            while True:
                cash_url = f"https://www.adr.db.com/drwebrebrand/api/corporateactions/cash-dividends/search?page={page}&size=80"
                while True:
                    resp = r.request("POST", cash_url, headers=headers,data=payload)
                    if resp.status_code == 200:
                        obj = json.loads(resp.text)
                        print('[200]')
                        break
                    
                if len(obj['results']) == 0 :
                    print(f'結束：{page}')
                    break
                page += 1
                for o in obj['results']:
                    companyName = o['companyName']
                    actionTypeName = o['actionTypeName']
                    drStructure = o['drStructure']
                    cusip = f"'{o['cusip']}'"
                    ticker = o['drSymbol'] #Ticker
                    Date = (datetime.strptime('1970-01-01','%Y-%m-%d') + timedelta(days=(o['date']/86400/1000))).strftime('%Y-%m-%d')
                    dateType = o['dateType']
                    URL = f"https://www.adr.db.com/drwebrebrand/dr-universe/dr_details.html?identifier={o['drId']}#/corporate-actions"
                    row = pd.Series([companyName,actionTypeName,drStructure,cusip,ticker,Date,dateType,URL])
                    row_df = pd.DataFrame([row])
                    df = pd.concat([df,row_df], ignore_index=True)
                
                

            df.columns = ['Company','ActionTypeName','ProgramType','CUSIP','Ticker','Date','DateType','URL']
            df.sort_values('Date',ascending=False,inplace=True)
            df.to_csv(f'{tail_path}/Deutsche.csv',index=False,encoding='utf-8-sig')
            log.processLog(f'[deutsche.py] 輸出：{tail_path}/Deutsche.csv')
            print(f'[deutsche.py] 輸出：{tail_path}/Deutsche.csv')
            end = time.perf_counter()
            log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
            log.processLog('==============================================================================================')
    except:
        log.processLog(f'[deutsche.py] 本次ADR新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')