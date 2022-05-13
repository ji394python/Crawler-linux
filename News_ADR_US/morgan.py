import requests
import json
import requests
import pandas as pd 
import os
import sys 
from traceback import format_exc
import log_manager as log
import time

# 只爬前1000筆
# obj['data']['items'][0]['action'] #取得動作
# obj['data']['items'][0]['announcementDate'] #發布日期
# obj['data']['items'][0]['country'] #國家
# obj['data']['items'][0]['cusip'] #美國證券編碼
# obj['data']['items'][0]['eventDate'] #事件發生日期
# obj['data']['items'][0]['name'] #Title
# obj['data']['items'][0]['ticker'] #ticker
# obj['data']['items'][0]['notices']['pdf'] #pdf網址
# obj['data']['items'][0]['status'] #不知道是甚麼
if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ADR_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    #計時開始
    start = time.perf_counter() 
    try:
            
        url = "https://api.markitdigital.com/jpmadr-public/v1/corporateActions?offset=0&limit=1000&sortBy=eventDate&sortOrder=desc"

        payload={}
        headers = {}
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
        if not os.path.exists(f'{tail_path}'):
            os.makedirs(f'{tail_path}')
            
        count = 0
        check = False
        while True:
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                obj = json.loads(response.text)
                break
            if count >= 10:
                log.processLog(f'[morgan.py] 10次連線失敗，本日未完成ADR新聞爬取')
                print(f'[morgan.py] 10次連線失敗，本日未完成ADR新聞爬取')
                check = True
                break
            else:
                count += 1
                print(f'[morgan.py] ADR新聞爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[morgan.py] ADR新聞爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
        
        
        if not check:
            log.processLog(f'[morgan.py] ADR新聞爬取完成...進入萃取階段')
            print(f'[morgan.py] ADR新聞爬取完成...進入萃取階段')
            
            df = pd.DataFrame()
            for o in obj['data']['items']:
                action = o['action']
                Date = o['announcementDate'][:10]
                Time = o['announcementDate'][11:19]
                Title = o['name']
                cusip = f"'{o['cusip']}'"
                eventDate = o['eventDate']
                Ticker = o['ticker']
                # status = o['status']
                pdf = o['notices'].get('pdf','')
                if pdf == '':
                    URL = ''
                else:
                    URL = f"https://api.markitdigital.com/jpmadr-public/v1/cms/document?cmsId={pdf}&sequenceNo=1"
                row = pd.Series([Date,Time,Ticker,cusip,action,eventDate,Title,URL])
                row_df = pd.DataFrame([row])
                df = pd.concat([df,row_df], ignore_index=True)
                
            df.columns = columns=['AnnounceDate','Time','Ticker','CUSIP','Action','EventDate','Title','URL']

            df.to_csv(f'{tail_path}/JP_Morgan.csv',index=False,encoding='utf-8-sig')
            log.processLog(f'[mogran.py] 輸出：{tail_path}/JP_Morgan.csv')
            print(f'[mogran.py] 輸出：{tail_path}/JP_Morgan.csv')
            end = time.perf_counter()
            log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
            log.processLog('==============================================================================================')
    except:
        log.processLog(f'[mogran.py] 本次ADR新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')

