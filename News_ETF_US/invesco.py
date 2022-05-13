import os
import pandas as pd
import requests
import json
from traceback import format_exc
import log_manager as log
import time
from bs4 import BeautifulSoup
import lxml
from datetime import datetime
#原本直系處理，後來發現會帶空直，故變成一列一列來判斷

if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    try:
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
    
        web_url = 'https://www.invesco.com'
        row = 0
        Title,Date,URL = [],[],[]
        for i in range(0,100,10):
            url = f'https://www.invesco.com/us/newsroom?FilterGGA=placeholder&maxResults=10& \
                    viewType=&firstResult={i}&FilterList=FCLASS_SUBJECT_MATTER%26NOOP_FIELD_VALUE&audienceType=Investor& \
                    audienceType=Investor'
            r = requests.get(url)  
            check = False
            while r.status_code != 200:
                if count > 10:
                    log.processLog(f'[invesco.py] 10次連線失敗，本日未完成ETF新聞爬取')
                    print(f'[invesco.py] 10次連線失敗，本日未完成ETF新聞爬取')
                    check = True
                    break
                count = count + 1
                print(f'[invesco.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[invesco.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(url)
            if check: break
            
            resp = BeautifulSoup(r.text,'lxml')
            
            log.processLog(f'[invesco.py] ETF新聞第{int(i/10)}頁爬取完成...進入剖析階段')
            print(f'[invesco.py] ETF新聞第{int(i/10)}頁爬取完成...進入剖析階段')
            
            for card in resp.select('.main-content .widget'):
                title = card.select('h3 a')[0].text.strip()
                url = card.select('h3 a')[0]['href']
                date = card.select('.muted')
                if sum([len(date),len(title),len(url)]) == 0:
                    log.processLog(f'[invesco.py] 第{int(i/10)}頁剖析無任何東西，判斷應以爬取完畢，故剖析結束...等候檔案輸出')
                    print(f'[invesco.py] 第{int(i/10)}頁剖析無任何東西，判斷應以爬取完畢，故剖析結束...等候檔案輸出')
                    break
                if len(date) == 0:
                    date = 'NA'
                else:
                    date = datetime.strptime(date[0].text.strip(),'%b %d, %Y').strftime('%Y-%m-%d')
                Title.append(title)
                URL.append(f"{web_url}{url}")
                Date.append(date)
                
            if len(Title) != len(URL) or len(Title) != len(Date) or len(URL) != len(Date):
                log.processLog(f'[invesco.py] 有問題，標題/日期/網址長度不相同，應是有遺漏標的，請詳細執行程式觀察 {(len(Date),len(Title),len(URL))}')
                print(f'[invesco.py] 有問題，標題/日期/網址長度不相同，應是有遺漏標的，請詳細執行程式觀察 {(len(Date),len(Title),len(URL))}')
                check = True
                break
            
        if not check:
            df = pd.DataFrame({'Date':Date,'Title':Title,'URL':URL})
            df['Issuer'] = 'Invesco'
            
            if not os.path.exists(f'{tail_path}'):
                os.makedirs(f'{tail_path}')
            df.to_csv(f'{tail_path}/invesco.csv',index=False,encoding='utf-8-sig')
            log.processLog(f'[invesco.py] 輸出：{tail_path}/invesco.csv')
            print(f'[invesco.py] 輸出：{tail_path}/invesco.csv')
                    
            end = time.perf_counter()
            log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
            log.processLog('==============================================================================================')
    except Exception as e:
        log.processLog(f'[invesco.py] 本次ETF新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(e)
        
