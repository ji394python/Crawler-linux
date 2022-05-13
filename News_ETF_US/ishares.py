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

if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    try:
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
        
        main_web = 'https://www.ishares.com'
        url = 'https://www.ishares.com/us/library/press-releases'
        data = {'declinePageUri': 'https://www.blackrock.com/corporate/en-zz/newsroom','ACCEPT': 'ACCEPT'}
        #mainWrapper a
        f = requests.post(url,data=data)  
        check = False
        while f.status_code != 200:
            if count > 10:
                log.processLog(f'[ishares.py] 10次連線失敗，本日未完成ETF新聞爬取')
                print(f'[ishares.py] 10次連線失敗，本日未完成ETF新聞爬取')
                check = True
                break
            count = count + 1
            print(f'[ishares.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
            log.processLog(f'[ishares.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
            time.sleep(5)
            f = requests.post(url,data=data)
            
        if not check:
            resp = BeautifulSoup(f.text,'lxml')

            log.processLog(f'[ishares.py] ETF新聞爬取完成...進入剖析階段')
            print(f'[ishares.py] ETF新聞爬取完成...進入剖析階段')

            Title = [ i.text.strip() for i in resp.select('#mainWrapper a')]
            URL = [ f"{main_web}{i['href']}" for i in resp.select('#mainWrapper a')]
            Date = [ datetime.strptime(i.text,'%b %d, %Y').strftime('%Y-%m-%d') for i in resp.select('.attribution')]
            if len(Title) != len(URL) or len(Title) != len(Date) or len(URL) != len(Date):
                log.processLog(f'[ishares.py] 有問題，標題/日期/網址長度不相同，應是有遺漏標的，請詳細執行程式觀察 {(len(Date),len(Title),len(URL))}')
                print(f'[ishares.py] 有問題，標題/日期/網址長度不相同，應是有遺漏標的，請詳細執行程式觀察 {(len(Date),len(Title),len(URL))}')
            else:
                df = pd.DataFrame({'Date':Date,'Title':Title,'URL':URL})
                df['Issuer'] = 'ishares'
                
                if not os.path.exists(f'{tail_path}'):
                    os.makedirs(f'{tail_path}')
                df.to_csv(f'{tail_path}/ishares.csv',index=False,encoding='utf-8-sig')
                log.processLog(f'[ishares.py] 輸出：{tail_path}/ishares.csv')
                print(f'[ishares.py] 輸出：{tail_path}/ishares.csv')
                
                end = time.perf_counter()
                log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
                log.processLog('==============================================================================================')
        
    except Exception as e:
        log.processLog(f'[ishares.py] 本次ETF新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(e)
