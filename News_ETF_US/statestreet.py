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

# 標題：'Headline' 
# 發布時間：'PressReleaseDate' 
# TagList： 'TagsList'  (空值的話代表PRESS RELEASE)
# 文章第一段：ShortBody 
# 文章網址： LinkToDetailPage

if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    try:
        
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
        
        web_url = 'https://investors.statestreet.com'
        url = 'https://investors.statestreet.com/feed/PressRelease.svc/GetPressReleaseList?apiKey=BF185719B0464B3CB809D23926182246 \
                &LanguageId=1&bodyType=3&pressReleaseDateFilter=3&categoryId=1cb807d2-208f-4bc3-9133-6a9ad45ac3b0&pageSize=-1&pageNumber=0 \
                &tagList=&includeTags=true&year=-1&excludeSelection=1'

        count = 0
        Title,Date,Time,Tag,Issuer,URL = [],[],[],[],[],[]
        r = requests.get(url)
        check = False
        while r.status_code != 200:
            if count > 10:
                log.processLog(f'[statestreet.py] 10次連線失敗，本日未完成ETF新聞爬取')
                print(f'[statestreet.py] 10次連線失敗，本日未完成ETF新聞爬取')
                check = True
                break
            count = count + 1
            print(f'[statestreet.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
            log.processLog(f'[statestreet.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
            time.sleep(5)
            r = requests.get(url)
        if not check:
            d = json.loads(r.text)
            log.processLog(f'[statestreet.py] ETF新聞爬取完成...進入json提取階段')
            print(f'[statestreet.py] ETF新聞爬取完成...進入json提取階段')

            date = d['GetPressReleaseListResult'][1]['PressReleaseDate']
            for article in d['GetPressReleaseListResult']:
                title = article['Headline']
                date = datetime.strptime(article['PressReleaseDate'],'%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d')
                t = datetime.strptime(article['PressReleaseDate'],'%m/%d/%Y %H:%M:%S').strftime('%H:%M:%S')
                url = f"{web_url}{article['LinkToDetailPage']}"
                tag = 'PRESS RELEASE' if len(article['TagsList']) == 0 else article['TagsList'][0]
                Title.append(title)
                Date.append(date)
                Time.append(t)
                Tag.append(tag)
                Issuer.append('State Street')
                URL.append(url)
            df = pd.DataFrame({'Date':Date,'Time':Time,'Tag':Tag,
                            'Title':Title,'Issuer':Issuer,'URL':URL})
            
            if not os.path.exists(f'{tail_path}'):
                os.makedirs(f'{tail_path}')
            df.to_csv(f"{tail_path}/states.csv",index=False,encoding='utf-8-sig')
            log.processLog(f'[statestreet.py] 輸出：{tail_path}/states.csv')
            print(f'[statestreet.py] 輸出：{tail_path}/states.csv')
            
            end = time.perf_counter()
            log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
            log.processLog('==============================================================================================')
        
    except Exception as e:
        log.processLog(f'[statestreet.py] 本次ETF新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(e)

