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

# 得到頁數：'.paginator__button'
# 得到標題：.article-card__card-wrap > a
# 得到內文連結：.article-card__link
# 得到發布日期：.article-card span

# r = requests.get('https://www.proshares.com/press-releases/?page=1')
# BeautifulSoup(r.text,'lxml').select('.article-card__card-wrap > a')[9].text
# BeautifulSoup(r.text,'lxml').select('.paginator__button')[2].text
# BeautifulSoup(r.text,'lxml').select('.article-card span')
# len(BeautifulSoup(r.text,'lxml').select('.article-card__link'))
# len(BeautifulSoup(r.text,'lxml').select('.paginator__button'))
# len(BeautifulSoup(r.text,'lxml').select('.article-card span'))



if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    tail_path = json.load(open(f"path.json",'r'))
    tail_path = tail_path['NasPublic']
    
    
    web_url = 'https://www.proshares.com'
    
    Title,Date,URL,Issuer = [],[],[],[]
    page = 1
    
    try:
        while True:
            url = f'https://www.proshares.com/press-releases/?page={page}'
            r = requests.get(url)
            check = False
            while r.status_code != 200:
                if count > 10:
                    log.processLog(f'[proshares.py] 10次連線失敗，本日未完成ETF新聞爬取')
                    print(f'[proshares.py] 10次連線失敗，本日未完成ETF新聞爬取')
                    check = True
                    break
                count = count + 1
                print(f'[proshares.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[proshares.py] ETF新聞爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(url)
            if check: break
            resp = BeautifulSoup(r.text,'lxml')
            log.processLog(f'[proshares.py] ETF新聞第{page}頁爬取完成...進入剖析階段')
            print(f'[proshares.py] ETF新聞第{page}頁爬取完成...進入剖析階段')
            
            titles = resp.select('.article-card__card-wrap > a')
            pages = resp.select('.paginator__button')
            dates = resp.select('.article-card span')
            urls = resp.select('.article-card__link')
            if len(titles)!=len(dates) or len(titles)!=len(urls) or len(dates)!=len(urls):
                print('本次爬蟲長度不合，有問題')
                log.processLog(f'[proshares.py] 本頁{url}的爬蟲長度不合(title,urls,dates)=({len(titles)},{len(urls)},{len(dates)})，請查看URL找出問題，會先忽略此頁繼續爬蟲')
                print(f'[proshares.py] 本頁{url}的爬蟲長度不合(title,urls,dates)=({len(titles)},{len(urls)},{len(dates)})，請查看URL找出問題，會先忽略此頁繼續爬蟲')
            else:
                for i in range(len(titles)):
                    title = titles[i].text
                    date = dates[i].text
                    url = urls[i]['href']
                    Title.append(title)
                    Date.append(datetime.strptime(date,'%b %d, %Y').strftime('%Y-%m-%d'))
                    URL.append(f"{web_url}{url}")
                    Issuer.append('Proshares')
            page += 1
            if page > len(pages):
                log.processLog(f'[proshares.py] 爬取與剖析完成...本次共爬取{page-1}頁')
                print(f'[proshares.py] 爬取與剖析完成...本次共爬取{page-1}頁')
                break
        if not check:
            df = pd.DataFrame({'Date':Date,'Title':Title,'Issuer':Issuer,'URL':URL})
            if not os.path.exists(f'{tail_path}'):
                os.makedirs(f'{tail_path}')
                
            df.to_csv(f'{tail_path}/proshares.csv',index=False,encoding='utf-8-sig')
            log.processLog(f'[proshares.py] 輸出：{tail_path}/proshares.csv')
            print(f'[proshares.py] 輸出：{tail_path}/proshares.csv')
            
            end = time.perf_counter()
            log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
            log.processLog('==============================================================================================')
    except Exception as e:
        log.processLog(f'[proshares.py] 本次ETF新聞爬蟲遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(e)
    
   

