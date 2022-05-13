import requests
import json
import requests
import pandas as pd 
import os
import sys 
from traceback import format_exc
import log_manager as log
import time
from datetime import datetime


# 只爬首頁
# obj['metadata']['status']
# obj['data'][0]['cusipDisplay'] #美國證券編碼
# obj['data'][0]['docIssueDt'] #日期與時間
# obj['data'][0]['docTtlTx'] #文件標題
# f"{pdf_url}{obj['data'][0]['docFileNmTx']}" #文件連結

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

    pdf_url = 'https://www.adrbnymellon.com/files/'
    url = "https://www.adrbnymellon.com/publicSiteProxy.jsp"

    payload = json.dumps({
      "countryCode": "",
      "fromDate": "",
      "toDate": "",
      "letter": "",
      "companyName": "",
      "searchText": "",
      "searchType": "1",
      "symbol": "",
      "cusip": "",
      "year": "",
      "limit": -1,
      "count": 50,
      "start": 0, #此參數控制從何開始爬
      "resourceName": "postCorporateActionDirSearch"
    })
    headers = {
      'origin': 'https://www.adrbnymellon.com',
      'referer': 'https://www.adrbnymellon.com/directory/corporate-actions',
      'adurm': 'isAjas:true',
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=94EE84A52EAC5223B93B5B1230EEDC05'
    }

    df = pd.DataFrame()
    count = 0
    check = False
    while True:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
          obj = json.loads(response.text)
          break
        if count >= 10:
          log.processLog(f'[bny.py] 10次連線失敗，本日未完成ADR新聞爬取')
          print(f'[bny.py] 10次連線失敗，本日未完成ADR新聞爬取')
          check = True
          break
        else:
          count += 1
          print(f'[bny.py] ADR新聞爬取第{count}次連線失敗...5秒後重試')
          log.processLog(f'[bny.py] ADR新聞爬取第{count}次連線失敗...5秒後重試')
          time.sleep(5)
          
    if not check:
      
      log.processLog(f'[bny.py] ADR新聞第爬取完成...進入提取階段')
      print(f'[bny.py] ADR新聞第爬取完成...進入提取階段')
      
      for o in obj['data']:
          cusip = o['cusipDisplay']
          cusip = f"'{cusip}'"
          Date = o['docIssueDt'][:10]
          Time = o['docIssueDt'][11:]
          Title = o['docTtlTx']
          URL = f"{pdf_url}{o['docFileNmTx']}"
          row = pd.Series([Date,Time,cusip,Title,URL])
          row_df = pd.DataFrame([row])
          df = pd.concat([df,row_df], ignore_index=True)
          
      df.columns = columns=['Date','Time','CUSIP','Title','URL']

      df.to_csv(f'{tail_path}/Bny.csv',index=False,encoding='utf-8-sig')
      log.processLog(f'[bny.py] 輸出：{tail_path}/Bny.csv')
      print(f'[bny.py] 輸出：{tail_path}/Bny.csv')
      end = time.perf_counter()
      log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
      log.processLog('==============================================================================================')
      
  except:
    log.processLog(f'[bny.py] 本次ADR新聞爬蟲遇到未知問題，請查看錯誤log檔')
    log.processLog(f'---------------------')
    log.errorLog(f'{format_exc()}')
    