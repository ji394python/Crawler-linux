'''
Author: Denver Liu

Last Modified: 2021/07/18

'''

#File_info: 檔案大小
#News_ID：新聞ID，但不知道是啥
#Short_text：網頁上的公告名稱
#Total_Count：本次查詢所獲得的資料總數
#DOD_WEB_PATH：不知道是甚麼，每次都是""
#Stock_Name：對應網頁上的Stock Short Name
#Stock_Code：06622
#File_link：檔案連結
#Title：檔案名稱

import requests as r
import json
import re
import time as t
import log_manager as log
import os
import traceback
from argparse import RawTextHelpFormatter
import argparse
from datetime import datetime,timedelta
import time
from pandas import date_range
import json

def getStockID(code:str)->str:
    '''
        取得股票代碼：User的代碼要唯一，不然可能會給錯
        例如打6622會出現兩支股票，請輸入完整唯一值
    '''

    code = input('輸入股票代碼')

    para = {'callback':'callback',
            'lang':'ZH',
            'type':'A',
            'name':code,
            'market':'SEHK'}

    response = r.get('https://www1.hkexnews.hk/search/prefix.do?',params=para)
    output = response.text
    output = json.loads(output[output.find('[')+1:output.find('}')+1])

    return(output['stockId'])



if __name__ == '__main__':
    
    # url = 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en' #第一層

    # body = {'lang':'en','category':'0'
    #         ,'from':startDate,'to':endDate,
    #         'stockId':'','searchType':'0',
    #         'documentType':'-1','t1code':'-2',
    #         't2code':'-2','t2Gcode':'-2',
    #         'title':'','MB-Daterange':'0',
    #         'market':'SEHK'}
    
    # firstPage = r.post(url,headers=header,data=body)
    # firstPage_bs4 = BeautifulSoup(firstPage.text,'lxml')
    # target = '.doc-link a'
    # for text in firstPage_bs4.select(target):
    #     print(text.text.strip())

    #路徑設定
    rootPath = json.load(open('set.json','r+'))['output_dir_path']
    output_dir_path =  f'{rootPath}/ETF_NAV/HK/'

    Date = datetime.now() - timedelta(days=1)

    parser = argparse.ArgumentParser(description=
        '目標：下載香港證交所ETF淨值 \
        \n you can download data for a specific time range. (Required)\
        \n \
        \nExamples: python3 crawler.py -st 07/01/2021 -et 07/18/2021 '
        , formatter_class=RawTextHelpFormatter)

    parser.add_argument('-st','--start', action='store', dest='startDate', type=str, 
        help='enter startDate: YYYY/mm/dd',default=Date.strftime('%Y%m%d'))

    parser.add_argument('-et','--end', action='store', dest='endDate', type=str,
        help='enter endDate: YYYY/mm/d',default=Date.strftime('%Y%m%d'))

    args = parser.parse_args()
    
    dates=date_range(start=args.startDate, end=args.endDate)

    try:
        start = time.perf_counter_ns()

        log.processLog('【開始執行香港證交所ETF淨值爬蟲專案】 crawler.py')

        for date_temp in dates:
            
            date=date_temp.strftime("%Y-%m-%d")
            week = datetime.weekday(date_temp)
            if week in [5,6]:
                log.processLog(f'【程序中止】 周末無資料_{date}，不需爬蟲')
                continue

            if not os.path.exists(output_dir_path + date):
                log.processLog(f'建立資料夾：{output_dir_path + date}')
                os.makedirs(output_dir_path + date)


            #欄位設定
            colNames = ['FILE_INFO', 'NEWS_ID', 'SHORT_TEXT', 'TOTAL_COUNT', 'DOD_WEB_PATH', 'STOCK_NAME', 'FILE_TYPE', 'DATE_TIME', 'LONG_TEXT', 'STOCK_CODE', 'FILE_LINK']
            log.processLog(f'解讀欄位設定：{colNames}')
            

            header = {
                'origin':'https://www1.hkexnews.hk',
                'referer': 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en',
                'User-Agent':'Edg/91.0.4472.124'
                }


            url2 = 'https://www1.hkexnews.hk/search/titleSearchServlet.do' #第二層


            t1code = [80000,81000]
            t1code = {
                80000:'Trading Information of Exchange Traded Funds (t1code = 80000)',
                81000:'Trading Information of Leveraged and Inverse Products (t1code = 81000)'
                }
            #Trading Information of Exchange Traded Funds：t1code(80000)
            #Trading Information of Leveraged and Inverse Products：t1code(81000)

            for t1 in t1code.keys():
                log.processLog(f'開始爬取：{t1code[t1]}')

                params = {
                    'sortDir':'0','sortByOptions':'DateTime',
                    'category':'0','market':'SEHK','stockId':'-1',
                    'documentType':'-1','fromDate': date.replace('-',''),'toDate':date.replace('-',''),
                    'title':'','searchType':'0','t1code':str(t1),
                    't2Gcode':'-2','t2code':'-2','rowRange':'600','lang':'en'
                    }

                log.processLog(f'=== 本次查詢參數設定完成：{params}')

                output = r.get(url2,headers=header,params=params)

                connect_count = 0
                while output.status_code != 200:
                    t.sleep(5)
                    connect_count += 1
                    log.processLog(f'=== [掉線]第{connect_count}次掉線，嘗試重連中...')
                    output = r.get(url2,headers=header,params=params)
                    if connect_count > 10:
                        log.processLog(f'=== [終止]第{connect_count}次掉線，程序中止，請手動查明網站失聯原因')
                        break

                results = json.loads(output.content.decode('utf-8'))
                log.processLog(f'=== 讀取Json回應檔完成')

                while results['hasNextRow']:
                    log.processLog(f'=== rowRange過少,新增load more')
                    params['rowRange'] = str(int(params['rowRange'])+1000)
                    connect_count = 0
                    while output.status_code != 200:
                        t.sleep(5)
                        connect_count += 1
                        log.processLog(f'=== [掉線]第{connect_count}次掉線，嘗試重連中...')
                        output = r.get(url2,headers=header,params=params)
                        if connect_count > 10:
                            log.processLog(f'=== [終止]第{connect_count}次掉線，程序中止，請手動查明網站失聯原因')
                            break
                    results = json.loads(output.content.decode('utf-8'))
                    log.processLog(f'=== 讀取Json回應檔完成')

                outcome = re.split('{(.*?)}',results['result'].replace('\\',''))[1:-1]
                log.processLog(f'=== 本次下載檔案數量：{(len(outcome)+1)/2}')

                for i in range(len(outcome)):
                    if i % 2==0:
                        resultText = outcome[i].replace('u003cbr/u003e','、').replace('"','')
                        resultText_clean = resultText[:resultText.find('TITLE')] + resultText[resultText.find('FILE_TYPE'):]

                        urlFileTail = ''
                        code = ''
                        for col in colNames:
                            if col == 'FILE_LINK':
                                # print(i,re.findall(f'FILE_LINK:.+',resultText_clean) )
                                urlFileTail = re.findall(f'FILE_LINK:.+',resultText_clean)[0].replace('FILE_LINK:','')
                            else:
                                if col == 'STOCK_CODE':
                                    code = re.findall(f'{col}:(.*?),',resultText_clean)[0].replace('、','_')
                                # print(i,re.findall(f'{i}:(.*?),',resultText_clean))

                        urlFileHead = 'https://www1.hkexnews.hk'
                        urlFile = urlFileHead + urlFileTail
                        name = urlFile[urlFile.rfind('/')+1:]
                        #name = 'HK_IV_'+ code + '_' + name[:4]+ '-' + name[4:6] + '-' + name[6:8] + name[name.find('.'):]
                        fileName = f"HK_IV_{code}_{name[:4]}-{name[4:6]}-{name[6:8]}{name[name.find('.'):]}"
                        
                        targetFile = r.get(urlFile)
                        with open(f"{output_dir_path}{date}/{fileName}",'wb+') as f:
                            print(f'寫入：{output_dir_path}{date}/{fileName}')
                            log.processLog(f'===== 寫入檔案_[{int(i/2)}]： {output_dir_path}{date}/{fileName} ')
                            f.write(targetFile.content)
                            f.close()

        end = time.perf_counter_ns() 
        log.processLog(f'【結束程序】 crawler.py - 執行時間:{(end-start)/10**9}')
        log.processLog('==============================================================================================')
        print('end')

    except Exception as e:
        log.processLog('[程序錯誤] crawler.py 執行錯誤，請查看錯誤log檔')
        log.errorLog(traceback.format_exc())
        traceback.print_exc()