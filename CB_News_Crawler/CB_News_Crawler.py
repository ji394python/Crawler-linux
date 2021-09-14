# -*- coding: utf-8 -*-
# 爬取公開資訊觀測站的公司債公告網址：https://mops.twse.com.tw/mops/web/t108sb08_1_q2
# 爬取原始新聞

"""
@author: Denver Liu

Last Modified：08/26

"""


import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime,timedelta
import time
import json
import argparse
from argparse import RawTextHelpFormatter
import log_manager as log
import traceback

# Get source code of the web page
def getWebpage(url: str) -> str:
    '''
            斷掉重連的機制函數
    '''
    r = requests.post(url=url,
                      allow_redirects=True,
                      headers=headers,
                      data=data)
    if r.status_code != 200:
        print('Invalid URL:', r.url)
        return None
    else:
        return r.text


# Convert year format to A.D. (轉換民國 -> 西元)
# Input type: YYY/MM/DD
def transferDate(dateString: str) -> datetime.timetuple:
    '''
            <Ex.> 97/06/13 -> 2008
    '''
    try:
        dateList = [int(num) for num in dateString.split('/')]
        dateList[0] += 1911
        return datetime(*dateList).date()
    except:
        return None


def main(DDate):

    start_time = datetime.now()

    data['month'] = DDate.split('-')[1]
    data['day1'] = DDate.split('-')[2]
    data['day2'] = DDate.split('-')[2]
    data['year'] = str(int(DDate.split('-')[0]) - 1911)
    date = DDate
        
    # Record number of crawled news
    news_count = 0
    # Transfer news Dataframes
    # Create empty transfer news DataFrame (記錄拿的到檔案但沒有資料的)
    empty_transfer_header = ['公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
    empty_transfer_df = pd.DataFrame(columns=empty_transfer_header)
    empty_transfer_count = 0
    # Create invalid transfer news DataFrame for URLs that cannot be connected (記錄拿不到檔案的)
    invalid_transfer_header = ['公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
    invalid_transfer_df = pd.DataFrame(columns=invalid_transfer_header)
    invalid_transfer_count = 0

    # Price news Dataframes
    # Create empty price news DataFrame
    empty_price_header = ['種類', '公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
    empty_price_df = pd.DataFrame(columns=empty_price_header)
    empty_price_count = 0
    # Create invalid price news DataFrame for URLs that cannot be connected
    invalid_price_header = ['種類', '公司代號', '公司名稱', '申報日期', '申報序號', '主旨']
    invalid_price_df = pd.DataFrame(columns=invalid_price_header)
    invalid_price_count = 0

    # Crawl index page
    # Retry when ConnectionError happened (Using status code to judge)
    connect_count = 0
    while connect_count < max_attempt:
        time.sleep(timeout)  # timeout = 6
        try:
            # index_url='https://mops.twse.com.tw/mops/web/ajax_t108sb08_1'
            index_page = getWebpage(INDEX_URL)
        except Exception as e:
            print('---------------------------------------------------')
            log.processLog('---------------------------------------------------')
            print('Crawling failed with error:', str(e))
            log.processLog(f'Crawling failed with error:{str(e)}')
            if connect_count == max_attempt - 1:
                print('Failed to reach the index page. Please restart the execution.')
                log.processLog('Failed to reach the index page. Please restart the execution.')
                break
            else:
                print('Reconnect to index page.')
                log.processLog('Reconnect to index page.')
            print('---------------------------------------------------')
            log.processLog('---------------------------------------------------')
            connect_count += 1
            continue

        if index_page:  # not amount to zero
            print('Start crawling news info on %s...' % date)
            log.processLog('Start crawling news info on %s...' % date)
            soup = BeautifulSoup(index_page, 'html.parser')
            form1 = soup.find('form', {'name': 'fm'})  # 轉(交)換公司債停止轉(交)換公告
            form2 = soup.find('form', {'name': 'formX'})  # 轉換公司債轉換價格變更公告

            # Crawl news in form 1
            for table in form1.find_all('table'):
                for tr in table.find_all('tr',
                                         {'class': re.compile('odd|even')}):
                    tds = tr.find_all('td')
                    for i in range(len(tds)):
                        # Retrieve posted data when clicking the button
                        if i == 5:
                            onclick = tds[i].input['onclick']
                            onclick_array = onclick.split(';')
                            for item in onclick_array:
                                if item.split('=')[0] == 'document.fm.dat1.value':
                                    data['dat1'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'document.fm.bro_date.value':
                                    data['bro_date'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'document.fm.co_id.value':
                                    data['co_id'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'document.fm.seq_no.value':
                                    data['seq_no'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'document.fm.date1.value':
                                    data['date1'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'document.fm.step.value':
                                    data['step'] = item.split('=')[1].strip('"')

                                elif item.split('=')[0] == 'action':
                                    news_url = item.split('=')[1].strip('"')

                        elif i == 2:
                            tds[i] = transferDate(tds[i].text)
                        else:
                            tds[i] = tds[i].text.strip().rstrip('*')

                    # Crawl news content in pop-up
                    # Retry when ConnectionError happened
                    connect_count = 0
                    while connect_count < max_attempt:
                        time.sleep(timeout)
                        try:
                            news_page = getWebpage(TWSE_URL + news_url)
                        except Exception as e:
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            print('Crawling failed with error:', str(e))
                            log.processLog(f'Crawling failed with error:{str(e)}')
                            if connect_count == max_attempt - 1:
                                print('Failed:', tds[1])
                                log.processLog(f'Failed:{tds[1]}')
                                print('Reached maximum number of attempts. Moving to next item.')
                                log.processLog('Reached maximum number of attempts. Moving to next item.')
                                # Save invalid transfer news info
                                invalid_transfer_df.loc[
                                    invalid_transfer_count] = [*tds[:-1]]
                                invalid_transfer_count += 1
                                break
                            else:
                                print('Reconnect:', tds[1])
                                log.processLog(f'Reconnect:{tds[1]}')
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            connect_count += 1
                            continue

                        if news_page:
                            print('Crawling news:', tds[1])
                            log.processLog(f'Crawling news:{tds[1]}')
                            try:
                                news_soup = BeautifulSoup(news_page, 'html.parser')
                                trs = news_soup.find_all('table', 'noBorder')[1].find_all('tr')[:-1]
                                text = ''
                                for i in range(len(trs)):
                                    text += trs[i].text + '\n'
                                text = text = text.replace('\xa0','').replace('\u3000','').replace('\ufa08','')
                                # Write transfer news to text file
                                text_path = os.path.join(output_dir_path, tds[2].strftime('%Y-%m-%d'))
                                if not os.path.exists(text_path):
                                    os.makedirs(text_path)
                                with open(os.path.join(text_path, tds[0] + '_' + tds[1] + '_' + tds[2].strftime('%Y-%m-%d') +'_' + tds[3] + '.txt'),'w',encoding='utf-8-sig') as f:
                                    f.write(text)
                                news_count += 1
                            except Exception as e:
                                print('---------------------------------------------------')
                                log.processLog('---------------------------------------------------')
                                print('Crawling failed with error:', str(e))
                                log.processLog(f'Crawling failed with error:{str(e)}')
                                print('Failed:', tds[1])
                                log.processLog(f'Failed:{tds[1]}')
                                print('The news page is connected without required information.')
                                log.processLog('The news page is connected without required information.')
                                print('---------------------------------------------------')
                                log.processLog('---------------------------------------------------')
                                # Save empty transfer news info
                                empty_transfer_df.loc[empty_transfer_count] = [*tds[:-1]]
                                empty_transfer_count += 1

                        else:
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            print('Failed:', tds[1])
                            log.processLog(f'Failed:{tds[1]}')
                            print('The news page is connected without required information.')
                            log.processLog('The news page is connected without required information.')
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            # Save empty transfer news info
                            empty_transfer_df.loc[empty_transfer_count] = [*tds[:-1]]
                            empty_transfer_count += 1
                        break

            # Crawl news in form 2
            for table in form2.find_all('table'):
                for tr in table.find_all('tr',{'class': re.compile('odd|even')}):
                    tds = tr.find_all('td')
                    # Set type of news as none if not exist
                    if len(tds) == 6:
                        tds.insert(0, None)

                    for i in range(len(tds)):
                        if tds[i]:
                            # Retrieve posted data when clicking the button
                            if i == 6:
                                onclick = tds[i].input['onclick']
                                onclick_array = onclick.split(';')[0].split(',')
                                data['co_id'] = onclick_array[2].strip('"')
                                data['seq_no'] = onclick_array[3].strip('"')
                                data['date1'] = onclick_array[4].split('"')[1]
                                data['step'] = '2'

                                # Calculate value attached in the news URL
                                x = int(onclick_array[1])
                                actionValue = 0
                                if x >= 1 and x <= 3:
                                    actionValue = (x - 1) * 5 + (int(onclick_array[5].split('"')[1]) -2) + 5
                                else:
                                    actionValue = (x - 4) + 20
                                if actionValue < 10:
                                    news_url = '/mops/web/ajax_t120sb0' + str(actionValue)
                                else:
                                    news_url = '/mops/web/ajax_t120sb' + str(actionValue)
                                if x >= 7 and x <= 9:
                                    data['pub_class'] = str(onclick_array[5].split('"')[1])

                            elif i == 3:
                                tds[i] = transferDate(tds[i].text)
                            else:
                                tds[i] = tds[i].text.strip().rstrip('*')

                    # Crawl news content in pop-up
                    # Retry when ConnectionError happened
                    connect_count = 0
                    while connect_count < max_attempt:
                        time.sleep(timeout)
                        try:
                            news_page = getWebpage(TWSE_URL + news_url)
                        except Exception as e:
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            print('Crawling failed with error:', str(e))
                            log.processLog(f'Crawling failed with error:{str(e)}')
                            if connect_count == max_attempt - 1:
                                print('Failed:', tds[2])
                                log.processLog(f'Failed:{tds[2]}')
                                print('Reached maximum number of attempts. Moving to next item.')
                                log.processLog('Reached maximum number of attempts. Moving to next item.')
                                # Save invalid price news info
                                invalid_price_df.loc[invalid_price_count] = [*tds[:-1]]
                                invalid_price_count += 1
                                break
                            else:
                                print('Reconnect:', tds[2])
                                log.processLog(f'Reconnect:{tds[2]}')
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            connect_count += 1
                            continue

                        if news_page:
                            print('Crawling news:', tds[2])
                            log.processLog(f'Crawling news:{tds[2]}')
                            try:
                                news_soup = BeautifulSoup(news_page, 'html.parser')
                                trs = news_soup.find_all('table', 'noBorder')[1].find_all('tr')
                                text = ''
                                for i in range(len(trs)):
                                    text += trs[i].text + '\n'
                                text = text.replace('\xa0','').replace('\u3000','').replace('\ufa08','')

                                # Write price news to text file
                                text_path = os.path.join(output_dir_path, tds[3].strftime('%Y-%m-%d'))
                                if not os.path.exists(text_path):
                                    os.makedirs(text_path)
                                with open(os.path.join(text_path, tds[1] + '_' + tds[2] +'_' + tds[3].strftime('%Y-%m-%d') +'_' + tds[4] + '.txt'),'w',encoding='utf-8-sig') as f:
                                    f.write(text)
                                news_count += 1
                            except Exception as e:
                                print('---------------------------------------------------')
                                log.processLog('---------------------------------------------------')
                                print('Crawling failed with error:', str(e))
                                log.processLog(f'Crawling failed with error:{str(e)}')
                                print('Failed:', tds[2])
                                log.processLog(f'Failed:{tds[2]}')
                                print('The news page is connected without required information.')
                                log.processLog('The news page is connected without required information.')
                                print('---------------------------------------------------')
                                log.processLog('---------------------------------------------------')
                                # Save empty price news info
                                empty_price_df.loc[empty_price_count] = [*tds[:-1]]
                                empty_price_count += 1

                        else:
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            print('Failed:', tds[2])
                            log.processLog(f'Failed:{tds[2]}')
                            print('The news page is connected without required information.')
                            log.processLog('The news page is connected without required information.')
                            print('---------------------------------------------------')
                            log.processLog('---------------------------------------------------')
                            # Save empty price news info
                            empty_price_df.loc[empty_price_count] = [*tds[:-1]]
                            empty_price_count += 1
                        break
        # Returned index page HTML is empty or does not contain required info
        else:
            print('---------------------------------------------------')
            log.processLog('---------------------------------------------------')
            print('The index page is connected without required information.')
            log.processLog('The index page is connected without required information.')
            print('---------------------------------------------------')
            log.processLog('---------------------------------------------------')
            break

        # Save transfer news info
        # Write empty transfer news info to CSV sorted by release date if any
        if not empty_transfer_df.empty:
            empty_transfer_df.sort_values(by=['申報日期'],ascending=False,inplace=True)
            empty_transfer_path = os.path.join(output_dir_path, 'transfer_empty_' +Date.strftime('%Y-%m-%d') + '.csv')
            
            if not os.path.isfile(empty_transfer_path):
                empty_transfer_df.to_csv(empty_transfer_path,encoding='utf-8-sig',index=False)
            else:
                empty_transfer_df.to_csv(empty_transfer_path,mode='a',header=False,encoding='utf-8-sig',index=False)

        # Write invalid transfer news info to CSV sorted by release date if any
        if not invalid_transfer_df.empty:
            invalid_transfer_df.sort_values(by=['申報日期'],ascending=False,inplace=True)
            invalid_transfer_path = os.path.join(output_dir_path, 'transfer_invalid_' +Date.strftime('%Y-%m-%d') + '.csv')
            
            if not os.path.isfile(invalid_transfer_path):
                invalid_transfer_df.to_csv(invalid_transfer_path,encoding='utf-8-sig',index=False)
            else:
                invalid_transfer_df.to_csv(invalid_transfer_path, mode='a', header=False,encoding='utf-8-sig',index=False)

        # Save price news info
        # Write empty price news info to CSV sorted by release date if any
        if not empty_price_df.empty:
            empty_price_df.sort_values(by=['申報日期'],ascending=False,inplace=True)
            empty_price_path = os.path.join(output_dir_path,'price_empty_' + Date.strftime('%Y-%m-%d') + '.csv')
            
            if not os.path.isfile(empty_price_path):
                empty_price_df.to_csv(empty_price_path,encoding='utf-8-sig',index=False)
            else:
                empty_price_df.to_csv(empty_price_path,mode='a',header=False,encoding='utf-8-sig',index=False)

        # Write invalid price news info to CSV sorted by release date if any
        if not invalid_price_df.empty:
            invalid_price_df.sort_values(by=['申報日期'],ascending=False,inplace=True)
            invalid_price_path = os.path.join(output_dir_path, 'price_invalid_' + Date.strftime('%Y-%m-%d') + '.csv')
            
            if not os.path.isfile(invalid_price_path):
                invalid_price_df.to_csv(invalid_price_path,encoding='utf-8-sig',index=False)

            else:
                invalid_price_df.to_csv(invalid_price_path,mode='a',header=False,encoding='utf-8-sig',index=False)

        print('--------------------------------------')
        print('Crawling finished.')
        print('Number of news retrieved: %d' % (news_count))
        print('Number of empty transfer news: %d' %
              (len(empty_transfer_df.index)))
        print('Number of invalid transfer news: %d' %
              (len(invalid_transfer_df.index)))
        print('Number of empty price news: %d' % (len(empty_price_df.index)))
        print('Number of invalid price news: %d' %
              (len(invalid_price_df.index)))
        
        log.processLog('===================================================')
        log.processLog('Crawling finished.')
        log.processLog('Number of news retrieved: %d' % (news_count))
        log.processLog('Number of empty transfer news: %d' %
              (len(empty_transfer_df.index)))
        log.processLog('Number of invalid transfer news: %d' %
              (len(invalid_transfer_df.index)))
        log.processLog('Number of empty price news: %d' % (len(empty_price_df.index)))
        log.processLog('Number of invalid price news: %d' %
              (len(invalid_price_df.index)))
        break
    end_time = datetime.now()
    time_diff = end_time - start_time
    print('Execution time:', str(time_diff))
    print('===================================================')
    log.processLog(f'Execution time:{str(time_diff)}')
    log.processLog('===================================================')


if __name__ == '__main__':

    #路徑控制
    rootPath = json.load(open('set.json','r+'))['output_dir_path']
    output_dir_path = f'{rootPath}/News_CB'
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    
    #日期預設
    Date = datetime.now() - timedelta(days=1) #預設爬取昨天的新聞

    log.processLog('【開始執行CB_News爬蟲專案】 CB_News_Crawler.py')
    log.processLog(f"本次根路徑：{rootPath}")
    start = time.perf_counter_ns()
    try:

        #參數呼叫方式
        parser = argparse.ArgumentParser(description='目標：下載公開資訊觀測站-公司債公告彙總表 \
            \n網址：https://mops.twse.com.tw/mops/web/t108sb08_1_q2\
            \nOptional you can download data for a specific time range.\
            \nDefault crawler date is your execute date - 1 \
            \nExamples: python3 CB_News_Crawler.py -st 2021/06/01 -et 2021/06/30 ', formatter_class=RawTextHelpFormatter)


        parser.add_argument('-st', '--start', action='store', dest='startDate', type=str,
                            help='enter startDate: YYYY/mm/dd', default=Date.strftime('%Y/%m/%d'))

        parser.add_argument('-et', '--end', action='store', dest='endDate', type=str,
                            help='enter endDate: YYYY/mm/dd', default=Date.strftime('%Y/%m/%d'))

        args = parser.parse_args()
        dates = pd.date_range(start=args.startDate, end=args.endDate)

        log.processLog(f"本次查詢日期：{args.startDate} ~ {args.endDate}")

        # Variables for connection
        # Maximum number of reconnection attempts
        max_attempt = 10
        # Timeout in second
        timeout = 6

        for date_temp in dates:

            TWSE_URL = 'https://mops.twse.com.tw'
            INDEX_URL = TWSE_URL + '/mops/web/ajax_t108sb08_1'
            headers = {
                'Content-Type':
                'application/x-www-form-urlencoded',
                'Origin':TWSE_URL,
                'Referer':TWSE_URL + '/mops/web/t108sb08_1_q2',
                'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
            }
            data = {
                'encodeURIComponent': '1',
                'run': 'Y',
                'step': '1',
                'firstin': 'true',
                'TYPEK': 'pub'  # 市場別,pub公開發行、sii上市、otc上櫃、rotc興櫃
            }

            DDate = date_temp.strftime("%Y-%m-%d")
            main(DDate)

        end = time.perf_counter_ns()
        log.processLog(f'【結束程序】 CB_News_Crawler.py - 總執行時間:{(end-start)/10**9}')
        log.processLog('==============================================================================================')

    except Exception as e:
        log.processLog('發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())

