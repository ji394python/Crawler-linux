# coding=utf-8
import urllib3
import os
import re
import requests
import sys
import traceback
from bs4 import BeautifulSoup
import time as t 
from datetime import datetime
import sys
sys.path.append(r'..')
import log_manager as log


def strToFile(file, text):
    print ('Output: ' + file)
    f = open(file, 'w+',encoding='utf-8-sig')
    f.write(text)
    f.close()

def strToAppend(file, text):
    #print 'output: ' + file
    f = open(file, 'a+')
    f.write(text + '\n')
    f.close()


if __name__ == '__main__':
    
    try:
        #計時開始
        start = t.perf_counter_ns()

        #日期設定
        today = datetime.now().strftime('%Y-%m-%d') 

        log.processLog('==============================================================================================')
        log.processLog(f'【開始執行臺灣證交所停券公告爬蟲專案】 {os.path.basename(__file__)}')

        # 輸入參數
        if len(sys.argv) < 4:
            print('Usage: python twse_mops_crawler.py <year> <month> <day> <output_folder>')
            print('       python twse_mops_crawler.py 2020 07 13 D:/output')
            exit(1)
        year = sys.argv[1]
        month = sys.argv[2]
        day = sys.argv[3]
        output_folder = '../../../NasHome/News_Stocks' #Default root
        dateCheck = f"{year}-{month}-{day}"
        
        if len(sys.argv) == 5:
            output_folder = sys.argv[4]
        
        output_dir_date_path = output_folder + '/{}-{}-{}'.format(year, month, day)
        log.processLog(f'本次查詢日期：{year}-{month}-{day}')
        year = str(int(year)-1911)

        #建立輸出資料夾
        if not os.path.exists(output_dir_date_path) :
            log.processLog(f'建立日期資料夾：{output_dir_date_path}')
            os.makedirs(output_dir_date_path)

        urllib3.disable_warnings()
        s = requests.Session()

        # 連線到首頁
        url = 'https://mops.twse.com.tw/mops/web/t05st02'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        }
        resp = s.get(url, headers = headers, verify = False)
        connect_count = 0
        while resp.status_code != 200:
            t.sleep(5)
            connect_count += 1
            log.processLog(f"===== 首頁連線：連線失敗(狀態碼：{resp.status_code})，嘗試第{connect_count}重連")
            resp = s.get(url, headers = headers, verify = False)
            if connect_count > 10:
                break
        log.processLog(f"===== 首頁連線：連線成功，網址：{url}")

        # 輸入日期搜尋新聞
        url = 'https://mops.twse.com.tw/mops/web/ajax_t05st02'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://mops.twse.com.tw',
            'Referer': 'https://mops.twse.com.tw/mops/web/t05st02',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        }
        data = {
            'encodeURIComponent': '1',
            'step': '1',
            'step00': '0',
            'firstin': '1',
            'off': '1',
            'TYPEK': 'all',
            'year': year,
            'month': month,
            'day': day
        }
        resp = s.post(url, headers = headers, data = data, verify = False)
        connect_count = 0
        while resp.status_code != 200:
            t.sleep(5)
            connect_count += 1
            log.processLog(f"===== 首頁查詢：查詢失敗(狀態碼：{resp.status_code})，嘗試第{connect_count}重連")
            resp = s.post(url, headers = headers, data = data, verify = False)
            if connect_count > 10:
                break
        log.processLog(f"===== 首頁查詢：查詢成功，網址：{url}、Header：{data}、data：{data}")

        soup = BeautifulSoup(resp.text.replace('</FONT>', ''), 'html.parser')
        tr_list = soup.find_all('tr', {'class': re.compile('odd|even')})
        Len = len(tr_list)
        print('# of news: {}'.format(Len)) #有幾則新聞
        log.processLog(f'===== 查詢結果: 本日共有{Len}筆資料')

        # 上方表格標題
        field = ['公司名稱', '公司代號', '發言日期', '發言時間', '主旨', '序號', '符合條款', '事實發生日', '說明']

        # 下方表格變數
        url2 = 'https://mops.twse.com.tw/mops/web/ajax_t59sb01'
        data2 = {
            'firstin': 'true',
            'co_id': '',
            'TYPEK': 'all',
            'YEAR': year,
            'MONTH': month,
            'SDAY': day,
            'EDAY': day,
            'DATE1': '',
            'SKEY': '',
            'step': '2b'
        }

        resp2 = None
        counterProcess = 0
        for tr in tr_list:
            input_list = tr.find_all('input')
            

            text = ''
            # 處理上方表格
            if len(input_list) == 10:
                log.processLog('----------------------------------------------------------------------------------')
                log.processLog(f'====== 查詢表格拆解Start：第{counterProcess}個開始，屬於上方表格')
                for i in range(len(input_list) - 1):
                    input_list[i] = input_list[i]['value']
                    if i == 4:
                        text += field[i] + '：' + input_list[i].replace('\r', '').replace('\n', '') + '\n'
                    elif i < len(input_list) - 2:
                        text += field[i] + '：' + input_list[i] + '\n'
                    else:
                        text += field[i] + '：' + '\n' + input_list[i] + '\n'
                day = datetime.strptime(input_list[2],'%Y%m%d').strftime('%Y-%m-%d')
                # if day == dateCheck: 
                #     log.processLog(f'====== 日期不對：非今日資料({day})，跳過此檔')
                #     continue
                path = output_dir_date_path + '/{}_{}_{}_{}.txt'.format(input_list[1], input_list[0].replace('*', ''), day, input_list[3])
                log.processLog(f'====== 查詢表格拆解End：第{counterProcess}個，Ticker：{input_list[1]}')
            # 處理下方表格
            else:
                log.processLog('----------------------------------------------------------------------------------')
                log.processLog(f'====== 查詢表格拆解Start：第{counterProcess}個開始，屬於下方表格')
                td_list = tr.find_all('td')
                for i in range(len(td_list)):
                    if i != 5:
                        td_list[i] = td_list[i].text.replace(' ', '')
                    else:
                        onclick = td_list[i].input['onclick']
                        onclick_array = onclick.split("'")
                        data2['SKEY'] = onclick_array[1]
                        data2['DATE1'] = onclick_array[3]
                        data2['co_id'] = onclick_array[5]

                day_array = td_list[0].split('/')
                day = '{}-{}-{}'.format(int(day_array[0])+1911, day_array[1], day_array[2])
                # if day == dateCheck: 
                #     log.processLog(f'====== 日期不對：非今日資料({day})，跳過此檔')
                #     continue
                time = td_list[1].replace(':', '')
                resp2 = s.post(url2, headers = headers, data = data2, verify = False)
                connect_count = 0
                while resp2.status_code != 200:
                    t.sleep(5)
                    connect_count += 1
                    log.processLog(f'====== 查詢表格拆解Start：下方表格個別查詢失敗(狀態碼：{resp.status_code})，嘗試第{connect_count}次重連')
                    resp2 = s.post(url2, headers = headers, data = data2, verify = False)
                    if connect_count > 10:
                        break
                log.processLog(f'====== 查詢表格拆解Start：下方表格個別查詢成功')
                soup2 = BeautifulSoup(resp2.text, 'html.parser')
                tr_list2 = soup2.find_all('tr')

                for tr2 in tr_list2:
                    if tr2.th != None and tr2.td != None:
                        th = tr2.th.text
                        td = tr2.td.text
                        if '主旨' in th: td = td.replace('\r', '').replace('\n', '')
                        text += th + '：' + td + '\n'
                    if tr2.a != None:
                        text += '其他：https://mops.twse.com.tw' + tr2.a['href']

                path = output_dir_date_path + '/{}_{}_{}_{}.txt'.format(td_list[2], td_list[3].replace('*', ''), day, time)
                log.processLog(f'====== 查詢表格拆解End：第{counterProcess}個，Ticker：{td_list[2]}')

            pathCheck = path[path.rfind('/')+1:]
        #    print(path,pathCheck,dateCheck)
            if pathCheck.split('_')[2] == dateCheck:
                strToFile(path, text)
                log.processLog(f'====== 查詢表格輸出：{path}')
            counterProcess += 1
        
        end = t.perf_counter_ns()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)/10**9}')
        log.processLog('==============================================================================================')

    except:
        end = t.perf_counter_ns()
        log.processLog(f'【程序錯誤】 {os.path.basename(__file__)} - 執行時間:{(end-start)/10**9}')
        log.processLog(f'【程序錯誤】：本次運行完成{counterProcess}筆目標,剩餘{Len-counterProcess-1}筆未爬')
        log.processLog(f'【程序錯誤】：錯誤詳情請查看errorlog')
        log.processLog('==============================================================================================')

        log.errorLog(traceback.format_exc())

