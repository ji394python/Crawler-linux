# coding=utf-8
# Usage: python twse_mops_today_crawler.py <output_folder>
#        python twse_mops_today_crawler.py D:\output
import urllib3
import os
import re
import requests
import sys
import time
from bs4 import BeautifulSoup
from time import gmtime, strftime

def strToFile(file, text):
    print ('Output: ' + file)
    f = open(file, 'w')
    f.write(text)
    f.close()

if __name__ == '__main__':
    # 輸入參數
    output_folder = 'News'
    today = strftime("%Y%m%d", gmtime())
    if len(sys.argv) == 2:
        output_folder = sys.argv[1]
    output_folder += '/'+ today

    # 建立輸出資料夾
    try:
        if os.path.isdir(output_folder):
            print('Folder exist: ' + output_folder)
        else:
            print('Create folder: ' + output_folder)
            os.mkdir(output_folder)
    except OSError:
        print ("Creation of the directory %s failed" % output_folder)
        exit(1)

    urllib3.disable_warnings()
    s = requests.Session()

    # 連線到首頁
    url = 'https://mops.twse.com.tw/mops/web/t05sr01_1'
    headers = {
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    resp = s.get(url, headers = headers, verify = False)
    #strToFile('main.html', resp.text.encode('utf8'))
    soup = BeautifulSoup(resp.text.replace('</FONT>', ''), 'html.parser')
    tr_list = soup.find_all('tr', {'class': re.compile('odd|even')})
    print ('# of news: {}'.format(len(tr_list)))

    # 上方表格標題
    field = ['序號', '發言日期', '發言時間', '發言人', '發言人職稱', '發言人電話', '主旨', '符合條款', '事實發生日', '說明']

    # 上方表格變數
    url2 = 'https://mops.twse.com.tw/mops/web/ajax_t05sr01_1'
    headers2 = {
        'Referer': 'https://mops.twse.com.tw/mops/web/t05sr01_1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    data2 = {
        'TYPEK': 'all',
        'step': '1',
        'skey': '',
        'hhc_co_name': '',
        'firstin': 'true',
        'COMPANY_ID': '',
        'COMPANY_NAME': '',
        'SPOKE_DATE': '',
        'SPOKE_TIME': '',
        'SEQ_NO': ''
    }
    resp2 = None

    # 下方表格變數
    url3 = 'https://mops.twse.com.tw/mops/web/ajax_t59sb01'
    headers3 = {
        'Referer': 'https://mops.twse.com.tw/mops/web/t05sr01_1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    data3 = {
        'firstin': 'true',
        'co_id': '',
        'colorchg': '',
        'TYPEK': 'all',
        'YEAR': str(int(today[0:4])-1911),
        'MONTH': today[4:6],
        'SDAY': today,
        'EDAY': today,
        'DATE1': '',
        'SKEY': '',
        'step': '2b'
    }
    resp3 = None

    path = ''
    for tr in tr_list:
        td_list = tr.find_all('td')
        COMPANY_ID = td_list[0].text
        COMPANY_NAME = td_list[1].text.replace('*', '')
        SPOKE_TIME = td_list[3].text.replace(':', '')
        text = '公司名稱：' + COMPANY_NAME + '\n'

        input = tr.find('input')
        onclick = input['onclick']
        onclick_array = onclick.split("'")

        if len(onclick_array) == 13:
            text += '公司代號：' + COMPANY_ID + '\n'
            data2['SEQ_NO'] = onclick_array[1]
            data2['SPOKE_TIME'] = onclick_array[3]
            data2['SPOKE_DATE'] = onclick_array[5]
            data2['COMPANY_ID'] = onclick_array[7]
            data2['skey'] = onclick_array[9]
            resp2 = s.post(url2, headers = headers2, data = data2, verify = False)
            soup2 = BeautifulSoup(resp2.text, 'html.parser')
            td_list = soup2.find_all('td', {'class': 'odd'})
            for i in range(len(td_list)):
                value = td_list[i].text
                if i == 6: value = value.replace('\r', '').replace('\n', '')
                elif i == 9: value = '\n' + value
                text += field[i] + '：' + value + '\n'

            path = output_folder + '/{}_{}_{}_{}.txt'.format(COMPANY_ID, COMPANY_NAME, data2['SPOKE_DATE'], SPOKE_TIME)

        elif len(onclick_array) == 11:
            data3['co_id'] = onclick_array[3]
            data3['SKEY'] = onclick_array[5]
            data3['DATE1'] = onclick_array[7]
            resp3 = s.post(url3, headers = headers3, data = data3, verify = False)
            soup3 = BeautifulSoup(resp3.text, 'html.parser')
            tr_list3 = soup3.find_all('tr')
            for tr3 in tr_list3:
                if tr3.th != None and tr3.td != None:
                    th = tr3.th.text
                    td = tr3.td.text
                    if '主旨' in th: td = td.replace('\r', '').replace('\n', '')
                    text += th + '：' + td + '\n'
                if tr3.a != None:
                    text += '其他：https://mops.twse.com.tw' + tr3.a['href']

            path = output_folder + '/{}_{}_{}_{}.txt'.format(COMPANY_ID, COMPANY_NAME, data3['DATE1'], SPOKE_TIME)

        strToFile(path, text)
        time.sleep(0.8)

    # 關閉連線
    resp.close()
    resp2.close()
    if resp3 != None: resp3.close()
