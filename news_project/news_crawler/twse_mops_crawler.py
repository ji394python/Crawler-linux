# coding=utf-8
import urllib3
import os
import re
import requests
import sys
import traceback
from bs4 import BeautifulSoup

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

    if os.path.exists('News') == False:
        os.mkdir('News')
    
    # 輸入參數
    if len(sys.argv) < 4:
        print('Usage: python twse_mops_crawler.py <year> <month> <day> <output_folder>')
        print('       python twse_mops_crawler.py 2020 07 13 D:/output')
        exit(1)
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
    output_folder = 'News'
    if len(sys.argv) == 5:
        output_folder = sys.argv[4]
    output_folder += '/{}{}{}'.format(year, month, day)
    year = str(int(year)-1911)

    # 建立輸出資料夾
    try:
        if os.path.isdir(output_folder):
            print('Folder exist: ' + output_folder)
        else:
            print('Create folder: ' + output_folder)
            os.mkdir(output_folder)
    except OSError:
        print ("Creation of the directory %s failed" % output_folder)
        traceback.print_exc()
        exit(1)

    urllib3.disable_warnings()
    s = requests.Session()

    # 連線到首頁
    url = 'https://mops.twse.com.tw/mops/web/t05st02'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    resp = s.get(url, headers = headers, verify = False)

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
    #strToFile('main.html', resp.text.encode('utf8'))
    soup = BeautifulSoup(resp.text.replace('</FONT>', ''), 'html.parser')
    tr_list = soup.find_all('tr', {'class': re.compile('odd|even')})
    print ('# of news: {}'.format(len(tr_list)))

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
    for tr in tr_list:
        input_list = tr.find_all('input')

        text = ''
        # 處理上方表格
        if len(input_list) == 10:
            for i in range(len(input_list) - 1):
                input_list[i] = input_list[i]['value']
                if i == 4:
                    text += field[i] + '：' + input_list[i].replace('\r', '').replace('\n', '') + '\n'
                elif i < len(input_list) - 2:
                    text += field[i] + '：' + input_list[i] + '\n'
                else:
                    text += field[i] + '：' + '\n' + input_list[i] + '\n'

            path = output_folder + '/{}_{}_{}_{}.txt'.format(input_list[1], input_list[0].replace('*', ''), input_list[2], input_list[3])

        # 處理下方表格
        else:
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
            day = '{}{}{}'.format(int(day_array[0])+1911, day_array[1], day_array[2])
            time = td_list[1].replace(':', '')
            resp2 = s.post(url2, headers = headers, data = data2, verify = False)
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

            path = output_folder + '/{}_{}_{}_{}.txt'.format(td_list[2], td_list[3].replace('*', ''), day, time)

        strToFile(path, text)

    # 關閉連線
    #resp2.close()
