# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 04:25:41 2020
Last Modified：10/19

@author: ATM、Denver Liu

"""
import os
import sys
sys.path.append('..')
import log_manager as log
#import twse_mops_crawler
import pandas as pd
from datetime import date
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime,timedelta
from time import sleep
import json

end = datetime.now() - timedelta(days=1)
start = datetime.now() - timedelta(days=30) #日後可以統一爬取昨天就好，不要這樣爬

parser = argparse.ArgumentParser(description=
        '目標：下載公開資訊觀測站-每日重大訊息/歷史重大訊息 \
        \nOptional you can download data for a specific time range. (Required Now)\
        \n \
        \nExamples: python3 news_crawler.py -st 06/01/2021 -et 06/04/2021 '
        , formatter_class=RawTextHelpFormatter)

parser.add_argument('-st','--start', action='store', dest='startDate', type=str, 
    help='enter startDate: YYYY/mm/dd',default=start.strftime('%Y/%m/%d'))

parser.add_argument('-et','--end', action='store', dest='endDate', type=str,
    help='enter endDate: YYYY/mm/d',default=end.strftime('%Y/%m/%d'))

args = parser.parse_args()


if __name__ == '__main__':

    #路徑設定 (需手動更新的地方)
    #只要決定"根路徑"即可
    output_dir_path_dict = json.load(open(r'../set.json','r+'))
    output_dir_path = '../' + output_dir_path_dict['output_dir_path']
    
    #路徑設定 (不用更動這裡)
    output_dir_path = os.path.join(output_dir_path,'News_Stocks')

    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    dates=pd.date_range(start=args.startDate, end=args.endDate)

    #得到運行環境
    platform = sys.platform

    for date_temp in dates:
        date=date_temp.strftime("%Y %m %d")
        if platform == 'win32':
            os.system('python twse_mops_crawler.py {date} {path}'.format(date=date,path=output_dir_path)) #windows
        elif platform == 'linux':
            os.system('python3 twse_mops_crawler.py {date} {path}'.format(date=date,path=output_dir_path)) #linux
        else:
            log.processLog(f"本次執行環境為：{platform},非win32 or linux，故程序終止")
            print(f"本次執行環境為：{platform},非win32 or linux，故程序終止")
        sleep(2)
