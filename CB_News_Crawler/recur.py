# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 04:25:41 2020
Last Modified：06/23

@author: ATM、Denver Liu

"""

import os
#import twse_mops_crawler
import pandas as pd
from datetime import date
import argparse
from argparse import RawTextHelpFormatter
from datetime import datetime,timedelta

Date = datetime.now() - timedelta(days=1)

parser = argparse.ArgumentParser(description=
        '目標：下載公開資訊觀測站-公司債公告彙總表 \
        \n網址：https://mops.twse.com.tw/mops/web/t108sb08_1_q2\
        \nOptional you can download data for a specific time range.\
        \nDefault crawler date is your execute date - 1 \
        \nExamples: python3 recur.py -st 06/01/2021 -et 06/04/2021 '
        , formatter_class=RawTextHelpFormatter)

parser.add_argument('-st','--start', action='store', dest='startDate', type=str, 
    help='enter startDate: YYYY-mm-dd',default=Date.strftime('%Y-%m-%d'))

parser.add_argument('-et','--end', action='store', dest='endDate', type=str,
    help='enter endDate: YYYY-mm-dd',default=Date.strftime('%Y-%m-%d'))

args = parser.parse_args()

dates=pd.date_range(start=args.startDate, end=args.endDate)

for date_temp in dates:
    date=date_temp.strftime("%Y-%m-%d")
    #os.system('python3 CB_News_Crawler.py {date}'.format(date=date)) #linux 
    os.system('python CB_News_Crawler.py {date}'.format(date=date)) #windows
