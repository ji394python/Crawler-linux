# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 04:25:41 2020
Last Modified：06/04

@author: ATM、Denver Liu

"""
import os
#import twse_mops_crawler
import pandas as pd
from datetime import date
import argparse
from argparse import RawTextHelpFormatter

#today = date.today()
#os.system('python twse_mops_crawler.py {date} G:\News'.format(date=today.strftime("%Y %m %d")))

#

parser = argparse.ArgumentParser(description=
        '目標：下載公開資訊觀測站-每日重大訊息/歷史重大訊息 \
        \nOptional you can download data for a specific time range. (Required Now)\
        \n \
        \nExamples: python3 news_crawler.py -st 06/01/2021 -et 06/04/2021 '
        , formatter_class=RawTextHelpFormatter)

parser.add_argument('-st','--start', action='store', dest='startDate', type=str, 
    help='enter startDate: YYYY/mm/dd', required=True)

parser.add_argument('-et','--end', action='store', dest='endDate', type=str,
    help='enter endDate: YYYY/mm/d', required=True)

args = parser.parse_args()

dates=pd.date_range(start=args.startDate, end=args.endDate)


if os.path.exists('News') == False:
    os.mkdir('News')

for date_temp in dates:
    date=date_temp.strftime("%Y %m %d")
    os.system('python3 twse_mops_crawler.py {date} \\News'.format(date=date)) #linux 
    #os.system('python twse_mops_crawler.py {date} News'.format(date=date)) #windows
