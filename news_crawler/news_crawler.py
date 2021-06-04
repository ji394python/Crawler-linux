# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 04:25:41 2020

@author: ATM
"""
import os
#import twse_mops_crawler
import pandas as pd
from datetime import date

#today = date.today()
#os.system('python twse_mops_crawler.py {date} G:\News'.format(date=today.strftime("%Y %m %d")))

#
dates=pd.date_range(start='06/04/2021', end='06/04/2021')
for date_temp in dates:
    date=date_temp.strftime("%Y %m %d")
    os.system('python3 twse_mops_crawler.py {date} \\News'.format(date=date))
