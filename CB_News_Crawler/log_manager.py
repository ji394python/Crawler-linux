# -*- coding: utf-8 -*-
# log紀錄使用
"""
@author: Denver Liu

Last Modified：07/10

"""

# Author : Denver Liu
import os
import time


def errorLog(exe_status: str):
    # log_path為路徑，判斷是否有，若無會自動創建資夾
    log_path = 'log'
    if not os.path.exists(log_path):
        os.mkdir(log_path)
        log_file = open(f'log/ERROR.log', mode='a', encoding='utf-8')
        record_time = time.strftime("%Y%m%d %H:%M:%S")
        log_file.write('Process:'+str(__file__)+'\n')
        log_file.write('Execute time:'+record_time+'\n')
        log_file.write('---------------------------------------'+'\n')
        log_file.write(record_time+"："+str(exe_status)+'\n')
    else:
        record_time = time.strftime("%Y%m%d %H:%M:%S")
        # 開啟並寫入ERROR.log，若無會自動創建
        log_file = open(f'log/ERROR.log', mode='a+', encoding='utf-8')
        log_file.write(record_time+"："+str(exe_status)+'\n')

# Author : Denver Liu


def processLog(exe_string: str):

    # log_path為路徑，判斷是否有，若無會自動創建資夾
    log_path = 'log'
    if not os.path.exists(log_path):
        os.mkdir(log_path)
        record_time = time.strftime("%Y%m%d %H:%M:%S")
        log_file = open(f'log/Process.log', mode='a+', encoding='utf-8')
        log_file.write('Process:'+str(__file__)+'\n')
        log_file.write('Execute time:'+record_time+'\n')
        log_file.write('---------------------------------------'+'\n')
        log_file.write(record_time+"："+str(exe_string)+'\n')
    else:
        record_time = time.strftime("%Y%m%d %H:%M:%S")
        # 開啟並寫入Process.log，若無會自動創建
        log_file = open(f'log/Process.log', mode='a+', encoding='utf-8')
        log_file.write(record_time+"："+str(exe_string)+'\n')
