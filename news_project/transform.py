# -*- coding: utf-8 -*-
"""
Author: Denver Liu

Last Updated: 2021/07/01

"""
import pandas as pd 
import log_manager as log
import time
import traceback

if __name__ == '__main__':

    #專為資料處理用的py檔，可延伸
    #目前處理更正年度與同時發生的列重複問題
    log.processLog('【開始執行News_Stocks爬蟲專案】 clean.py')

    try:
        
        start = time.perf_counter_ns() 

        file = '../../ShareDiskE/News_Stocks/print/data.csv'
        df = pd.read_csv(file,encoding='utf-8-sig')
        df.drop_duplicates(['0.股票代號', '1.股票名稱',
                            '2.停券起日', '3.停券迄日',
                            '5.發言日期'],inplace=True,keep='last')
        df.to_csv('../../ShareDiskE/News_Stocks/print/data.csv',encoding='utf-8-sig',index=False)
        
        end = time.perf_counter_ns() 
        log.processLog(f'【結束程序】 parsing.py - 執行時間:{(end-start)/10**9}')
        
        log.processLog('==============================================================================================')

    except Exception as e:

        log.processLog('發生錯誤:請查看error.log')
        print(traceback.print_exc())
        log.errorLog(traceback.format_exc())