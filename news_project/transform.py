# -*- coding: utf-8 -*-
"""
Author: Denver Liu

Last Updated: 2021/07/01

"""
import pandas as pd 
import log_manager as log
import time
import traceback
import os 

if __name__ == '__main__':

	#路徑設定 (需要手動改的地方)
	#只要決定"根路徑"即可
    output_dir_path = r'../../ShareDiskE'

	#路徑設定 (不用更動的地方)
    output_dir_path_News = os.path.join(output_dir_path,'News_Stocks')
    output_dir_path_print = os.path.join(output_dir_path,'News_Stocks','print')
    output_file_path_dataCsv = os.path.join(output_dir_path,'News_Stocks','print','data.csv')
    output_file_path_doneDate = os.path.join(output_dir_path,'News_Stocks','print','done_date.csv')


    #專為資料處理用的py檔，可延伸
    log.processLog('【開始執行News_Stocks爬蟲專案】 clean.py')

    try:
        
        start = time.perf_counter_ns() 
        
        df = pd.read_csv(output_file_path_dataCsv,encoding='utf-8-sig')
        # df.drop_duplicates(['股票代號', '股票名稱',
        #                     '停券起日', '停券迄日'],inplace=True,keep='last')
        df.sort_values('停券起日',inplace=True)
        df.to_csv(output_file_path_dataCsv,encoding='utf-8-sig',index=False)
        
        end = time.perf_counter_ns() 
        log.processLog(f'【結束程序】 parsing.py - 執行時間:{(end-start)/10**9}')
        
        log.processLog('==============================================================================================')

    except Exception as e:

        log.processLog('發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())