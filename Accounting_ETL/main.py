#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import time


from utils.log2record import log2record
from utils.record2IncomeAndInventoryDetail import record2IncomeAndInventoryDetail
from utils.InventoryDetail2Inventory import InventoryDetail2Inventory

from SinoPac_preprocess import SinoPac_Future_preprocess, SinoPac_Stock_preprocess


# run_day函數: 將 昨日庫存明細報表+今日委託明細 轉換成 今日的四張報表。
def run_day(logpath, basepath = None):
    # 擷取logpath(委託明細的路徑)中的類別(Stocks or Futures)，然後存在logtype變數
    if "Stocks" in logpath:
        logtype = "Stocks"
    elif "Futures" in logpath:
        logtype = "Futures"
        
    # 擷取logpath(委託明細的路徑)中的日期(ex. YYYY-MM-DD)，然後存在date變數
    date = logpath.split("_")[-1].replace(".log", "") 
    
    # 讀取昨日庫存明細報表(base_df) ps.若希望以某一天的委託明細做為全新的基準點，則可讓basepath為None(無昨日庫存明細報表)
    base_df = pd.DataFrame()
    if basepath != None:
        try:
            base_df = pd.read_csv(basepath, encoding = "ms950").astype({"商品代碼": str}) # 股票的商品代碼可能會被pandas讀取成int，故轉成str
        except UnicodeDecodeError:
            base_df = pd.read_csv(basepath, encoding = "utf-8-sig").astype({"商品代碼": str}) # 股票的商品代碼可能會被pandas讀取成int，故轉成str
    
    # 重點: 今日委託明細(logpath) -> 今日成交報表(Record_df)
    Record_df = log2record(logpath, logtype, base_df)
    
    # 重點: 昨日庫存明細報表(base_df) + 今日成交報表(Record_df) -> 今日損益報表(Income_df) & 今日庫存明細報表(InventoryDetail_df)
    Income_df, InventoryDetail_df = record2IncomeAndInventoryDetail(Record_df, logtype, base_df, date)
    
    # 重點: 今日庫3存明細報表(InventoryDetail_df) -> 今日庫存合併報表(Inventory_df)
    Inventory_df = InventoryDetail2Inventory(InventoryDetail_df, logtype, date)

    
    # 將四張報表存成csv檔
    import os
    if not os.path.exists(f'./output/{logtype}'):
        os.makedirs(f'./output/{logtype}')
    
    Record_df.to_csv("./output/{}/Trades_{}_{}.csv".format(logtype, logtype, date), encoding = "utf-8-sig", index=False)
    Income_df.to_csv("./output/{}/PnL_{}_{}.csv".format(logtype, logtype, date), encoding = "utf-8-sig", index=False)
    InventoryDetail_df.to_csv("./output/{}/Position_Detailed_{}_{}.csv".format(logtype, logtype, date), encoding = "utf-8-sig", index=False)
    Inventory_df.to_csv("./output/{}/Position_Consolidated_{}_{}.csv".format(logtype, logtype, date), encoding = "utf-8-sig", index=False)
    return Record_df, Income_df, InventoryDetail_df, Inventory_df


# run_period函數: 將 第0天券商庫存報表 以及 第1~N天的委託明細 放進「/Accounting/rawdata/Futures」或「/Accounting/rawdata/Stocks」資料夾中，並連點兩下main.py，即可執行程式。
def run_period(fileType):
    startTime = time.time()
    directory = "./rawdata/{}/".format(fileType)
    fileNames = os.listdir(directory)
    # logPaths為所有委託明細的路徑(list of string)
    logPaths = sorted([directory + fn for fn in fileNames if ".log" in fn])
    # csvPath為 券商提供的 庫存明細報表的路徑(string)
    csvPath = [directory + fn for fn in fileNames if ".csv" in fn and "(改)" not in fn][0]
    # csvPathNew為 標準化後的 庫存明細報表的路徑(string) ps. 此部分之後改由家銘負責
    if fileType == "Stocks":
        _, csvPathNew = SinoPac_Stock_preprocess(csvPath) # 永豐_{證券,期貨}庫存_2021-08-25.csv -> 永豐_{證券,期貨}庫存_2021-08-25(改).csv
        # csvPathNew = f'rawdata\Stocks\Position_Consolidated_Stocks_Sinopac_2022-01-17.csv'
    elif fileType == "Futures":
        _, csvPathNew = SinoPac_Future_preprocess(csvPath) # 同上
        # csvPathNew = f'rawdata\Futures\Position_Consolidated_Futures_Sinopac_2022-01-17.csv'
    count = 0
    for logPath in logPaths:
        if count == 0: # 重點: 第0天
            # print('Record_df, Income_df, InventoryDetail_df, Inventory_df = run_day("{}", "{}")'.format(logPath, csvPathNew))            
            run_day(logPath, csvPathNew)
        else: # 重點: 第1~N天
            # print('Record_df, Income_df, InventoryDetail_df, Inventory_df = run_day("{}", "{}")'.format(logPath, "./output/{}/Position_Detailed_{}_{}.csv".format(fileType, fileType, lastDate)))                        
            run_day(logPath, "./output/{}/Position_Detailed_{}_{}.csv".format(fileType, fileType, lastDate))
        lastDate = logPath.split("_")[-1].replace(".log", "")
        count += 1
        print("{} in progress... {}/{}, time spent: {}s".format(fileType, count, len(logPaths), round(time.time()-startTime, 2)))


run_period("Stocks")
run_period("Futures")
