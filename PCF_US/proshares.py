#LOG、斷線重連、測試read_url
#採邊觀察邊修改的開發方式

import requests
import numpy as np
import pandas as pd
from io import StringIO
import time
import os
import csv
from datetime import datetime
import pathlib
from re import findall
from traceback import format_exc
import log_manager as log
import json

def strip(text):
    return text.replace('\r', '').replace('\n', '').replace('              ', ' ').replace('    ', ' ').replace('  ', '').strip().replace(',', '')


if __name__ == '__main__':
    
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US成份表爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    #路徑設定
    in_path = str(pathlib.Path(__file__).parent.absolute())
    tail_path = json.load(open(f"{in_path}/path.json",'r'))
    tail_path = tail_path['NasPublic']
    
    try:
        # import product list
        df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
        df_products.set_index( keys=['type','code'],inplace=True)
        stock_list = list(df_products.loc["E"].index)
        future_list = list(df_products.loc["F"].index)
        swap_list = list(df_products.loc["S"].index)
        log.processLog(f'[proshares.py] 讀取前置檔案：{in_path + "/product_code.csv"}')



        monthDict = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
        toSymbol = {'IPATH SERIES-B S&P 500 VIX SHT-TERM FUT SWAP - GS': 'VXX', 'CBOE VIX FUTURE Jan21': 'VXF1', 'CBOE VIX FUTURE Feb21': 'VXG1', 'CBOE VIX FUTURE Mar21': 'VXH1', 'CBOE VIX FUTURE Apr21': 'VXJ1', 'CBOE VIX FUTURE May21': 'VXK1', 'CBOE VIX FUTURE Jun21': 'VXM1', 'CBOE VIX FUTURE Jul21': 'VXN1', 'CBOE VIX FUTURE Aug21': 'VXQ1', 'CBOE VIX FUTURE Sep21': 'VXU1', 'CBOE VIX FUTURE Oct21': 'VXV1', 'CBOE VIX FUTURE Nov21': 'VXX1', 'CBOE VIX FUTURE Dec21': 'VXZ1'}
        Ticker = ['TOLZ', 'EMTY', 'EQRR', 'PEX', 'HDG', 'HYHG', 'RINF', 'IGHG', 'OILK', 'CSM', 'CLIX', 'EFAD', 'EMDV', 'EUDV', 'ANEW', 'FUT', 'MRGR', 'ALTS', 'ONLN', 'PAWZ', 'RALS', 'SMDV', 'TMDV', 'SPXB', 'NOBL', 'SPXE', 'SPXN', 'SPXV', 'SPXT', 'REGL', 'TDV', 'TBF', 'TBX', 'SBM', 'DOG', 'EUFX', 'YXI', 'SEF', 'SJB', 'EFZ', 'EUM', 'MYY', 'DDG', 'PSQ', 'REK', 'RWM', 'SH', 'SBB', 'EMSH', 'SVXY', 'UBT', 'UST', 'UYM', 'UCO', 'BOIL', 'UGE', 'UCC', 'DDM', 'ULE', 'XPP', 'UPV', 'UYG', 'UGL', 'RXL', 'UJB',
                'UXI', 'UBR', 'EFO', 'EET', 'EZJ', 'MVV', 'BIB', 'SKYU', 'UCYB', 'DIG', 'QLD', 'URE', 'UWM', 'SSO', 'USD', 'AGQ', 'SAA', 'ROM', 'LTL', 'UPW', 'UVXY', 'YCL', 'UDOW', 'UMDD', 'TQQQ', 'URTY', 'UPRO', 'TTT', 'SDOW', 'SMDD', 'SQQQ', 'SRTY', 'SPXU', 'TBT', 'PST', 'CROC', 'SMN', 'SCO', 'KOLD', 'SZK', 'SCC', 'DXD', 'EUO', 'FXP', 'EPV', 'SKF', 'GLL', 'RXD', 'SIJ', 'BZQ', 'EFU', 'EEV', 'EWV', 'MZZ', 'BIS', 'DUG', 'QID', 'SRS', 'TWM', 'SDS', 'SSG', 'ZSL', 'SDD', 'REW', 'SDP', 'YCS', 'VIXM', 'VIXY','BITO']
        
        print('[proshares.py] 已設定本次需爬取Ticker')
        log.processLog(f'[proshares.py] 已設定本次需爬取Tickers，數量：{len(Ticker)}')
        countN = 0
        for ticker in Ticker:
            try:
                countN = countN+1
                check = False
                count = 0
                log.processLog(f'[proshares.py] {countN}/{len(Ticker)}... 開始爬取{ticker}')
                print(f'[proshares.py] {countN}/{len(Ticker)}... 開始爬取{ticker}')
                while(1):
                    try:
                        count = count+1
                        #historical data (歷史淨值檔)
                        url1 = f'https://accounts.profunds.com/etfdata/ByFund/{ticker}-historical_nav.csv'
                        resp1 = requests.get(url1)
                        ddf1 = pd.read_csv(StringIO(resp1.text), sep=',')
                        Nav = ddf1.iloc[0, 3]
                        Shares = ddf1.iloc[0, 7] #在外流通股數
                        Date = datetime.strptime(ddf1.Date[0], '%m/%d/%Y').strftime("%Y-%m-%d") #取得最後一天

                        #holdings
                        url = 'https://accounts.profunds.com/etfdata/ByFund/' + ticker + '-psdlyhld.csv'
                        resp = requests.get(url)
                        break
                    except Exception as e:
                        print(f'[proshares.py] {ticker}：淨值檔/成分表爬取第{count}次連線失敗...5秒後重試')
                        log.processLog(f'[proshares.py] {ticker}：淨值檔/成分表爬取第{count}次連線失敗...5秒後重試')
                        time.sleep(5)
                        continue
                
                log.processLog(f'[proshares.py] {ticker}：淨值檔/成分表爬取完成...開始進行成分表處理程序')
                print(f'[proshares.py] {ticker}：淨值檔/成分表爬取完成...開始進行成分表處理程序')
                
                daily_data = resp.content.splitlines()
                daily_array = np.reshape(
                    np.array(daily_data[3].decode().split(",")), (1, -1)) #取得欄位名稱
                
                for j in range(4, len(daily_data)): #從第4欄開始才是資料
                    raw_data = np.array(daily_data[j].decode().split(","))
                    raw_data = raw_data[:-1]
                    raw_data = np.reshape(raw_data, (1, -1))
                    daily_array = np.row_stack((daily_array, raw_data)) #一種row bind的方式
                df = pd.DataFrame(daily_array[1:], columns=daily_array[0].tolist(), index = None)
                def f(x):
                    return x.apply(lambda y:(y.strip("\"")))
                df = df.apply(lambda x:f(x))
                df = df.replace('', np.nan)

                df.columns = [i.strip(" ") for i in df.columns ]

                path = f"{tail_path}/{Date}/proshares/"
                if not os.path.exists(f"{path}"):
                    log.processLog(f'[proshares.py] 尚無本日資料夾，建立：{path}')
                    os.makedirs(path)
                    
                #Fund Ticker：ETF縮寫
                #Fund Name：ETF全名
                df = df.rename(columns={'Security Ticker':'Ticker', 'Security Description':'Name', 'Shares/Contracts':'Shares'})
                FundName = df['Fund Name'][0]

                mask = df.loc[:, 'Market Value'].isna()
                df.loc[mask, 'Market Value'] = df.loc[mask, 'Exposure Value (Notional + G/L)'] #補植，應該原先就有遇過這種狀況

                df = df.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price', 'Multiplier', 'FX Rate']    )
                df.loc[:, "Market Value"] = df.loc[:, "Market Value"].astype('float')
                df.loc[:, "Shares"]  = df.loc[:, "Shares"].astype('float')

                df.loc[:, 'Price'] = df.loc[:, "Market Value"] / df.loc[:, "Shares"]
                df.loc[:, 'Weight'] = df.loc[:, 'Market Value'] / df.loc[:, 'Market Value'].sum()
                df.insert(loc=4, column='Expiry_month',value=['' for i in range(df.shape[0])]) #新增空欄位 這用法還不錯

                path =  f"{tail_path}/{Date}/proshares/{ticker}_PCF_N_{Date}.csv"
                with open(path, "w",encoding='utf-8-sig', newline='') as csvfile:    
                    #這邊本來就會自主空一行還蠻奇怪的
                    spamwriter = csv.writer(csvfile, delimiter=',') 
                    spamwriter.writerow(['Date', Date])
                    spamwriter.writerow(['Name', FundName])
                    spamwriter.writerow(['Net Asset', Nav * Shares * 1000])
                    spamwriter.writerow(['Shares Outstanding', Shares * 1000])
                    spamwriter.writerow(['ETF Currency', 'USD'])
                    spamwriter.writerow(['Nav per Share', Nav])
                    spamwriter.writerow([])
                
                for value in df.values:
                    if value[0] not in stock_list and value[0] not in future_list:
                        if '(' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = value[1].split('(')[1].split(')')[0]+' F/S' #情況一、我猜是面對債券
                        elif 'WTI CRUDE FUTURE' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = df_products.loc[df_products['productname'].str.contains('WTI CRUDE FUTURE')]['productid'].values[0]+' F/S'
                        elif 'CBOE VIX FUTURE' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = df_products.loc[df_products['productname'].str.contains('CBOE VIX FUTURE')]['productid'].values[0]+' F/S'
                        elif 'SILVER FUTURE' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = df_products.loc[df_products['productname'].str.contains('SILVER FUTURE')]['productid'].values[0]+' F/S'
                        elif 'NATURAL GAS FUTR' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = df_products.loc[df_products['productname'].str.contains('NATURAL GAS FUTR')]['productid'].values[0]+' F/S'
                        elif 'GOLD 100 OZ' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = df_products.loc[df_products['productname'].str.contains('GOLD 100 OZ')]['productid'].values[0]+' F/S'
                        df = df[df['Name']!='Net Other Assets / Cash']

                        
                for value in df.values:
                    if pd.notnull(value[0]):
                        if (value[0].split()[0][:-2] in future_list or value[0].split()[0].strip() in future_list) and value[0] not in swap_list and 'F/S' in value[0]:
                            if len(value[0].split()[0]) > 2:
                                code = value[0].split()[0][:-2]
                            else:
                                code = value[0].split()[0].strip()
                            try:
                                if '(' in value[1]:
                                    expiry_month = datetime.strptime(value[1].split('(')[0].strip().split()[-1],'%m/%d/%y').strftime('%Y%m%d')
                                else:
                                    expiry_month = datetime.strptime(value[1].split()[-1],'%b%y').strftime('%Y%m')
                            except:
                                try:
                                    expiry_month = datetime.strptime(value[1].split('(')[0].strip().split()[-1],'%m/%d/%Y').strftime('%Y%m%d')
                                except Exception as e:
                                    print(e)
                            current = df_products.loc['F'].loc[code]
                            pos2 = df['Ticker']==value[0]
                            df.loc[pos2,'Ticker'] = current[0]
                            df.loc[pos2,'Currency'] = current[3]
                            df.loc[pos2,'Multiplier'] = current[4]
                            df.loc[pos2,'Expiry_month'] = expiry_month
                            if current[3]=='USD':
                                df.loc[pos2,'FX Rate'] = '1.0000'
                        elif value[0].split()[0].strip() in swap_list and 'F/S' in value[0]:
                            current = df_products.loc['S'].loc[value[0].split()[0].strip()]
                            pos2 = df['Ticker']==value[0]
                            df.loc[pos2,'Ticker'] = current[0]
                            df.loc[pos2,'Currency'] = current[3]
                            df.loc[pos2,'Multiplier'] = current[4]
                            if current[3]=='USD':
                                df.loc[pos2,'FX Rate'] = '1.0000'
                        elif value[0] in stock_list:
                            current = df_products.loc['E'].loc[value[0]]
                            pos2 = df['Ticker']==value[0]
                            df.loc[pos2,'Ticker'] = current[0]
                            df.loc[pos2,'Currency'] = current[3]
                            df.loc[pos2,'Multiplier'] = current[4]
                            if current[3]=='USD':
                                df.loc[pos2,'FX Rate'] = '1.0000'
                    else:
                        if 'TREASURY BILL' in value[1] or 'TRSRY' in value[1] or 'CASH' in value[1]:
                            pos = df['Name']==value[1]
                            df.loc[pos,'Ticker'] = 'Cash'
                            df.loc[pos,'Currency'] = 'USD'
                            df.loc[pos,'Multiplier'] = '1'
                            df.loc[pos,'FX Rate'] = '1.0000'
                
                df.to_csv(path, mode = 'a', index = False)
                log.processLog(f'Output:{path}')
                log.processLog(f'[proshares.py] {ticker}：本支ETF處理完畢')
                log.processLog(f'---------------------')
            except Exception as e:
                log.processLog(f'[proshares.py] {ticker}：本支ETF處理遇到未知問題，請查看錯誤log檔，本處先從下一支繼續開始')
                log.processLog(f'---------------------')
                log.errorLog(f'{format_exc()}')
                print(e)
            
        end = time.perf_counter()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
        log.processLog('==============================================================================================')
    except:
        log.processLog(f'[proshares.py] {ticker}：本次爬蟲出現嚴重錯誤，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(ticker,"---------------------")