from logging import exception
import requests
import json
import pandas as pd
import csv
import os
import requests
from datetime import datetime
from io import StringIO
import requests
import pathlib
import time
from traceback import print_exc,format_exc
import log_manager as log

def strip(text):
    return text.replace('\r', '').replace('\n', '').replace('              ', ' ').replace('    ', ' ').replace('  ', '').strip().replace(',', '')


if __name__ == '__main__':
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US成份表爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    try:
        #路徑設定
        in_path = str(pathlib.Path(__file__).parent.absolute())
        tail_path = json.load(open(f"{in_path}/path.json",'r'))
        tail_path = tail_path['NasPublic']
        
        requests.packages.urllib3.disable_warnings()
        # import product list
        df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
        df_products.set_index( keys=['type','code'],inplace=True)
        stock_list = list(df_products.loc["E"].index)
        future_list = list(df_products.loc["F"].index)
        log.processLog(f'[direxion.py] 讀取前置檔案：{in_path + "/product_code.csv"}')


        s = requests.Session()
        params = {'operationName':'ListKurtosysFunds','variables':{'filter':{'Level1':{'eq':'ETF'}},'limit':300},'query':'query ListKurtosysFunds($filter: TableKurtosysFundsFilterInput, $limit: Int, $nextToken: String) {\n  listKurtosysFunds(filter: $filter, limit: $limit, nextToken: $nextToken) {\n    items {\n      Ticker\n      FundName\n      FundCode\n      Index\n      IndexName\n      Level1\n      Level2\n      Level3\n      Level4\n      Prospectus\n      Target\n      Duration\n      IntradayValue\n      IndexCusip\n      Distribution {\n        IncomeDividend\n        IncomeDividendConvertibleToReturnOfCapital\n        LongTermCapitalGain\n        ExDate\n        PayDate\n        RecordDate\n        ReturnOfCapital\n        ShortTermCapitalGain\n        APIInfo\n        __typename\n      }\n      Distributions {\n        IncomeDividend\n        IncomeDividendConvertibleToReturnOfCapital\n        LongTermCapitalGain\n        ExDate\n        PayDate\n        RecordDate\n        ReturnOfCapital\n        ShortTermCapitalGain\n        APIInfo\n        __typename\n      }\n      MonthlyMarketPerformance {\n        ExpenseRatioGross\n        ExpenseRatioNet\n        InceptionDate\n        TradeDate\n        ItemPerformanceType\n        OneMonth\n        ThreeMonth\n        YTD\n        OneYear\n        ThreeYear\n        FiveYear\n        TenYear\n        SinceInception\n        APIInfo\n        __typename\n      }\n      QuarterlyMarketPerformance {\n        ExpenseRatioGross\n        ExpenseRatioNet\n        InceptionDate\n        TradeDate\n        ItemPerformanceType\n        OneMonth\n        ThreeMonth\n        YTD\n        OneYear\n        ThreeYear\n        FiveYear\n        TenYear\n        SinceInception\n        APIInfo\n        __typename\n      }\n      MonthlyNavPerformance {\n        ExpenseRatioGross\n        ExpenseRatioNet\n        InceptionDate\n        TradeDate\n        ItemPerformanceType\n        OneMonth\n        ThreeMonth\n        YTD\n        OneYear\n        ThreeYear\n        FiveYear\n        TenYear\n        SinceInception\n        APIInfo\n        __typename\n      }\n      QuarterlyNavPerformance {\n        ExpenseRatioGross\n        ExpenseRatioNet\n        InceptionDate\n        TradeDate\n        ItemPerformanceType\n        OneMonth\n        ThreeMonth\n        YTD\n        OneYear\n        ThreeYear\n        FiveYear\n        TenYear\n        SinceInception\n        APIInfo\n        __typename\n      }\n      Pricing {\n        MarketClosingPrice\n        MarketClosingChangeDollar\n        MarketClosingChangePercent\n        Nav\n        NavChangeDollar\n        NavChangePercent\n        PremiumDiscount\n        Ticker\n        TradeDate\n        __typename\n      }\n      Url\n      TimeStamp\n      TradeDate\n      EndOfMonthDate\n      EndOfQuarterDate\n      EndOfQuarterHoldingsDate\n      EndOfQuarterBondStatisticsDate\n      EndOfQuarterPerformanceDate\n      APIInfo\n      __typename\n    }\n    nextToken\n    __typename\n  }\n}\n'}
        s.headers.update({'x-api-key': 'da2-iafcow2bfbcgfj7l2yk4rixypm'})
        r = s.post("https://p6vg2dlkpverjbr2afvpbqupky.appsync-api.us-east-1.amazonaws.com/graphql", data = json.dumps(params))
        count = 0
        while r.status_code != 200:
            count = count+1
            print(f'[direxion.py] 索取Tickers與淨值程序-第{count}次連線失敗...5秒後重試')
            log.processLog(f'[direxion.py] 索取Tickers與淨值程序-第{count}次連線失敗...5秒後重試')
            time.sleep(5)
            r = s.post("https://p6vg2dlkpverjbr2afvpbqupky.appsync-api.us-east-1.amazonaws.com/graphql", data = json.dumps(params))
    
            
        
        df = pd.json_normalize(json.loads(r.text)['data']['listKurtosysFunds']['items'])
        df.to_csv("data.csv", index = False)
        #0506補充程序
        NAV_Table = df[['Ticker','Pricing.TradeDate','Pricing.Nav']]
        NAV_Table['Pricing.TradeDate'] = NAV_Table['Pricing.TradeDate'].apply(lambda x: datetime.strptime(x,'%m%d%Y').strftime('%Y-%m-%d'))
        NAV_Table.columns = ['Ticker','Date','NAV']
        if len(set(list((NAV_Table['Date'].values)))) > 1:
            print('NAV仍有問題，不需要輸出')
        else:
            NAV_Table.insert(0,'Issuer','Direxion')
            NAV_Table.to_csv(f"Direxion_{NAV_Table['Date'][0]}_temp.csv",index=False,encoding='utf-8-sig')
            log.processLog(f"[direxion.py] 輸出：Direxion_{NAV_Table['Date'][0]}_temp.csv")
        
        
        df = df.set_index("Ticker")
        text = 'Code,Name,Ticker\n'
        
    
        print('[direxion.py] 已取得本次需爬取Tickers/對應淨值')
        log.processLog(f'[direxion.py] 已取得本次需爬取Tickers/對應淨值，數量：{len(df.index)}')
        countN = 0
        for ticker in df.index:
            countN = countN+1
            check = False
            log.processLog(f'[direxion.py] {countN}/{len(df.index)}... 開始爬取{ticker}')
            print(f'[direxion.py] {countN}/{len(df.index)}... 開始爬取{ticker}')
            
            try:
                r = requests.get(f"https://www.direxion.com/holdings/{ticker}.csv", verify=False)
                count = 0
                while r.status_code != 200:
                    if count > 10:
                        check = True 
                        log.processLog(f'[direxion.py] {ticker}：10次連線失敗，忽略掉此標的')
                        print(f'[direxion.py] {ticker}：10次連線失敗，忽略掉此標的')
                        break
                    count = count + 1
                    print(f'[direxion.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                    log.processLog(f'[direxion.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                    time.sleep(5)
                    r = requests.get(f"https://www.direxion.com/holdings/{ticker}.csv", verify=False)
                if check:
                    continue
                else:
                    df2 = pd.read_csv(StringIO(r.text), skiprows=5)
                    log.processLog(f'[direxion.py] {ticker}：成分表爬取完成...開始進行成分表處理程序')
                    print(f'[direxion.py] {ticker}：成分表爬取完成...開始進行成分表處理程序')
                
                    
                Date = datetime.strptime(df.loc[ticker, 'Pricing.TradeDate'], '%m%d%Y').strftime("%Y-%m-%d")

                path = f"{tail_path}/{Date}/direxion/"
                
                if not os.path.exists(path):
                    log.processLog(f'[direxion.py] 尚無本日資料夾，建立：{path}')
                    os.makedirs(f"{path}")
                    # os.system(f"mkdir -p {path}")
                
                if 'Cusip' in df2.columns:
                    df2 = df2.drop(['TradeDate', 'AccountTicker', 'Cusip'], axis =1)
                else:
                    df2 = df2.drop(['TradeDate', 'AccountTicker'], axis =1)

                df2 = df2.rename(columns={ "StockTicker" : "Ticker", "SecurityDescription" : "Name",  " Shares/Contracts" : "Shares", "HoldingsPercent" : "Weight", "MarketValue" : "Market Value"})
                df2 = df2.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price',
                            'Multiplier', 'FX Rate']    )
                df2.insert(loc=4, column='Expiry_month',value=['' for i in range(df2.shape[0])])
                
                path = f"{tail_path}/{Date}/direxion/{ticker}_PCF_N_{Date}.csv"
                #path = f"PCF_Data/{Date}/direxion/{ticker}_PCF_N_{Date}.csv"

                with open(path, 'wb') as f:
                    f.write(r.content)

                with open(path, "r",encoding='utf-8-sig', newline='') as csvfile:
                    spamreader = csv.reader(csvfile, delimiter=',')
                    rows = [line for line in spamreader]
                    FundName = rows[0][0]
                    Shares = float(rows[2][0].split(":")[1])
                    Nav = df.loc[ticker, 'Pricing.Nav']

                with open(path, "w",encoding='utf-8-sig', newline='') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',')
                    spamwriter.writerow(['Date', Date])
                    spamwriter.writerow(['Name', FundName])
                    spamwriter.writerow(['Net Asset', Nav * Shares])
                    spamwriter.writerow(['Shares Outstanding', Shares])
                    spamwriter.writerow(['ETF Currency', 'USD'])
                    spamwriter.writerow(['Nav per Share', Nav])
                    spamwriter.writerow([])

                #set symbol
                df2.loc[:, 'Price'] = df2.loc[:,'Market Value'] / df2.loc[:, 'Shares']
                df2.loc[:, 'Weight'] = df2.loc[:,'Market Value'].abs() / df2.loc[:, 'Market Value'].abs().sum()
                df2 = df2[df2['Name']!='Net Current Assets']
                for value in df2.values:
                    if pd.notnull(value[0]):
                        if value[0][0:2].strip() in future_list and value[0] not in stock_list:
                            code = value[0][0:2].strip()
                            current = df_products.loc['F'].loc[code]
                            pos2 = df2['Ticker']==value[0]
                            try:
                                expiry_month = datetime.strptime(value[1].split()[-1],'%b%y').strftime('%Y%m')
                            except:
                                expiry_month = ''
                                print(value[0],'Datetime format cannot be parsed.')
                                #看起來他找不到就直接給他空值了
                                
                            df2.loc[pos2,'Ticker'] = current[0]
                            df2.loc[pos2,'Currency'] = current[3]
                            df2.loc[pos2,'Multiplier'] = current[4]
                            df2.loc[pos2,'Expiry_month'] = expiry_month
                            if current[3]=='USD':
                                df2.loc[pos2,'FX Rate'] = '1.0000'
                        elif value[0] in stock_list:
                            current = df_products.loc['E'].loc[value[0]]
                            pos2 = df2['Ticker']==value[0]
                            df2.loc[pos2,'Ticker'] = current[0]
                            df2.loc[pos2,'Currency'] = current[3]
                            df2.loc[pos2,'Multiplier'] = current[4]
                            if current[3]=='USD':
                                df2.loc[pos2,'FX Rate'] = '1.0000'
                    else:
                        if value[1] in list(df_products.loc['S']['productname']):
                            pos = df_products.loc['S']['productname']==value[1]
                            loc = df2['Name']==value[1]
                            df2.loc[loc,'Ticker'] = df_products.loc['S'].loc[pos]['productid'].values[0]
                            df2.loc[loc,'Currency'] = df_products.loc['S'].loc[pos]['Currency'].values[0]
                            df2.loc[loc,'Multiplier'] = df_products.loc['S'].loc[pos]['Multiplier'].values[0]
                            if df2.loc[loc,'Currency'].values[0]=='USD':
                                df2.loc[loc,'FX Rate'] = '1.0000'
                        elif 'GOVT INST' in value[1] or 'TRSRY' in value[1] or 'CASH' in value[1]:
                            pos = df2['Name']==value[1]
                            df2.loc[pos,'Ticker'] = 'Cash'
                            df2.loc[pos,'Currency'] = 'USD'
                            df2.loc[pos,'Multiplier'] = '1'
                            df2.loc[pos,'FX Rate'] = '1.0000'
                df2.to_csv(path, mode = "a", index=False)
                log.processLog(f'Output:{path}')
                log.processLog(f'[direxion.py] {ticker}：本支ETF處理完畢')
                log.processLog(f'---------------------')
                time.sleep(1)
            except:
                log.processLog(f'[direxion.py] {ticker}：本支ETF處理遇到未知問題，請查看錯誤log檔，本處先從下一支繼續開始')
                log.processLog(f'---------------------')
                log.errorLog(f'{format_exc()}')
                continue
        end = time.perf_counter()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
        log.processLog('==============================================================================================')
    except:
        log.processLog(f'[direxion.py] {ticker}：本次爬蟲出現嚴重錯誤，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')