import datetime
import csv
import os
import zipfile
import pandas as pd
from pandas import DataFrame
import pathlib
import requests
import json
from traceback import format_exc,print_exc
import log_manager as log
import time

def parseMonth(x):
    try:
        if len(x)== 2:
            result = '202' + x[1] + monthDict[x[0]]
        else:
            result = ''
    except:
        result = ''
    return result


def process(code = str, name = str,ticker=str):
    code = code.strip()
    # import normalized product list
    df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
    df_products.set_index( keys=['type','code'],inplace=True)
    stock_list = list(df_products.loc["E"].index)
    future_list = list(df_products.loc["F"].index)

    # processing new products
    id=list(map(str,code.split('-')))
    if len(id) > 0:
        if(code not in stock_list and id[0] not in future_list):
            if(len(id)==1):
                # bonds or us stock
                # id = id[0] + '_US'
                # new_row=pd.Series(data={'productid':id,'productname':name,'Exchange':'','Currency':'USD','Multiplier':1,'type':'E'}, name=code)
                # df_products=df_products.append(new_row, ignore_index=False)
                # print(df_products.loc['E'].loc[code])
                if code not in existed: #本次尚未被記錄到的成分股，才會記錄到unrecorded
                    existed.append(code)
                    with open(in_path + '/unrecorded.csv', "a", newline='') as csvfile:
                        spamwriter = csv.writer(csvfile, delimiter=',')
                        spamwriter.writerow([code,name,ticker])
            elif len(id) > 1:
                # stocks, preferred stocks
                if(id[1] in stock_info.index):
                    if(id[1]=='CH'): #Chinese stock
                        if(id[0][0]=='9' or id[0][0]=='6'): # Shanghai Stock Exchange
                            stock_type=stock_info.loc[id[1]]
                            productid='{}_{}'.format(id[0],stock_type[0])
                            new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':'SH','Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                            with open(in_path + '/product_code.csv', "a", newline='') as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,'SH',stock_type[2],stock_type[3],'E'])
                            print(new_row)
                        elif(id[0][0]=='0' or id[0][0]=='3'): #Shengzen Stock Exchange
                            stock_type=stock_info.loc[id[1]]
                            productid='{}_{}'.format(id[0],stock_type[0])
                            new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':'SZ','Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                            with open(in_path + '/product_code.csv', "a", newline='') as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,'SZ',stock_type[2],stock_type[3],'E'])
                            print(new_row)
                    else:
                        stock_type=stock_info.loc[id[1]]
                        productid='{}_{}'.format(id[0],stock_type[0])
                        new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':stock_type[1],'Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                        with open(in_path + '/product_code.csv', "a", newline='') as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,stock_type[1],stock_type[2],stock_type[3],'E'])
                        print(new_row)
                else:
                    if code not in existed:
                        existed.append(code)
                        with open(in_path + '/unrecorded.csv', "a", newline='') as csvfile:
                            spamwriter = csv.writer(csvfile, delimiter=',')
                            spamwriter.writerow([code,name,ticker])

#holdings
def process_df(df=DataFrame,ticker=str):
    # import product list
    df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
    df_products.set_index( keys=['type','code'],inplace=True)
    stock_list = list(df_products.loc["E"].index)
    future_list = list(df_products.loc["F"].index)
    swap_list = list(df_products.loc["S"].index)
    flag = 1
    for i in range(len(df.values)): #每一個成分股逐一判斷
        try:
            value = df.iloc[i]
            if pd.notnull(value[0]): #Ticker非空值的，此處是為了填上三個欄位
                code = value[0].strip().replace('CASH_','').replace('_US','')
                if value[0] == '-CASH-' or value[0] == 'Cash':
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = '1.0000'
                elif 'Treasury Bill' in value[1]:
                    pos = df['Name']==value[1]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = '1.0000'
                elif len(code) == 9:
                    pos2 = df['Ticker']==value[0]
                    df.loc[pos2,'Multiplier'] = 1
                    df.loc[pos2,'FX Rate'] = '1.0000'
                elif (code[:-2].strip() in future_list or code[:-3] in future_list) and code not in stock_list:
                    if len(code)==4:
                        mon = code[2:4]
                        code = code[:-2].strip()
                        expiry_month = parseMonth(mon)
                    elif len(code) == 5 and code[:-2].strip() in future_list:
                        mon = code[2:4]
                        code = code[:-2].strip()
                        expiry_month = parseMonth(mon)
                    elif len(code) == 5 and code[:-3].strip() in future_list:
                        mon = code[2] + code[4]
                        code = code[:-3].strip()
                        expiry_month = parseMonth(mon)
                    if code in future_list:
                        current = df_products.loc['F'].loc[code]
                        pos2 = df['Ticker']==value[0]
                        df.loc[pos2,'Ticker'] = current[0]
                        df.loc[pos2,'Multiplier'] = current[4]
                        df.loc[pos2,'Expiry_month'] = expiry_month
                        if current[3]=='USD':
                            df.loc[pos2,'FX Rate'] = '1.0000'
                elif code in swap_list:
                    current = df_products.loc['S'].loc[code]
                    pos2 = df['Ticker']==value[0]
                    df.loc[pos2,'Ticker'] = current[0]
                    df.loc[pos2,'Multiplier'] = current[4]
                    if current[3]=='USD':
                        df.loc[pos2,'FX Rate'] = '1.0000'
                elif code in stock_list:
                    current = df_products.loc['E'].loc[code]
                    pos2 = df['Ticker']==value[0]
                    df.loc[pos2,'Ticker'] = current[0]
                    df.loc[pos2,'Multiplier'] = current[4]
                    if current[3]=='USD':
                        df.loc[pos2,'FX Rate'] = '1.0000'
                else:
                    process(value[0],value[1],ticker) #這邊不會Return任何東西，該列的三個欄位直接就是空值
                    flag = -1
            else:
                if 'TREASURY BILL' in value[1] or 'TRSRY' in value[1]: #改用詳細名稱來判斷
                    pos = df['Name']==value[1]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'USD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = '1.0000'
                else:
                    with open(in_path + '/unrecorded.csv', "a", newline='') as csvfile:
                        spamwriter = csv.writer(csvfile, delimiter=',')
                        spamwriter.writerow([value[0],value[1],ticker]) 
        except Exception as e:
            print('Error when processing df. Exception:',e)
            continue
    df.to_csv(path, mode = 'a', index = 0)
    print('Output:',path)
    log.processLog(f'Output:{path}')
    return flag

def recur_connect(url:str,row=False) -> pd.DataFrame:
    try:
        if row:
            r = requests.get(url)
            with open('test.xlsx','wb+') as f:
                f.write(r.content)
                f.close()
            # 錯誤：'https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-gldm.xlsx'
            # 正確：'https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-hybl.xlsx'

            df = pd.read_excel(url, engine='openpyxl',skiprows=3)
        else:
            df = pd.read_excel(url, engine='openpyxl')
    except:
        if row:
            df = recur_connect(url,True)
        else:
            df = recur_connect(url)
    return df


if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US成份表爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    #路徑設定
    in_path = str(pathlib.Path(__file__).parent.absolute())
    tail_path = json.load(open(f"{in_path}/path.json",'r'))
    tail_path = tail_path['NasPublic']

    with open(in_path + '/unrecorded.csv', "w", newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        spamwriter.writerow(['Code','Name','Ticker'])
    log.processLog(f'[statestreet.py] 建立紀錄未見過的成分檔案：{in_path + "/unrecorded.csv"}')

    # import stock info (已知的成分標的)
    stock_info = pd.read_csv(in_path + "/stock.csv",header=0,encoding='utf-8-sig')
    stock_info = stock_info.set_index('Suffix')
    log.processLog(f'[statestreet.py] 讀取前置檔案：{in_path + "/stock.csv"}')

    monthDict = {'F':'01','G':'02','H':'03',
                'J':'04','K':'05','M':'06',
                'N':'07','Q':'08','U':'09',
                'V':'10','X':'11','Z':'12'}
    # import product list
    df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
    df_products.set_index( keys=['type','code'],inplace=True)
    stock_list = list(df_products.loc["E"].index)
    future_list = list(df_products.loc["F"].index)
    
    existed = []
    log.processLog(f'[statestreet.py] 讀取前置檔案：{in_path + "/product_code.csv"}')

    #0423改寫，改用key作為連線判斷
    log.processLog(f'[statestreet.py] 開始爬取：Statestreet網 - https://www.ssga.com/bin/v1/ssmp/fund/fundfinder?country=us&language=en&role=institutional&product=etfs&ui=fund-finder')
    count = 0
    r = requests.get("https://www.ssga.com/bin/v1/ssmp/fund/fundfinder?country=us&language=en&role=institutional&product=etfs&ui=fund-finder")
    while r.status_code != 200:
        count = count+1
        print(f'[statestreet.py] 索取Tickers程序-第{count}次連線失敗...5秒後重試')
        log.processLog(f'[statestreet.py] 索取Tickers程序-第{count}次連線失敗...5秒後重試')
        time.sleep(5)
        r = requests.get("https://www.ssga.com/bin/v1/ssmp/fund/fundfinder?country=us&language=en&role=institutional&product=etfs&ui=fund-finder")
   
    data = json.loads(r.text)
    print('[statestreet.py] 索取Tickers程序完成')
    log.processLog(f'[statestreet.py] 索取Tickers程序完成')
            

    TICKERS = []
    for i in data['data']['funds']['etfs']['datas']:
        TICKERS.append(i['fundTicker'].lower())
        
    print('[statestreet.py] 已取得本次需爬取Tickers...')
    log.processLog(f'[statestreet.py] 已取得本次需爬取Tickers，數量：{len(TICKERS)}')
    countN = 0
    count = 0
    target  = ['SPY','DIA','MDY','XLI','XPH','KRE','XLY','XBI','XLK','XLU','XLE','XME','XOP']
    target = [ i.lower() for i in target]
    for ticker in TICKERS:
        countN = countN+1
        check = False
        if ticker not in target:
            continue
        log.processLog(f'[statestreet.py] {countN}/{len(TICKERS)}... 開始爬取{ticker}')
        try:
            #Get historical NAV data：/library-content/products/fund-data/etfs/us/navhist-us-en-${ticker}.xlsx
            url = f"https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/navhist-us-en-{ticker}.xlsx"
            # df = pd.read_excel(url, engine='openpyxl', skiprows=3) #這行有問題再改成遞迴
            # df = recur_connect(url,True)
            count = 0
            r = requests.get(url)
            while r.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[statestreet.py] {ticker}：10次連線失敗，忽略掉此標的')
                    print(f'[statestreet.py] {ticker}：10次連線失敗，忽略掉此標的')
                    break
                count = count + 1
                print(f'[statestreet.py] {ticker}：歷史淨值檔爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[statestreet.py] {ticker}：歷史淨值檔爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(url)
            if check:
                continue
            else:
                with open('NAV.xlsx','wb+') as f:
                    f.write(r.content)
                    f.close()
                df = pd.read_excel('NAV.xlsx',engine='openpyxl',skiprows=3)
            
            Date = datetime.datetime.strptime(df.Date[0], '%d-%b-%Y').strftime("%Y-%m-%d")
            log.processLog(f'[statestreet.py] {ticker}：歷史淨值檔爬取完成')
            print(f'[statestreet.py] {ticker}：歷史淨值檔爬取完成')
            
            Nav = df.NAV[0]
            Shares = df.iloc[0, 2]
            NetAsset = df.iloc[0, 3]

            path = f"{tail_path}/{Date}/statestreet/"
            if not os.path.exists(path):
                log.processLog(f'[statestreet.py] 尚無本日資料夾，建立：{path}')
                os.makedirs(f"{path}")
                # os.system(f"mkdir -p {path}")

            #holdings
            # Holding Data：/library-content/products/fund-data/etfs/us/holdings-daily-us-en-${ticker}.xlsx
            url = f"https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx"
            # df = pd.read_excel(url, engine='openpyxl')            
            count = 0
            r = requests.get(url)
            while r.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[statestreet.py] {ticker}：10次連線失敗，忽略掉此標的')
                    print(f'[statestreet.py] {ticker}：10次連線失敗，忽略掉此標的')
                    log.processLog(f'---------------------')
                    break
                count = count + 1
                print(f'[statestreet.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[statestreet.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(url)
            if check:
                continue
            else:
                with open('holding.xlsx','wb+') as f:
                    f.write(r.content)
                    f.close()
                df = pd.read_excel('holding.xlsx',engine='openpyxl')
                
            ## 去除檔案標頭的程序
            Name = df.columns[1]
            df.columns = df.iloc[3, :]
            df = df[4:] #
            log.processLog(f'[statestreet.py] {ticker}：成分表爬取完成...開始進行成分表處理程序')

            if 'Ticker' not in df.columns:
                df = df.rename(columns={'Identifier':'Ticker'})
            df = df.rename(columns={'Local Currency':'Currency', 'Shares Held':'Shares'})
            df = df.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price',
                                'Multiplier', 'FX Rate']    ) #這邊拿掉了很多欄位和創造空欄位
            mask = df.Currency == "USD"
            df.loc[mask, 'FX Rate'] = 1 #補上匯率
            df = df[df.Weight.notnull()]
            df.insert(loc=4, column='Expiry_month',value=['' for i in range(df.shape[0])])
        
            path = f"{tail_path}/{Date}/statestreet/{ticker.upper()}_PCF_N_{Date}.csv"
            with open(path, "w",encoding='utf-8-sig', newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                spamwriter.writerow(['Date', Date])
                spamwriter.writerow(['Name', Name])
                spamwriter.writerow(['Net Asset', NetAsset])
                spamwriter.writerow(['Shares Outstanding', Shares])
                spamwriter.writerow(['ETF Currency', 'USD'])
                spamwriter.writerow(['Nav per Share', Nav])
                spamwriter.writerow([])
            for value in df.values:
                if value[0] not in stock_list and value[0] not in future_list:
                    if '(' in str(value[1]) and ')' in str(value[1]):
                        pos = df['Name']==value[1]
                        df.loc[pos,'Ticker'] = value[1].split('(')[1].split(')')[0]
                        ## 若非已知的成分股，則此處是辨識方法 (但只應用於可找出括弧內)
                df = df[df['Name']!='Net Other Assets / Cash'] #排除淨值產 (非成分股的部分)
            df = df.reindex()
            stat = process_df(df,ticker)
            if stat == -1:
                print('Unrecorded products exist.')
            log.processLog(f'[statestreet.py] {ticker}：本支ETF處理完畢')
            log.processLog(f'---------------------')
        except (IOError, zipfile.BadZipFile )as e:
            log.processLog(f'[statestreet.py] {ticker}：發生IO錯誤，本支Ticker未爬取成功，請查看錯誤Error檔')
            log.processLog(f'---------------------')
            log.errorLog(f'{format_exc()}')
            print(ticker,"---------------------")
        except Exception as e:
            log.processLog(f'[statestreet.py] {ticker}：本支ETF處理遇到未知問題，重新加回鋤列')
            log.processLog(f'---------------------')
            log.errorLog(f'{format_exc()}')
            print(e)
            TICKERS.append(ticker)
    
    os.system('rm -f holding.xlsx')
    os.system('rm -f NAV.xlsx')
    end = time.perf_counter()
    log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
    log.processLog('==============================================================================================')


    # url = 'https://www.spdrgoldshares.com/usa/financial-information/'

