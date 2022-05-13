import pandas as pd 
from simplified_scrapy import SimplifiedDoc, utils, req
import datetime
from dateutil import parser
import requests
import os
import json
import csv
import numpy as np
import pandas as pd
from pandas import DataFrame
import pathlib
from traceback import print_exc,format_exc
import log_manager as log 
import time
from datetime import datetime
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
    # import normalized product list
    df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
    df_products.set_index( keys=['type','code'],inplace=True)
    stock_list = list(df_products.loc["E"].index)
    future_list = list(df_products.loc["F"].index)

    # processing new products
    id=list(map(str,code.split()))
    if len(id) > 0:
        if(code not in stock_list and id[0] not in future_list):
            if(len(id)==1):
                # bonds or us stock
                # id = id[0] + '_US'
                # new_row=pd.Series(data={'productid':id,'productname':name,'Exchange':'','Currency':'USD','Multiplier':1,'type':'E'}, name=code)
                # df_products=df_products.append(new_row, ignore_index=False)
                # print(df_products.loc['E'].loc[code])
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
                        elif(id[0][0]=='0' or id[0][0]=='3'): #Shengzen Stock Exchange
                            stock_type=stock_info.loc[id[1]]
                            productid='{}_{}'.format(id[0],stock_type[0])
                            new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':'SZ','Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                            with open(in_path + '/product_code.csv', "a", newline='') as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,'SZ',stock_type[2],stock_type[3],'E'])
                    else:
                        stock_type=stock_info.loc[id[1]]
                        productid='{}_{}'.format(id[0],stock_type[0])
                        new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':stock_type[1],'Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                        with open(in_path + '/product_code.csv', "a", newline='') as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,stock_type[1],stock_type[2],stock_type[3],'E'])
                else:
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
    fx_list = list(df_products.loc["X"].index)
    flag = 1
    for i in range(len(df.values)):
        value = df.iloc[i]
        if pd.notnull(value[0]):
            code = value[0].strip().replace('_US','').replace('MARGIN_','')
            if (code[:-2].strip() in future_list or code[:-3] in future_list) and code not in stock_list:
                if len(code)==4:
                    mon = code[2:4]
                    code = code[:-2].strip()
                    expiry_month = parseMonth(mon)
                elif len(code) == 5 and code[:-2].strip() in future_list:
                    mon = code[3:5]
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
                    df.loc[pos2,'Currency'] = current[3]
                    df.loc[pos2,'Multiplier'] = current[4]
                    df.loc[pos2,'Expiry_month'] = expiry_month
            elif code in swap_list:
                current = df_products.loc['S'].loc[code]
                pos2 = df['Ticker']==value[0]
                df.loc[pos2,'Ticker'] = current[0]
                df.loc[pos2,'Currency'] = current[3]
                df.loc[pos2,'Multiplier'] = current[4]
            elif code in fx_list:
                current = df_products.loc['X'].loc[code]
                pos2 = df['Ticker']==value[0]
                df.loc[pos2,'Ticker'] = current[0]
                df.loc[pos2,'Currency'] = current[3]
                df.loc[pos2,'Multiplier'] = current[4]
            elif code in stock_list:
                current = df_products.loc['E'].loc[code]
                pos2 = df['Ticker']==value[0]
                df.loc[pos2,'Ticker'] = current[0]
                df.loc[pos2,'Currency'] = current[3]
                df.loc[pos2,'Multiplier'] = current[4]
                if current[3]=='USD': 
                    df.loc[pos2,'FX Rate'] = '1.0000'
            elif 'TREASURY BILL' in value[1] or 'CASH' in value[1] or 'Cash' in value[1] or 'CSH' in value[1]:
                pos = df['Name']==value[1]
                df.loc[pos,'Ticker'] = 'Cash'
                df.loc[pos,'Currency'] = 'USD'
                df.loc[pos,'Multiplier'] = '1'
                df.loc[pos,'FX Rate'] = '1.0000'
            elif len(code) == 9:
                pos2 = df['Ticker']==value[0]
                df.loc[pos2,'Currency'] = 'USD'
                df.loc[pos2,'Multiplier'] = 1
                df.loc[pos2,'FX Rate'] = '1.0000'
            else:
                process(value[0],value[1],ticker)
                flag = -1
        else:
            if 'TREASURY BILL' in value[1] or 'CASH' in value[1] or 'Cash' in value[1] or 'CSH' in value[1]:
                pos = df['Name']==value[1]
                df.loc[pos,'Ticker'] = 'Cash'
                df.loc[pos,'Currency'] = 'USD'
                df.loc[pos,'Multiplier'] = '1'
                df.loc[pos,'FX Rate'] = '1.0000'
            else:
                with open(in_path + '/unrecorded.csv', "a", newline='') as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',')
                    spamwriter.writerow([value[0],value[1],ticker])
    df.to_csv(path, mode = 'a', index = 0)
    print('Output:',path)
    log.processLog(f'Output:{path}')
    return flag


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
        #儲存NAV資訊
        dict_NAV = {}
        with open(in_path + '/unrecorded.csv', "w", newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',')
            spamwriter.writerow(['Code','Name','Ticker'])
        log.processLog(f'[ishares.py] 建立紀錄未見過的成分檔案：{in_path + "/unrecorded.csv"}')

            
        # import stock info
        stock_info = pd.read_csv(in_path + "/stock.csv",header=0,encoding='utf-8-sig')
        stock_info = stock_info.set_index('Suffix')
        log.processLog(f'[ishares.py] 讀取前置檔案：{in_path + "/stock.csv"}')

        monthDict = {'F':'01','G':'02','H':'03','J':'04',
                    'K':'05','M':'06','N':'07','Q':'08',
                    'U':'09','V':'10','X':'11','Z':'12'}
        # import product list
        df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
        df_products.set_index( keys=['type','code'],inplace=True)
        stock_list = list(df_products.loc["E"].index)
        future_list = list(df_products.loc["F"].index)
        log.processLog(f'[ishares.py] 讀取前置檔案：{in_path + "/product_code.csv"}')



        url = 'https://www.ishares.com/us/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/product-screener-ketto&siteEntryPassthrough=true'
        count = 0 
        r = requests.get(url)
        while r.status_code != 200:
            count = count+1
            print(f'[ishares.py] 索取Tickers程序-第{count}次連線失敗...5秒後重試')
            log.processLog(f'[ishares.py] 索取Tickers程序-第{count}次連線失敗...5秒後重試')
            time.sleep(5)
            r = requests.get(url)
        
        data = json.loads(r.text)['data']['tableData']['data'][16:]
        print('[ishares.py] 索取Tickers程序完成')
        log.processLog(f'[ishares.py] 索取Tickers程序完成')
        
        
        print('[ishares.py] 已取得本次需爬取Tickers...')
        log.processLog(f'[ishares.py] 已取得本次需爬取Tickers，數量：{len(data)}')
        log.processLog(f'---------------------')
        countN = 0
        for i in data:
            countN = countN+1
            check = False
            id = i[54].split('/')[3]
            Name = i[54].split('/')[-1]
            symbol = i[28]   
            if symbol not in ['IWM','SOXX','ITB','ITA','IGV','TLT','IEF','IJR','FXI','EEM','EWW','ILF','EWZ','INDA','ICLN','EFA','EWJ','EWY','IVV']:
                continue
        
            log.processLog(f'[ishares.py] {countN}/{len(data)}... 開始爬取{symbol}')
            url = f"https://www.ishares.com/us/products/{id}/fund/1521942788811.ajax?fileType=xls&fileName={Name}&dataType=fund"
            count = 0
            resp = requests.get(url)
            while resp.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[ishares.py] {symbol}：10次連線失敗，忽略掉此標的')
                    print(f'[ishares.py] {symbol}：10次連線失敗，忽略掉此標的')
                    break
                count = count + 1
                print(f'[ishares.py] {symbol}：淨值表/成分表爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[ishares.py] {symbol}：淨值表/成分表爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                resp = requests.get(url)
            if check:
                continue
            else:
                doc = SimplifiedDoc(resp.text)
                
            worksheets = doc.selects('ss:Worksheet') # Get all Worksheets
            log.processLog(f'[ishares.py] {symbol}：淨值表/成分表爬取完成...開始進行成分表處理程序')


            holdings = pd.DataFrame()
            history = pd.DataFrame()
            
            for worksheet in worksheets: # break the excel to each sheet
                if worksheet['ss:Name'] == 'Holdings':
                    holdings =pd.DataFrame(worksheet.selects('ss:Row').selects('ss:Cell>text()'))
                if worksheet['ss:Name'] == 'Historical':
                    history =pd.DataFrame(worksheet.selects('ss:Row').selects('ss:Cell>text()'))
            if holdings.empty or history.empty:
                continue
            ##0506補充程序
            dict_NAV.update({f'{symbol}':[history.iloc[1,0],history.iloc[1,1]]})
            
            Date = datetime.strptime(holdings.iloc[0, 0], '%d-%b-%Y').strftime("%Y-%m-%d")

            path = f'{tail_path}/{Date}/ishares/'
            if not os.path.exists(path):
                log.processLog(f'[ishares.py] 尚無本日資料夾，建立：{path}')
                os.makedirs(f"{path}")
                # os.system(f"mkdir -p {path}")
            
            Name = holdings.iloc[1, 0]

            history.columns = history.iloc[0]
            history = history[1:]
            history = history.set_index('As Of')
            nav_date = datetime.strptime(holdings.iloc[0, 0], '%d-%b-%Y').strftime("%b %d, %Y")
            if holdings.shape[1] == 2:
                holdings.columns = holdings.iloc[8]
                holdings = holdings[9:]
                holdings = holdings.rename(columns = {"Weight (%)" : "Weight"})
                holdings = holdings.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price',
                                'Multiplier', 'FX Rate']    )
                Nav = history.loc[nav_date].loc["NAV per Share"]
                Shares = np.nan
                NetAsset = np.nan
            else:
                holdings.columns = holdings.iloc[7]
                holdings = holdings[8:]
                holdings = holdings.rename(columns = {"Weight (%)" : "Weight"})
                holdings = holdings.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price', 'Multiplier', 'FX Rate'])
                Nav = float(history.loc[nav_date].loc["NAV per Share"])
                Shares = float(history.loc[nav_date].loc["Shares Outstanding"])
                NetAsset = Nav * Shares


            path = f'{tail_path}/{Date}/ishares/{symbol}_PCF_N_{Date}.csv'

            with open(path, "w",encoding='utf-8-sig', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=',')
                writer.writerow(['Date', Date])
                writer.writerow(['Name', Name])
                writer.writerow(['Net Asset', NetAsset])
                writer.writerow(['Shares Outstanding', Shares])
                writer.writerow(['ETF Currency', 'USD'])
                writer.writerow(['Nav per Share', Nav])
                writer.writerow([])
            holdings = holdings.reindex()
            holdings.insert(loc=4, column='Expiry_month',value=['' for i in range(holdings.shape[0])])
            process_df(holdings,symbol)
            
            log.processLog(f'[ishares.py] {symbol}：本支ETF處理完畢')
            log.processLog(f'---------------------')

        ## 0506補充程序
        ddddf = pd.DataFrame({'Ticker':list(dict_NAV),
                            'Date':[ datetime.strptime(i[0],'%b %d, %Y').strftime('%Y-%m-%d')  for i in dict_NAV.values()],
                            'NAV':[ i[1] for i in dict_NAV.values()]})
        if len(set(list((ddddf['Date'].values)))) > 1:
            print('NAV仍有問題，不需要輸出')
            log.processLog(f'[ishares.py] NAV暫時表出現長度不一情形，代表連NAV都會延遲更新')
        else:
            ddddf.insert(0,'Issuer','iShares')
            ddddf.to_csv(f"ishares_{ddddf['Date'][0]}_temp.csv",index=False,encoding='utf-8-sig')
            log.processLog(f"[ishares.py] 輸出：ishares_{ddddf['Date'][0]}_temp.csv")
            # import pdb; pdb.set_trace()
            
        end = time.perf_counter()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
        log.processLog('==============================================================================================')
    except:
        log.processLog(f'[ishares.py] {symbol}：本次爬蟲出現嚴重錯誤，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(symbol,"---------------------")