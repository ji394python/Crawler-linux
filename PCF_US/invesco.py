from datetime import datetime
import csv
import os.path
from os import path
from io import StringIO
from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import pathlib
import time
import string
from traceback import format_exc,print_exc
import time 
import log_manager as log
import json


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
    id=list(map(str,code.split()))
    if len(id) > 0:
        if(code not in stock_list and id[0] not in future_list):
            if(len(id)==1):
                # bonds or us stock
                # id = id[0] + '_US'
                # new_row=pd.Series(data={'productid':id,'productname':name,'Exchange':'','Currency':'USD','Multiplier':1,'type':'E'}, name=code)
                # df_products=df_products.append(new_row, ignore_index=False)
                # print(df_products.loc['E'].loc[code])
                if code not in existed:
                    existed.append(code)
                    with open(in_path + '/unrecorded.csv', "a") as csvfile:
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
                            with open(in_path + '/product_code.csv', "a") as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,'SH',stock_type[2],stock_type[3],'E'])
                        elif(id[0][0]=='0' or id[0][0]=='3'): #Shengzen Stock Exchange
                            stock_type=stock_info.loc[id[1]]
                            productid='{}_{}'.format(id[0],stock_type[0])
                            new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':'SZ','Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                            with open(in_path + '/product_code.csv', "a") as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,'SZ',stock_type[2],stock_type[3],'E'])
                    else:
                        stock_type=stock_info.loc[id[1]]
                        productid='{}_{}'.format(id[0],stock_type[0])
                        new_row=pd.Series(data={'productid':productid,'productname':name,'Exchange':stock_type[1],'Currency':stock_type[2],'Multiplier':stock_type[3]}, name=code)
                        with open(in_path + '/product_code.csv', "a") as csvfile:
                                spamwriter = csv.writer(csvfile, delimiter=',')
                                spamwriter.writerow([code,productid,name,stock_type[1],stock_type[2],stock_type[3],'E'])
                else:
                    if code not in existed:
                        existed.append(code)
                        with open(in_path + '/unrecorded.csv', "a") as csvfile:
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
    for i in range(len(df.values)):
        try:
            value = df.loc[i]
            if pd.notnull(value[0]):
                code = value[0].strip()
                if value[0] == '-CASH-' or value[0] == 'Cash':
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'USD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = '1.0000'
                elif 'GBP' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'GBP'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Euro' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'EUR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'HongKong' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'HKD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'JapanYen' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'JPY'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'DenmarkKr' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'DKK'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'SAfricaRa' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'ZAR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'SwissFran' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'CHF'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'SwedKrona' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'SEK'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Thai Baht' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'THB'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'TurkLiraN' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'TRY'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Philippin' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'PHP'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'MexicNewP' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'MXN'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'HungForin' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'HUF'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'EgyptPoun' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'EGP'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'IndonesRp' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'IDR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'SouthKore' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'KRW'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Singapore' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'SGD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Pol Zlot' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'PLN'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'IsraelShe' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'ILS'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'NewZealnd' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'NZD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Australia' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'AUD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'CAD' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'CAD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'China Yua' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'CNY'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'SaudArabR' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'SAR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'ChileanPe' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'CLP'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'BrazilRea' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'BRL'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'IndiaRupe' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'INR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'NorwayKrn' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'NOK'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'PakstanRu' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'PKR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'PeruNewSo' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'PEN'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Qatar Ria' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'QAR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'RussiaRub' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'RUB'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Taiwan' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'NTD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'UAE Dirha' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'AED'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'KuwaitiDi' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'KWD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'MalaysnRi' in value[0]:
                    pos = df['Ticker']==value[0]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'MYR'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = ''
                elif 'Treasury Bill' in value[1]:
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
                        df.loc[pos2,'Currency'] = current[3]
                        df.loc[pos2,'Multiplier'] = current[4]
                        df.loc[pos2,'Expiry_month'] = expiry_month
                        if current[3]=='USD':
                            df.loc[pos2,'FX Rate'] = '1.0000'
                elif code in swap_list:
                    current = df_products.loc['S'].loc[code]
                    pos2 = df['Ticker']==value[0]
                    df.loc[pos2,'Ticker'] = current[0]
                    df.loc[pos2,'Currency'] = current[3]
                    df.loc[pos2,'Multiplier'] = current[4]
                    if current[3]=='USD':
                        df.loc[pos2,'FX Rate'] = '1.0000'
                elif code in stock_list:
                    current = df_products.loc['E'].loc[code]
                    pos2 = df['Ticker']==value[0]
                    df.loc[pos2,'Ticker'] = current[0]
                    df.loc[pos2,'Currency'] = current[3]
                    df.loc[pos2,'Multiplier'] = current[4]
                    if current[3]=='USD':
                        df.loc[pos2,'FX Rate'] = '1.0000'
                else:
                    process(value[0],value[1],ticker)
                    flag = -1
            else:
                if 'TREASURY BILL' in value[1] or 'TRSRY' in value[1] or 'Cash' in value[1]:
                    pos = df['Name']==value[1]
                    df.loc[pos,'Ticker'] = 'Cash'
                    df.loc[pos,'Currency'] = 'USD'
                    df.loc[pos,'Multiplier'] = '1'
                    df.loc[pos,'FX Rate'] = '1.0000'
                else:
                    with open(in_path + '/unrecorded.csv', "a") as csvfile:
                        spamwriter = csv.writer(csvfile, delimiter=',')
                        spamwriter.writerow([value[0],value[1],ticker])
        except Exception as e:
            print('Error when processing df. Exception:',e)
            continue
    df.to_csv(path, mode = 'a', index = 0)
    print('Output:',path)
    log.processLog(f'Output:{path}')
    return flag
            

if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US成份表爬蟲專案】 {os.path.basename(__file__)}')
    
    #計時開始
    start = time.perf_counter() 
    
    #ASCII設定
    alphabet_string = string.ascii_uppercase
    stk_tpe = list(alphabet_string)

    #路徑設定
    in_path = str(pathlib.Path(__file__).parent.absolute())
    tail_path = json.load(open(f"{in_path}/path.json",'r'))
    tail_path = tail_path['NasPublic']

    try:
        # import stock info
        stock_info = pd.read_csv(in_path + "/stock.csv",header=0,encoding='utf-8-sig')
        stock_info = stock_info.set_index('Suffix')
        log.processLog(f'[invesco.py] 讀取前置檔案：{in_path + "/stock.csv"}')

        monthDict = {'F':'01','G':'02','H':'03','J':'04',
                    'K':'05','M':'06','N':'07','Q':'08',
                    'U':'09','V':'10','X':'11','Z':'12'}
        
        # import product list
        df_products = pd.read_csv(in_path + "/product_code.csv",header=0,encoding='utf-8-sig',dtype={'code':str})
        df_products.set_index( keys=['type','code'],inplace=True)
        stock_list = list(df_products.loc["E"].index)
        future_list = list(df_products.loc["F"].index)
        swap_list = list(df_products.loc["S"].index)
        existed = []
        log.processLog(f'[invesco.py] 讀取前置檔案：{in_path + "/product_code.csv"}')


        # TICKERS = ['IBBQ', 'BKLN', 'DBA', 'DBB', 'DBC', 'DBE', 'DGL', 'DBO', 'DBP', 'DBS', 'PDBC', 'DBV', 'UDN', 'UUP', 'PBP', 'PHDG', 'PSP', 'PSR', 'KBWY', 'PSMB', 'PSMC', 'PSMG', 'PSMM', 'CVY', 'ADRE', 'CQQQ', 'PIZ', 'PIE', 'IDLB', 'PXF', 'PDN', 'PXH', 'PGJ', 'PIN', 'IPKW', 'IMFL', 'PID', 'GBLD', 'PBDM', 'PBEE', 'ISDX', 'ISEM', 'EELV', 'EEMO', 'IDHD', 'IDLV', 'IDMO', 'IDHQ', 'PPA', 'PYZ', 'PEZ', 'PSL', 'PXI', 'PFI', 'PTH', 'PRN', 'PTF', 'PUI', 'PBE', 'PKB', 'PXE', 'PBJ', 'PEJ', 'PBS', 'PXQ', 'PXJ', 'PJP', 'PSI', 'PSJ', 'PBD', 'PIO', 'KBWB', 'KBWP', 'KBWR', 'CUT', 'ERTH', 'PNQI', 'EWCO', 'RCD', 'RHS', 'RYE', 'RYF', 'RYH', 'RGI', 'RTM', 'EWRE', 'RYT', 'RYU', 'CGW', 'PSCD', 'PSCC', 'PSCE', 'PSCF', 'PSCH', 'PSCI', 'PSCT', 'PSCM', 'PSCU', 'TAN', 'PHO', 'PBW', 'PKW', 'PCEF', 'PDP', 'DWAS', 'DEF', 'PFM', 'DJD', 'PWB', 'PWV', 'PWC', 'PRF', 'PRFZ', 'PGF', 'IVDG', 'PEY', 'KBWD', 'QQQM', 'QQQJ', 'PGX', 'PBUS', 'PBSM', 'QQQ', 'IUS', 'IUSS', 'RYJ', 'IVRA', 'USEQ', 'EQAL', 'USLB', 'OMFL', 'OMFS', 'EQWL', 'SPGP', 'SPMV', 'RWL', 'SPVM', 'SPVU', 'RSP', 'SPHB', 'SPHD', 'SPLV', 'SPMO', 'RPG', 'RPV', 'SPHQ', 'XLG', 'XRLV', 'RWK', 'EWMC', 'RFG', 'RFV', 'XMLV', 'XMMO', 'XMHQ', 'XMVM', 'RWJ', 'EWSC', 'RZG', 'RZV', 'XSHD', 'XSLV', 'XSMO', 'XSHQ', 'XSVM', 'CSD', 'RDIV', 'IVSG', 'IVLC', 'VRP', 'CZA', 'BSAE', 'BSBE', 'BSCE', 'BSDE', 'PCY', 'PGHY', 'IHYF', 'PICB', 'PLW', 'BSCJ', 'BSJJ', 'BSCL', 'BSJL', 'BSML', 'BSCM', 'BSJM', 'BSMM', 'BSCN', 'BSJN', 'BSMN', 'BSCO', 'BSJO', 'BSMO', 'BSCP', 'BSJP', 'BSMP', 'BSCQ', 'BSJQ', 'BSMQ', 'BSCR', 'BSJR', 'BSMR', 'BSCS', 'BSJS', 'BSMS', 'BSCT', 'BSMT', 'BSCU', 'BSMU', 'PWZ', 'PHB', 'PFIG', 'IIGD', 'IIGV', 'PZA', 'PZT', 'PBTP', 'PBND', 'BAB', 'GTO', 'CLTL', 'GSY', 'PVI', 'VRIG']
        TICKERS = ['QQQ', 'SPHB', 'IBBQ','QQQM']
        print(f'[invesco.py] 已設定本次需爬取Ticker，數量：{len(TICKERS)}')
        log.processLog(f'[invesco.py] 已設定本次需爬取Tickers，數量：{len(TICKERS)}')
        countN = 0
        for ticker in TICKERS:
            if ticker not in ['QQQ', 'SPHB', 'IBBQ']:
                continue
            countN = countN+1
            check = False
            log.processLog(f'[invesco.py] {countN}/{len(TICKERS)}... 開始爬取{ticker}')
            print(f'[invesco.py] {countN}/{len(TICKERS)}... 開始爬取{ticker}')
            
            r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/product-detail/main/sidebar/0?audienceType=Investor&action=download&ticker={ticker}")
            count = 0
            while r.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    print(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    break
                count = count + 1
                print(f'[invesco.py] {ticker}：淨值表爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[invesco.py] {ticker}：淨值表爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/product-detail/main/sidebar/0?audienceType=Investor&action=download&ticker={ticker}")
            if check:
                continue
            else:
                df2 = pd.read_csv(StringIO(r.text))
                Date = datetime.strptime(df2.Date[0], '%m/%d/%Y').strftime("%Y-%m-%d")
                for i in range(0, 4):
                    if datetime.strptime(df2.Date[i], '%m/%d/%Y').weekday()!=6 and datetime.strptime(df2.Date[i], '%m/%d/%Y').weekday()!=5:
                        Date = datetime.strptime(df2.Date[i], '%m/%d/%Y').strftime("%Y-%m-%d")
                        break
                log.processLog(f'[invesco.py] {ticker}：淨值表爬取完成')
                print(f'[invesco.py] {ticker}：淨值表爬取完成')
                
            r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker={ticker}")
            count = 0
            while r.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    print(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    break
                count = count + 1
                print(f'[invesco.py] {ticker}：Shares/NetAsset爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[invesco.py] {ticker}：Shares/NetAsset爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'-----------------------------')
                time.sleep(5)
                r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker={ticker}")
            if check:
                continue
            else:
                try:
                    Shares = float(bs(r.text, features="lxml").select('ul li.clearfix div span.pull-right')[0].text.strip("M")) * 1000000
                    NetAsset = float(bs(r.text, features="lxml").select('ul li.clearfix div span.pull-right')[1].text.strip("M$").replace(",", "")) * 1000000
                except:
                    log.processLog(f'[invesco.py] {ticker}：Shares/NetAsset解析錯誤，此程序為邏輯錯誤，有空才會回來看，故忽略爬取此標的')
                    print(f'[invesco.py] {ticker}：Shares/NetAsset解析錯誤，此程序為邏輯錯誤，有空才會回來看，故忽略爬取此標的')
                    print_exc()
                    continue
                
                log.processLog(f'[invesco.py] {ticker}：Shares/NetAsset爬取完成')
                print(f'[invesco.py] {ticker}：Shares/NetAsset爬取完成')
            
            r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={ticker}")
            count = 0
            while r.status_code != 200:
                if count > 10:
                    check = True 
                    log.processLog(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    print(f'[invesco.py] {ticker}：10次連線失敗，忽略掉此標的')
                    break
                count = count + 1
                print(f'[invesco.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                log.processLog(f'[invesco.py] {ticker}：成分表爬取第{count}次連線失敗...5秒後重試')
                time.sleep(5)
                r = requests.get(f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={ticker}")
            if check:
                continue
            else:
                df = pd.read_csv(StringIO(r.text), skipinitialspace=True)
                log.processLog(f'[invesco.py] {ticker}：成分表爬取完成')
                print(f'[invesco.py] {ticker}：成分表爬取完成')

            path = f"{tail_path}/{Date}/invesco/"

            if not os.path.exists(path):
                log.processLog(f'[invesco.py] 尚無本日資料夾，建立：{path}')
                os.makedirs(f"{path}")

            if 'Market Value' in df.columns:
                df = df.rename(columns={'PercentageOfFund':'Weight', 'Shares/Par Value':'Shares', 'Holding Ticker':'Ticker'})
                mask = df.loc[:, 'Ticker'].isna()
                df.loc[mask, 'Ticker'] = df.loc[mask, 'Security Identifier']

            elif '$ Value' in df.columns:
                df = df.rename(columns={'$ Value':'Market Value', 'Identifier':'Ticker'})
                mask = df.loc[:, 'Ticker'].isna()
                df.loc[mask, 'Ticker'] = df.loc[mask, 'Security Identifier']

            elif 'Class of Shares' in df.columns:
                df = df.rename(columns={'MarketValue':'Market Value', 'Holding Ticker':'Ticker', 'Shares/Par Value':'Shares'})
                mask = df.loc[:, 'Ticker'].isna()
                df.loc[mask, 'Ticker'] = df.loc[mask, 'Security Identifier']

            elif 'Next_Call_Date' in df.columns:
                df = df.rename(columns={'MarketValue':'Market Value', 'Holding Ticker':'Ticker', 'Shares/Par Value':'Shares', 'PercentageOfFund':'Weight'})
                mask = df.loc[:, 'Ticker'].isna()
                df.loc[mask, 'Ticker'] = df.loc[mask, 'Security Identifier']
            df = df.reindex(columns = ['Ticker', 'Name', 'Currency', 'Shares', 'Market Value', 'Weight', 'Price',
                            'Multiplier', 'FX Rate']    )
            df.insert(loc=4, column='Expiry_month',value=['' for i in range(df.shape[0])])
            

            path = f"{tail_path}/{Date}/invesco/{ticker}_PCF_N_{Date}.csv"

            with open(path, "w",encoding='utf-8-sig',newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                spamwriter.writerow(['Date', Date])
                spamwriter.writerow(['Name', ticker])
                spamwriter.writerow(['Net Asset', NetAsset])
                spamwriter.writerow(['Shares Outstanding', Shares])
                spamwriter.writerow(['ETF Currency', 'USD'])
                spamwriter.writerow(['Nav per Share', df2.iloc[0,1]])
                spamwriter.writerow([])
            
            for value in df.values:
                if value[0] not in stock_list and value[0] not in future_list:
                    if '(' in str(value[1]) and ')' in str(value[1]):
                        pos = df['Name']==value[1]
                        df.loc[pos,'Ticker'] = value[1].split('(')[1].split(')')[0]
                df = df[df['Name']!='Net Other Assets / Cash']
            process_df(df,ticker)
            log.processLog(f'[invesco.py] {ticker}：本支ETF處理完畢')
            log.processLog(f'---------------------')
        
        end = time.perf_counter()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
        log.processLog('==============================================================================================')
    except:
        log.processLog(f'[invesco.py] {ticker}：本次爬蟲出現嚴重錯誤，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(ticker,"---------------------")
