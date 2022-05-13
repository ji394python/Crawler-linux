# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 11:00:47 2022
Last edited：2022/04/29

@author: Administrator、Denver
"""
import pandas as pd
## NAV Source: Public/PCF_Data
## preC Source: Yahoo Finance API
import yfinance as yf
from datetime import datetime
import csv
# import datetime
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay
import json
import os 
from traceback import print_exc

# most_recent = today - timedelta
def PCF_Reader(issuer, sym, date):
    record_date="NA"
    nav='NA'
    try:
        with open("../../NasPublic/PCF_Data/{}/{}/{}_PCF_N_{}.csv".format(date, issuer, sym, date), "r",encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter="\t")
            for i, line in enumerate(reader):
                if line[0].split(",")[0]=="Date":
                    record_date=line[0].split(",")[1]
                    # print ('line[{}] = {}'.format(i, record_date))
                elif line[0].split(",")[0]=="Nav per Share":
                    nav=line[0].split(",")[1]
                    print ('{} @ {} finished'.format(sym, date))
                    return (issuer,sym, record_date, nav)
                # df.loc[len(df.index)] = [sym, record_date, nav]
                    break
                if i>=6:
                    print ('{} @ {} @ {} not finding nav, break'.format(sym, date, issuer))
                    return (issuer,sym, record_date, nav)
    except:
        print("not finding file {} @ {} @ {}".format(sym, date, issuer))
        return (issuer,sym, record_date, nav)
        
# Fetching NAV from Public\PCF_Data      

if __name__ == "__main__":
    
    try:
        temp=pd.read_csv('ETF_List.csv')
        
        today = datetime.today()
        date=((today - BDay(1)).strftime('%Y-%m-%d'))
        dateStart = ((today - BDay(4)).strftime('%Y-%m-%d'))
        Date = pd.date_range(start=dateStart, end=date)
        
        for d in Date[1:]:
            d =  d.strftime('%Y-%m-%d')
            df=pd.DataFrame(columns=['Issuer','Ticker', 'NAV Date', 'preNAV'])
            # date='2022-04-22'
            # date=datetime.today().strftime('%Y-%m-%d')
            for index, row in temp.iterrows():
                issuer=(row['US Issuer']).lower().replace(" ", "")
                sym=(row['US Ticker'])
                (issue,sym, record_date, nav)=PCF_Reader(issuer, sym, d)
                df.loc[len(df.index)] = [issue,sym, record_date, nav]
                # print((row['US Issuer']).lower().replace(" ", ""))
                
            for index, row in temp[['ORD Ticker','ORD Issuer']].drop_duplicates().iterrows():
                issuer=(row['ORD Issuer']).lower().replace(" ", "")
                sym=(row['ORD Ticker'])
                (issue,sym, record_date, nav)=PCF_Reader(issuer, sym, d)
                df.loc[len(df.index)] = [issue,sym, record_date, nav]

            ## 0506補充程序
            dire = False 
            ishar = False 
            if os.path.exists(f'direxion_{d}_temp.csv'):
                direxion = pd.read_csv(f'direxion_{d}_temp.csv',encoding='utf-8-sig')
                dire = True
            elif os.path.exists(f'ishares_{d}_temp.csv'):
                ishares = pd.read_csv(f'ishares_{d}_temp.csv',encoding='utf-8-sig')
                ishar = True
                
            for k,v in df.iterrows():
                name = v['Issuer']
                tick = v['Ticker']
                preNAV = v['preNAV']
                if name == 'direxion' and preNAV == 'NA' and dire:
                    if len(direxion[direxion['Ticker']==tick]) != 0:
                        df.loc[k,'preNAV'] = direxion[direxion['Ticker']==tick]['NAV'].values[0]
                        df.loc[k,'NAV Date'] = direxion[direxion['Ticker']==tick]['Date'].values[0]
                elif name == 'ishares' and preNAV == 'NA' and ishar:
                    if len(ishares[ishares['Ticker']==tick]) != 0:
                        df.loc[k,'preNAV'] = ishares[ishares['Ticker']==tick]['NAV'].values[0]
                        df.loc[k,'NAV Date'] = ishares[ishares['Ticker']==tick]['Date'].values[0]
                else:
                    continue
            
            
            # Fetching preC from Yahoo Finance
            # date=datetime.today().strftime('%Y-%m-%d')
            df_C=pd.DataFrame(columns=['Ticker', 'Close Date', 'preC'])
            # date="2022-02-25"
            for sym in df['Ticker']:
                # preC="NA"
                # C_date="NA"
                # sym='LBJ'
                print("downloading {} from Yahoo finance".format(sym))
                data = yf.download(sym, start=Date[0].strftime('%Y-%m-%d'), end=(datetime.strptime(d,'%Y-%m-%d') + BDay(1)).strftime('%Y-%m-%d') )
                try:
                    C_date=data.index[-1].strftime('%Y-%m-%d')
                    preC=data.Close[-1]
                except:
                    print ("not finding file {} @ Yahoo Finance".format(sym))
                    preC="NA"
                    C_date="NA"
                    # continue
                
                df_C.loc[len(df_C.index)] = [sym, C_date, preC]
            df_all = pd.merge(df, df_C, on='Ticker')
            perc = []
            for i,v in df_all.iterrows():
                if v['preNAV'] == 'NA' or v['preC'] == 'NA':
                    perc.append('NA')
                else:
                    nav = float(v['preNAV'])
                    close = float(v['preC'])
                    if abs((nav-close)/nav) <= 0.01:
                        perc.append("TRUE")
                    else:
                        perc.append("False")
                    
            df_all['check_1%'] = perc
            if not os.path.exists('../../NasPublic/NAV_Table/'):
                os.makedirs(f"../../NasPublic/NAV_Table/")
            df_all.drop_duplicates(inplace=True)
            df_all = df_all[['Ticker', 'NAV Date', 'preNAV', 'Close Date', 'preC','check_1%','Issuer']]
            df_all.to_csv('../../NasPublic/NAV_Table/US_NAV_{}.csv'.format(d),index=False)
            print(f'輸出成功：{date}')
    except:
        print_exc()
        import pdb; pdb.set_trace()