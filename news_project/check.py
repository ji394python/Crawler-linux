# -*- coding: utf-8 -*-
"""
Author: Denver Liu

Last Updated: 2021/07/08

用來對答案用 (程式運行用不到)

"""
import pandas as pd
from datetime import datetime
import time

holidayList = pd.read_csv('predata/holiday.csv')['date'].apply(lambda x: time.strftime('%Y/%m/%d',time.strptime(x,'%Y/%m/%d'))) 
workdayList = pd.read_csv('predata/workday.csv')['date'].apply(lambda x: time.strftime('%Y/%m/%d',time.strptime(x,'%Y/%m/%d'))) 

df_up = pd.read_csv('資料/上市.csv',encoding='cp950')
df_stock = pd.read_csv('資料/上櫃.csv',encoding='cp950')

df_up.columns = ['股票代號','股票名稱','停券起日','停券迄日','原因']
df_up.fillna(0,inplace=True)
for i in df_up.iterrows():
    row = i[1]
    if row['停券迄日'] == 0:
        target = row['停券起日'].split('.')
        target = time.strptime(f'{int(target[0])+1911}.01.06','%Y.%m.%d')
        time_stamp = int(time.mktime(target))
        count = 0
        while count < 3:
            time_stamp += 86400
            cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
            if str(cache) in holidayList:
                pass
            elif str(cache) in workdayList:
                count +=1
            elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
                pass
            else:
                count += 1
        cache_list = cache.split('/')
        end_date = str(int(cache_list[0]) - 1911) + '/' + cache_list[1] + '/' + cache_list[2]
        df_up.iloc[i[0],]['停券起日'] = row['停券起日'].replace('.','/')
        df_up.iloc[i[0],]['停券迄日'] = end_date
    else:
        df_up.iloc[i[0],]['停券起日'] = row['停券起日'].replace('.','/')
        df_up.iloc[i[0],]['停券迄日'] = row['停券迄日'].replace('.','/')
#df_up['停券起日'] = df_up['停券起日'].apply(lambda x: str(int(x.split('.')[0])+1911)+'/'+x.split('.')[1]+'/'+x.split('.')[2])
#df_up['停券迄日'] = df_up['停券迄日'].apply(lambda x: str(int(x.split('.')[0])+1911)+'/'+x.split('.')[1]+'/'+x.split('.')[2])


df_stock.columns = ['股票代號', '股票名稱', '停券起日','停券迄日', '原因']
df_stock.fillna(0,inplace=True)
df_stock_main = df_stock[( df_stock['停券起日']!= 0) & (df_stock['停券迄日']!=0 )]
df_stock_sub = df_stock[( df_stock['停券起日']== 0) & (df_stock['停券迄日']==0 )]

for i in df_stock_main.iterrows():
    row = i[1]
    if row['停券起日'] == 0:
        target = row['停券迄日'].split('.')
        target = time.strptime(f'{int(target[0])+1911}.01.06','%Y.%m.%d')
        time_stamp = int(time.mktime(target))
        count = 0
        while count < 3:
            time_stamp += 86400
            cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
            if str(cache) in holidayList:
                pass
            elif str(cache) in workdayList:
                count +=1
            elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
                pass
            else:
                count += 1
        cache_list = cache.split('/')
        end_date = str(int(cache_list[0]) - 1911) + '/' + cache_list[1] + '/' + cache_list[2]
        df_stock_main.iloc[i[0],]['停券起日'] = end_date
        df_stock_main.iloc[i[0],]['停券迄日'] = row['停券迄日'].replace('.','/')
    if row['停券迄日'] == 0:
        target = row['停券起日'].split('.')
        target = time.strptime(f'{int(target[0])+1911}.01.06','%Y.%m.%d')
        time_stamp = int(time.mktime(target))
        count = 0
        while count < 3:
            time_stamp += 86400
            cache = time.strftime('%Y/%m/%d', time.localtime(time_stamp))
            if str(cache) in holidayList:
                pass
            elif str(cache) in workdayList:
                count +=1
            elif datetime.strptime(cache, '%Y/%m/%d').weekday() in [5,6]:
                pass
            else:
                count += 1
        cache_list = cache.split('/')
        end_date = str(int(cache_list[0]) - 1911) + '/' + cache_list[1] + '/' + cache_list[2]
        df_stock_main.iloc[i[0],]['停券起日'] = row['停券起日'].replace('.','/')
        df_stock_main.iloc[i[0],]['停券迄日'] = end_date
df_stock = pd.concat([df_stock_main,df_stock_sub],axis=0)

df = pd.concat([df_up,df_stock],axis=0,ignore_index=True)

df = df.drop_duplicates(['股票代號','股票名稱','停券起日','停券迄日'])
df.groupby(['股票代號','股票名稱','停券起日','停券迄日']).filter(lambda df:df.shape[0] > 1)
df.groupby([...], group_keys=False).apply(lambda df:df if df.shape[0] > 1 else None) #只保留重複
df.to_excel('上市櫃測資.xlsx',index=False)

df_stock.sor

df_data = pd.read_csv('../../ShareDiskE/News_Stocks/print/data.csv')
x = list(set(df_data.iloc[:,1].values.tolist()))
y = list(set(df_stock.iloc[:,1].values.tolist()))
z = list(set(df_up.iloc[:,1].values.tolist()))
x = [ i.strip() for i in x]
y = [ i.strip() for i in y]
z = [ i.strip() for i in z]
df_up['股票名稱'] = df_up['股票名稱'].apply(lambda x:x.strip())
temp = pd.DataFrame()
for i in range(len(z)):
    if z[i] in x:
        pass
    else:
        last = df_up[df_up['股票名稱']==z[i]].sort_values('停券起日',ascending=False)
        temp = pd.concat([temp,last])
        print("上市不在:",z[i],last.iloc[0,:]['停券起日'])

for i in range(len(y)):
    if y[i] in x:
        pass
    else:
        last = df_stock[df_stock['股票名稱']==z[i]].sort_values('停券起日',ascending=False)
        temp = pd.concat([temp,last])
        print("上櫃不在:",y[i])



z = [ i.strip() for i in z]

main = df_data[['停券起日','停券迄日']].values.tolist()
main1 = [ i[0] for i in main]
main2 = [ i[1] for i in main]
sub = df_up[['停券起日','停券迄日']].values.tolist()
sub1 = [ i[0] for i in sub]
sub2 = [ i[1] for i in sub]

for i in range(len(main)):
    if main1[i] in sub1:
        answer = sub2[sub1.index(main1[i])]
        if answer != main2[i]:
            print(main1[i],main2[i],answer,i)


## 股東常會 股息同時招開
df = pd.read_excel('重複資料.xlsx',engine='openpyxl',index=False)
df.groupby(['股票代號','股票名稱','停券起日','停券迄日']).filter(lambda df:df.shape[0] > 1).sort_values('股票名稱').to_csv('重複資料(整理).csv',index=False,encoding='cp950')
df.columns



##輸出上市櫃合併資料
df.to_csv('上市櫃.csv',index=False,encoding='cp950')

## Q1. data裡面有但上市櫃無
data = pd.read_csv(r'C:\Users\chiaming\Documents\GitHub\ShareDiskE\News_Stocks\print\data.csv')
data_name = list(set(data.iloc[:,1].values.tolist()))
len(data_name)
df_name = list(set(df.iloc[:,1].apply(lambda x: str(x).strip()).values.tolist()))
len(df_name)
df_stock_name = list(set(df_stock.iloc[:,1].apply(lambda x: str(x).strip()).values.tolist()))
len(df_stock_name)
df_up_name = list(set(df_up.iloc[:,1].apply(lambda x: str(x).strip()).values.tolist()))
len(df_up_name)

ignore_stock_name = []
ignore_up_name = []
ignore_name = []
for i in data_name:
    if i not in df_name:
        ignore_name.append(i)

for i in df_stock_name:
    if i not in data_name:
        print(i)
    

data[data['股票名稱'].isin(ignore_name)].to_csv('data有蛋上市櫃沒有.csv',index=False,encoding='cp950')
data[data['股票名稱'].isin(ignore_name)==False]
data.drop_duplicates(['停券起日','停券迄日']).sort_values('停券起日').to_csv('data照日期牌.csv',index=False,encoding='cp950')
data.drop_duplicates(['停券起日'])


df.drop_duplicates(['停券起日','停券迄日']).sort_values('停券起日').to_csv('上市櫃照日期牌.csv',index=False,encoding='cp950')
df['停券起日'] = df['停券起日'].apply(lambda x:str(x)).values.tolist()
df.drop_duplicates(['停券起日'])


df_up.drop_duplicates(['停券起日','停券迄日'])
df_up.drop_duplicates(['停券起日'])



data_date = dict(zip(data.iloc[:,2],df.iloc[:,3]))
df_date = dict(zip(df.iloc[:,2],data.iloc[:,3]))

for k,v in data_date.items():
    if k[:3] in ['99','100','101','102','103']:
        continue
    elif k not in df_date.keys():
        print('日期不存在',k)
    else:
        if v != df_date[k]:
            if v == 0:
                continue
            elif v[:3] in ['101','102','103']:
                continue
            else:
                print(f'日期錯誤：原始_{k}_{v} / 答案_{k}_{df_date[k]}')
        else:
            continue

data[data.iloc[:,2]=='107/07/17']