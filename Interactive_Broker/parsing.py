import pandas as pd
import os 
import csv
from datetime import datetime,timedelta
import time 
from dateutil import tz
#- 未標準化檔案：  Shortable\IB\{Country}\ {Date}\Base \{Country}_Shortable_{YY-MM-DD}_{HH:MM:SS}.csv
#- 標準化檔案： Shortable\IB\{Country}\ {Date}\Timeseies \{Ticker}_{Country}_Shortable_{Date}.csv
def pathControl(path:str):
    
    def dirCreate(path:str):
        if os.path.exists(path)==False:
            os.mkdir(path) #建立base目錄\
    length = path.split('/') #parsing path

    if len(length) == 1: 
        if length[0].find('.') != -1:
            return('')
        dirCreate(length[0])
        return('')

    bool = 1 if length[-1].find('.') != -1 else 1
    length = length[:len(length)-bool]
    temp = length[0]
    dirCreate(temp)

    for i in length[1:]:
        temp = temp + '/' + i
        dirCreate(temp)
        

if __name__ == '__main__':
    basePath = 'Shortable/IB'
    outputHeader = ['Machine Time', 'Unix Time','SYM', 'CUR', 'NAME', 'CON', 'ISIN', 'REBATERATE', 'FEERATE','AVAILABLE']
    gmt = str(datetime.now())[:10]
    dfCheck = pd.read_csv('date.csv')
    checkDate = list(set(dfCheck['date'].values))
    storeDate = checkDate.copy()
    for countryFold in os.listdir(basePath):
        country = basePath + '/' + countryFold
        dateList = os.listdir(country)
        for date in dateList:
            if date.find('.') != -1: continue 
            if date in storeDate: continue
            if date == gmt: continue
            storeDate.append(date)
            header = ['SYM', 'CUR', 'NAME', 'CON', 'ISIN', 'REBATERATE', 'FEERATE','AVAILABLE','Machine Time','Unix Time']
            rowStore = []
            fileList = os.listdir(country+'/'+date+'/Base')
            for file in fileList:
                if file.find('.') == -1 : continue 
                temp = pd.read_csv(country+'/'+date+'/Base/'+file)
                d = file[file.find('Shortable')+10:file.find('.csv')]
                date = datetime.strptime(d,'%Y-%m-%d_%H-%M-%S')
                machine = date.strftime('%Y/%m/%d %H:%M:%S')
                unix = time.mktime(date.timetuple())
                pathD = machine[:10].replace('/','-') #儲存路徑使用
                for row in temp.iterrows():
                    if row[1]['SYM'] == '#EOF' : 
                        break
                    row = list(row[1].values)
                    row.extend([machine,unix])
                    rowStore.append(row)
                df = pd.DataFrame(rowStore,columns=header)
                df.sort_values('Machine Time',inplace=True)
                tickerSet = set(df['SYM'])
                for i in tickerSet:
                    temp_df_ticker = df[df['SYM']==i][outputHeader]
                    pathControl(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv")
                    temp_df_ticker.to_csv(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv",index=False,encoding='big5')

    dfCheck = pd.DataFrame({'date':storeDate})
    dfCheck.drop_duplicates('date',inplace=True)
    dfCheck.to_csv('date.csv',index=False)

                    # for d in dateSet:
                    #     if ((d.replace('/','-') == gmt) | (d.replace('/','-')  in checkDate)): continue
                    #     storeDate.append(d.replace('/','-'))
                    #     temp_df_ticker_date = temp_df_ticker[temp_df_ticker['date']==d]
                    #     temp_df_ticker_date = temp_df_ticker_date[outputHeader]
                    #     pathD = d.replace('/','-')
                    #     pathControl(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv")
                    #     temp_df_ticker_date.to_csv(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv",index=False,encoding='big5')



        # fileList = os.listdir(country)
        # header = ['SYM', 'CUR', 'NAME', 'CON', 'ISIN', 'REBATERATE', 'FEERATE','AVAILABLE','Machine Time','Unix Time']
        # rowStore = []
        # for file in fileList:
        #     if file.find('.') == -1: continue
        #     temp = pd.read_csv(country+'/'+file)
        #     d = file[file.find('Shortable')+10:file.find('.csv')]
        #     date = datetime.strptime(d,'%Y-%m-%d_%H-%M-%S')
        #     machine = date.strftime('%Y/%m/%d %H:%M:%S')
        #     unix = time.mktime(date.timetuple())
        #     for row in temp.iterrows():
        #         if row[1]['SYM'] == '#EOF' : 
        #             break
        #         row = list(row[1].values)
        #         row.extend([machine,unix])
        #         rowStore.append(row)
        # df = pd.DataFrame(rowStore,columns=header)
        # df.sort_values('Machine Time',inplace=True)
        # tickerSet = set(df['SYM'])
        # for i in tickerSet:
        #     temp_df_ticker = df[df['SYM']==i]
        #     #temp_df_ticker = df[df['SYM']=='IXUS']
        #     temp_df_ticker['date'] = temp_df_ticker['Machine Time'].apply(lambda x: x.split(' ')[0])
        #     dateSet = set(temp_df_ticker.date.values)
        #     for d in dateSet:
        #         if ((d.replace('/','-') == gmt) | (d.replace('/','-')  in checkDate)): continue
        #         storeDate.append(d.replace('/','-'))
        #         temp_df_ticker_date = temp_df_ticker[temp_df_ticker['date']==d]
        #         temp_df_ticker_date = temp_df_ticker_date[outputHeader]
        #         pathD = d.replace('/','-')
        #         pathControl(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv")
        #         temp_df_ticker_date.to_csv(f"Shortable/IB/{countryFold}/{pathD}/Timeseries/{i}_{countryFold}_Shortable_{pathD}.csv",index=False,encoding='big5')

#Shortable\IB\{Country}\{Date}\{Ticker}_{Country}_Shortable_{Date}.csv
#Shortable\IB\{Country}\{Country}_Shortable_{YY-MM-DD}_{HH:MM:SS}.csv

