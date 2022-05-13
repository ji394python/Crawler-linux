import os 
import pandas as pd
import sys 
import log_manager as log 
from traceback import print_exc,format_exc
import json
import time 


if __name__ == '__main__':
    
    log.processLog('==============================================================================================')
    log.processLog(f'【執行ETF_US新聞爬蟲專案】 {os.path.basename(__file__)}')
    
    try:
        #計時開始
        start = time.perf_counter() 
        tail_path = json.load(open(f"path.json",'r'))
        tail_path = tail_path['NasPublic']
        
        keyword = json.load(open(f'keyword.json','r'))
        # tail_path = tail_path['NasHome']
        
        col = ['AnnounceDate','Ticker','Action','Title','Link','EventDate','Source','Category']
        invesco = pd.read_csv(f"{tail_path}/invesco.csv",encoding='utf-8-sig')
        ishares = pd.read_csv(f"{tail_path}/ishares.csv",encoding='utf-8-sig')
        proshares = pd.read_csv(f"{tail_path}/proshares.csv",encoding='utf-8-sig')
        states = pd.read_csv(f"{tail_path}/states.csv",encoding='utf-8-sig')
        direxion = pd.read_csv(f"{tail_path}/direxion.csv",encoding='utf-8-sig')
        bny = pd.read_csv(f"{tail_path}/Bny.csv",encoding='utf-8-sig')
        bny['Ticker'] = ''
        bny['Action'] = ''
        bny['EventDate'] = ''
        bny['Source'] = 'BNY'
        bny['Category'] = 'ADR'
        bny = bny[['Date','Ticker','Action', 'Title', 'URL','EventDate','Source','Category']]
        bny.columns = col
        city = pd.read_csv(f"{tail_path}/City.csv",encoding='utf-8-sig')
        city['EventDate'] = ''
        city['Source'] = 'City'
        city['Category'] = 'ADR'
        city = city[['Date of Notice','Ticker','Corporate Action Type', 'Company', 'URL','EventDate','Source','Category']]
        city.columns = col
        deutsche = pd.read_csv(f"{tail_path}/Deutsche.csv",encoding='utf-8-sig')
        deutsche['EventDate'] = ''
        deutsche['Source'] = 'Deutsche'
        deutsche['Category'] = 'ADR'
        deutsche = deutsche[['Date','Ticker','ActionTypeName', 'Company', 'URL','EventDate','Source','Category']]
        deutsche.columns = col
        morgan = pd.read_csv(f"{tail_path}/JP_Morgan.csv",encoding='utf-8-sig')
        morgan['Source'] = 'JP.Morgan'
        morgan['Category'] = 'ADR'
        morgan = morgan[['AnnounceDate','Ticker','Action', 'Title', 'URL','EventDate','Source','Category']]
        morgan.columns = col
        df_adr = pd.concat([bny,city,deutsche,morgan])
        
        log.processLog(f'[News_Table.py] 讀取ETF/ADR爬蟲資料完成...執行萃取關鍵字程序')
        print(f'[News_Table.py] 讀取ETF/ADR爬蟲資料完成...執行萃取關鍵字程序')

        # check = ['Split', 'Dividend', 'Distribution', 'Launch', 'Lineup', 'Terminate', 'Halt', 'Liquidate', 'Expose', 'Suspend', 'Creation', 'Redemption', 'Conver']

        df_all = pd.concat([invesco,ishares,proshares,states,direxion])[['Date','Title', 'URL', 'Issuer']]
        df_all['Action'] = ''
        df_all['EventDate'] = ''
        df_all['Ticker'] = ''
        df_all['Category'] = 'ETF'
        df_all = df_all[['Date','Ticker','Action','Title','URL', 'EventDate', 'Issuer','Category']]
        df_all.columns = col
        Keys = []
        df_all = pd.concat([df_all,df_adr])
        for v in df_all['Title'].values:
            string = ''
            for k,e in keyword.items():
                key = e['Key']
                check = e['Multi Check']
                if len(key) != 0:
                    for word in key:
                        if word.lower() in v.lower():
                            string = string + f',{k}'
                if len(check) != 0:
                    multiWork_bool = False 
                    for word in check:
                        if word.lower() in v.lower():
                            multiWork_bool = True 
                        else:
                            multiWork_bool = False 
                            break
                    if multiWork_bool:
                         string = string + f',{k}'
            if len(string) > 0:
                Keys.append(string[1:])
            else:
                Keys.append(string)
        df_all.insert(3,'Keyword',Keys)
        df_all.sort_values(['AnnounceDate'],ascending=[False],inplace=True)
        df_all = df_all[['AnnounceDate','EventDate','Ticker','Action','Keyword','Title','Source','Category','Link']]
        df_all.to_csv(f'{tail_path}/News_Tables.csv',encoding='utf-8-sig',index=False)
        
        log.processLog(f'[News_Table.py] 輸出：{tail_path}/News_Tables.csv')
        print(f'[News_Table.py] 輸出：{tail_path}/News_Tables.csv')
        
        end = time.perf_counter()
        log.processLog(f'【結束程序】 {os.path.basename(__file__)} - 執行時間:{(end-start)}')
        log.processLog('==============================================================================================')
    
    except Exception as e:
        log.processLog(f'[News_Table.py] 本次ETF新聞合併程序遇到未知問題，請查看錯誤log檔')
        log.processLog(f'---------------------')
        log.errorLog(f'{format_exc()}')
        print(e)
    