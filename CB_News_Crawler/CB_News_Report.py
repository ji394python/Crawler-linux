import pandas as pd 
import json
import os
import log_manager as log
from copy import deepcopy
import re
from datetime import datetime
import csv
import time
import traceback

def transferDate(dateString: str) -> datetime.timetuple:
    '''
            <Ex.> 97/06/13 -> 2008
    '''
    try:
        dateList = [int(num) for num in dateString.split('/')]
        dateList[0] += 1911
        return datetime(*dateList).date()
    except:
        return None

if __name__ == '__main__':

    #路徑設定：Folder、CSV
    rootPath = json.load(open('set.json','r+'))['output_dir_path']
    output_dir_path = f'{rootPath}/News_CB' #專案根路徑
    output_dir_path_csv = f'{output_dir_path}/Standard'
    output_file_path_adj = f'{output_dir_path_csv}/CB_Strike_Adjust.csv'   #價格調整
    output_file_path_hal = f'{output_dir_path_csv}/CB_Conversion_Halt.csv' #停止轉換
    output_file_path_red = f'{output_dir_path_csv}/CB_Redemption.csv'      #強制贖回
    output_file_path_date = f'{output_dir_path_csv}/done_date.csv'      #強制贖回
    
    log.processLog('【開始執行CB_News爬蟲專案】正規化程序 CB_report.py')
    log.processLog(f"本次根路徑：{rootPath}")
    start = time.perf_counter_ns()
    try:

        if not  os.path.exists(output_dir_path_csv):
            log.processLog(f'創立資料夾：{output_dir_path_csv}')
            os.makedirs(output_dir_path_csv)
        
        if not os.path.exists(output_file_path_adj):
            with open(output_file_path_adj, 'a', newline='',encoding='utf-8-sig') as csvfile:
                log.processLog(f'價格調整檔不存在...創立檔案：{output_file_path_adj}')
                writer = csv.writer(csvfile)
                writer.writerow(['公告日','可轉債名稱','可轉債代號','調整生效日','原訂轉換價','調整後轉換價'])
                csvfile.close()
        
        if not os.path.exists(output_file_path_hal):
            with open(output_file_path_hal, 'a', newline='',encoding='utf-8-sig') as csvfile:
                log.processLog(f'停止轉換檔不存在...創立檔案：{output_file_path_hal}')
                writer = csv.writer(csvfile)
                writer.writerow(['公告日','可轉債名稱','可轉債代號','停止轉換起日','停止轉換迄日'])
                csvfile.close()
        
        if not os.path.exists(output_file_path_red):
            with open(output_file_path_red, 'a', newline='',encoding='utf-8-sig') as csvfile:
                log.processLog(f'檔案不存在...創立檔案：{output_file_path_red}')
                writer = csv.writer(csvfile)
                writer.writerow(['公告日','可轉債名稱','可轉債代號','強制贖回日','最後轉換日'])
                csvfile.close()

        if not os.path.exists(output_file_path_date):
            with open(output_file_path_date, 'a', newline='',encoding='utf-8-sig') as csvfile:
                log.processLog(f'檔案不存在...創立檔案：{output_file_path_red}')
                writer = csv.writer(csvfile)
                writer.writerow(['date'])
                csvfile.close()
        

        #讀取已剖析過的日期記錄檔
        log.processLog('讀取日期資料檔: done_date.csv')
        completeDate = pd.read_csv(output_file_path_date)['date'].values.tolist()
        completeDate = [ i.replace('/','-') for i in completeDate]
        #這樣才能夠去排除申報日期大於公告日期的問題
        completeDate = [ str(datetime.strptime(i,'%Y-%m-%d'))[:10] if datetime.strptime(i,'%Y-%m-%d') < datetime.now() else '1970-01-01' for i in completeDate] 

        #讀取公司債公告路徑
        filePath,dateProc,bondName,bondCode = [],[],[],[]
        for iterFoldDate in os.listdir(output_dir_path):
            if iterFoldDate=='Standard': continue #忽略輸出資料夾
            if iterFoldDate.find('.csv')!=-1: continue #忽略輸出資料夾
            #print(iterFoldDate,iterFoldDate in completeDate,completeDate) 可以驗證done_date的日期
            if iterFoldDate in completeDate: continue #忽略已剖析的日期
            print(iterFoldDate)
            #寫入新剖析的日期於done_date
            with open(output_file_path_date, 'a', newline='',encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([iterFoldDate])
                csvfile.close()

            for iterFile in os.listdir(f'{output_dir_path}/{iterFoldDate}'):
                temp = f'{output_dir_path}/{iterFoldDate}/{iterFile}'
                print(temp)
                filePath.append(temp)
                dateProc.append(iterFoldDate)
                with open(temp,encoding='utf-8-sig') as f:
                    content  = f.read()
                    pos1 = content.find('簡稱')
                    pos2 = content.find(')',pos1)
                    bond_info = content[pos1:pos2]
                    bond_name = bond_info[bond_info.find('：')+1:bond_info.find('，')]
                    bond_code = bond_info[bond_info.find('代碼：')+3:]
                    bondName.append(bond_name)
                    bondCode.append(bond_code)

        DT = pd.DataFrame(dict(zip(['file_path','date','name','id_hat'],[filePath,dateProc,bondName,bondCode])))
        
        if len(DT) != 0:
            DT1 = deepcopy(DT)
            AdjPubDate,AdjDate,AdjPriceO,AdjPriceA,AdjName,AdjCode = [],[],[],[],[],[]
            RedPubDate,RedDate,RedName,RedCode,RedLastDate = [],[],[],[],[]
            HalStartDate,HalEndDate,HalPubDate,HalName,HalCode, = [],[],[],[],[]
            for i in range(len(DT1)):
                with open(DT1.iloc[i,0],'r',encoding='utf-8-sig') as f:
                #with open('../../NasHome/News_CB/2021-01-06/3346_麗清_2021-01-06_1.txt','r+',encoding='utf-8-sig') as f:
                    test = f.readlines()
                    test_f = []
                    test_temp = []
                    for row in test:
                        if row[:2] == '一、':
                            test_temp = row
                        elif row[:2] not in ['一、','二、','三、','四、','五、','六、','七、','八、','九、']:
                            test_temp = test_temp + row
                        elif row[:2] in ['二、','三、','四、','五、','六、','七、','八、','九、']:
                            test_f.append(test_temp)
                            test_temp = row
                    test_f.append(test_temp)
                    test = test_f

                for newR in test:
                    if newR.find('二、主旨') != -1:
                        if newR.find('轉換價格') != -1:
                            AdjpubDate = os.path.dirname(DT1.iloc[i,0])
                            AdjpubDate = AdjpubDate[AdjpubDate.rfind('/')+1:].replace('-','/') #發布日期
                            log.processLog(f'{AdjpubDate}：轉換價格程序 - 剖析{DT1.iloc[i,0]}')
                            #print(f'進入轉換價格程序:{DT1.iloc[i,0]}')
                            adjDate = re.findall('([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日',newR)[0] #調整生效日
                            adjDate = adjDate[0] + '/' + adjDate[1] + '/' + adjDate[2]
                            adjDate = transferDate(adjDate)
                            adjDate = str(adjDate).replace('-','/') #調整日期
                            AdjpriceO = re.findall('價格自([0-9.]+)元',newR)[0] #原定轉換價
                            AdjpriceA = re.findall('調整為([0-9.]+)元',newR)[0] #原定轉換
                            Adjname = re.findall('簡稱：(.*?)，',newR)[0] #公司債名稱
                            Adjcode = re.findall('代碼：(.*?)\)',newR)[0] #公司債代碼
                            AdjPubDate.append(AdjpubDate)
                            AdjDate.append(adjDate)
                            AdjPriceO.append(AdjpriceO)
                            AdjPriceA.append(AdjpriceA)
                            AdjName.append(Adjname)
                            AdjCode.append(Adjcode)
                        if newR.find('終止櫃檯買賣') != -1:
                            if newR.find('已全數轉換為普通股') != -1:continue
                            RedpubDate = os.path.dirname(DT1.iloc[i,0])
                            RedpubDate = RedpubDate[RedpubDate.rfind('/')+1:].replace('-','/') #發布日期
                            log.processLog(f'{RedpubDate}：強制贖回程序 - 剖析{DT1.iloc[i,0]}')
                            #print(f'進入強制贖回程序:{DT1.iloc[i,0]}')
                            redDate = re.findall('([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日',newR)[0] #調整生效日
                            redDate = redDate[0] + '/' + redDate[1] + '/' + redDate[2]
                            redDate = transferDate(redDate)
                            redDate = str(redDate).replace('-','/') #調整日期
                            Redname = re.findall('簡稱：(.*?)，',newR)[0] #公司債名稱
                            Redcode = re.findall('代碼：(.*?)\)',newR)[0] #公司債代碼
                            RedPubDate.append(RedpubDate)
                            RedDate.append(redDate)
                            RedName.append(Redname)
                            RedCode.append(Redcode)
                            index = test.index(newR)
                            RedlastDate_keep = ''
                            for p in test[index+1:]:
                                if len(RedlastDate_keep) == 0: 
                                    RedlastDate = re.findall('最遲應於([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日',p,re.S)
                                    if len(RedlastDate) == 0:
                                        RedlastDate = re.findall('最遲應於民國([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日',p,re.S)
                                    if len(RedlastDate) != 0:
                                        RedlastDate = RedlastDate[0][0] + '/' + RedlastDate[0][1] + '/' + RedlastDate[0][2]
                                        RedlastDate = transferDate(RedlastDate)
                                        RedlastDate = str(RedlastDate).replace('-','/') #最後轉換日
                                        RedlastDate_keep = RedlastDate
                                    else:
                                        RedlastDate == ''
                                    
                            RedLastDate.append(RedlastDate)

                        
                        if ( (newR.find('停止受理轉換') != -1) | (newR.find('停止受理認購') != -1) ):
                            HalpubDate = os.path.dirname(DT1.iloc[i,0])
                            HalpubDate = HalpubDate[HalpubDate.rfind('/')+1:].replace('-','/') #發布日期
                            log.processLog(f'{HalpubDate}：停止轉換程序 - 剖析{DT1.iloc[i,0]}')
                            #print(f'進入停止程序:{DT1.iloc[i,0]}')
                            Halname = re.findall('簡稱：(.*?)，',newR)[0] #公司債名稱
                            Halcode = re.findall('代碼：(.*?)\)',newR)[0] #公司債代碼
                            index = test.index(newR)
                            for p in test[index+1:]:
                                if ((p.find('四、公告事項') != -1) & (p.find('停止受理轉換登記起訖日期')!=-1) ):
                                    #print(p)
                                    HalDate = re.findall('停止受理轉換登記起訖日期：(.*?)。',p,re.S)[0].replace('\n','') 
                                    HalDate = re.findall('([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日',HalDate)
                                    HalstartDate = HalDate[0][0] + '/' + HalDate[0][1] + '/' + HalDate[0][2]
                                    HalstartDate = transferDate(HalstartDate)
                                    HalstartDate = str(HalstartDate).replace('-','/') #停止轉換起日
                                    HalendDate = HalDate[1][0] + '/' + HalDate[1][1] + '/' + HalDate[1][2]
                                    HalendDate = transferDate(HalendDate)
                                    HalendDate = str(HalendDate).replace('-','/') #停止轉換起日
                            HalStartDate.append(HalstartDate)
                            HalEndDate.append(HalendDate)
                            HalPubDate.append(HalpubDate)
                            HalName.append(Halname)
                            HalCode.append(Halcode)

                            


            rows = zip(AdjPubDate,AdjName,AdjCode,AdjDate, AdjPriceO, AdjPriceA)
            with open(output_file_path_adj, 'a', newline='',encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows)
                csvfile.close()
            
            rows = zip(RedPubDate,RedName,RedCode,RedDate,RedLastDate)
            with open(output_file_path_red, 'a', newline='',encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows)
                csvfile.close()

            rows = zip(HalPubDate,HalName,HalCode,HalStartDate,HalEndDate)
            with open(output_file_path_hal, 'a', newline='',encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows)
                csvfile.close()
        else:
            log.processLog(f'本日無資料需剖析')
        
        
        end = time.perf_counter_ns()
        log.processLog(f'【結束程序】 CB_report.py - 總執行時間:{(end-start)/10**9}')
        log.processLog('==============================================================================================')
    
    except Exception as e:
        log.processLog('發生錯誤:請查看error.log')
        traceback.print_exc()
        log.errorLog(traceback.format_exc())

