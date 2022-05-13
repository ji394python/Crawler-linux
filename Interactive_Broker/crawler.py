import pandas as pd 
import ftplib
import json 
import os 
import csv
from datetime import datetime, tzinfo
from dateutil import tz
from random import random
#import log_manager as log

#- 未標準化檔案：  Shortable\IB\{Country}\ {Date}\Base \{Country}_Shortable_{YY-MM-DD}_{HH:MM:SS}.csv
#- 標準化檔案： Shortable\IB\{Country}\ {Date}\Timeseies \{Ticker}_{Country}_Shortable_{Date}.csv

def convertCsv(filePath,outputPath):
    
    ET = tz.gettz('America/New_York')
    BJ = tz.gettz('Taiwan/Taipel')
    f = open(r'%s' % filePath,'rb+')
    date = f.readline().decode('big5').strip()
    date = date[date.find('|')+1:]
    
    date_USD = datetime.strptime(date,"%Y.%m.%d|%H:%M:%S")
    date_USD = date_USD.replace(tzinfo=ET)
    #date = f.readline().decode('big5').strip().split('|')
    date_NTD = str(date_USD.astimezone(BJ))
    day,time = date_NTD[:10],date_NTD[11:19].replace(':',"-")
    header = [ i.replace('#','') for i in f.readline().decode('big5').strip().split('|')][:-1]
    
    index = outputPath.rfind('/Base')
    headPath = outputPath[:index]
    tailPath = outputPath[index+6:]
    fileName = f"{headPath}/{day}/Base/{tailPath}{day}_{time}.csv"
    pathControl(fileName)

    if os.path.exists(fileName):
        f.close()
        return("")

    file = open(fileName,'w+',encoding='big5',newline='')
    writer = csv.writer(file)
    writer.writerow(header)
    for i in f.readlines():
        writer.writerow(i.strip().decode('big5').split('|')[:-1])
    file.close()
    f.close()

def downloadFile(ftp, remotepath, localpath):
    pathControl(localpath) #確認儲存路徑存在
    bufsize = 1024  # 設置緩衝大小
    fp = open(localpath, 'wb+')  # 以二進位形式打開檔案
    ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)  # 寫入文件
    #ftp.set_debuglevel(0)  # 關閉調試
    fp.close()  # 關閉文件

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
    nums = length.count('..')
    if nums != 0:
        temp = '/'.join(length[:nums+1])
        length = length[nums+1:]
    else:
        temp = length[0]
        length = length[1:]
    dirCreate(temp)

    for i in length:
        temp = temp + '/' + i
        dirCreate(temp)

if __name__ == '__main__':
    
    output_dir_path = json.load(open('set.json','r+'))['output_dir_path']
    country = json.load(open('country.json','r+',encoding='utf-8'))
    ftp = ftplib.FTP(host='ftp3.interactivebrokers.com',user='shortstock',passwd='')
    target = ftp.nlst()
    for file in ftp.nlst():
        index = file.rfind('.txt')
        if file[index:] == '.txt':
            output = f"N{int(random()*100000)}.txt"
            
            downloadFile(ftp,file, output_dir_path + '/Shortable/IB/Raw/'+output )
            
            name = country[file[:-4]]['alpha2']
            
            convertCsv(output_dir_path + '/Shortable/IB/Raw/'+output,output_dir_path + '/Shortable/IB/'+name+'/Base/'+name+'_Shortable_')

    os.system(f'rm -r {output_dir_path + "/Shortable/IB/Raw"}')

    with open('record.txt','a+') as f:
        f.write(f"{datetime.now()}")
        f.write('\n')
        f.close()

