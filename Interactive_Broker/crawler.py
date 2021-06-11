import pandas as pd 
import ftplib
import json 
import os 
import csv
import datetime

#Shortable\IB\{Country}\{Country}_Shortable_{YY-MM-DD}_{HH:MM:SS}.csv
def convertCsv(filePath,outputPath):
    f = open(r'%s' % filePath,'rb+')
    date = f.readline().decode('big5').strip().split('|')
    day,time = date[1].replace('.','-'),date[2].replace(':','-')
    header = [ i.replace('#','') for i in f.readline().decode('big5').strip().split('|')][:-1]
    fileName = outputPath + day + '_' + time + '.csv' 
    pathControl(fileName)
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
    temp = length[0]
    dirCreate(temp)

    for i in length[1:]:
        temp = temp + '/' + i
        dirCreate(temp)


if __name__ == '__main__':

    country = json.load(open('country.json','r+',encoding='utf-8'))
    ftp = ftplib.FTP(host='ftp3.interactivebrokers.com',user='shortstock',passwd='')
    target = ftp.nlst()
    for file in ftp.nlst():
        index = file.rfind('.txt')
        if file[index:] == '.txt':

            downloadFile(ftp,file,'Shortable/IB/Raw/'+file)
            
            name = country[file[:-4]]['alpha2']
            
            convertCsv('Shortable/IB/Raw/'+file,'Shortable/IB/'+name+'/'+name+'_Shortable_')

    os.system('rm -r Shortable/IB/Raw')

    with open('record.txt','w+') as f:
        f.write(f"{datetime.datetime.now()}")
        f.close()


#Shortable\IB\{Country}\{Date}\{Ticker}_{Country}_Shortable_{Date}.csv
#Shortable\IB\{Country}\{Country}_Shortable_{YY-MM-DD}_{HH:MM:SS}.csv
