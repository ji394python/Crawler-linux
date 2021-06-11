# !/usr/bin/python
# coding: utf-8
from ftplib import FTP


def ftpconnect(host, username, password):
    ftp = FTP()
    ftp.set_debuglevel(2)  # 開啟debug模式
    ftp.connect(host, 21)  # 連接FTP Server
    ftp.login(username, password)  #登入
    return ftp


def downloadfile(ftp, remotepath, localpath):
    bufsize = 1024  # 設置緩衝大小
    fp = open(localpath, 'wb+')  # 以二進位形式打開檔案
    ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)  # 寫入文件
    #ftp.set_debuglevel(0)  # 關閉調試
    fp.close()  # 關閉文件


def uploadfile(ftp, remotepath, localpath):
    bufsize = 1024
    fp = open(localpath, 'rb')
    ftp.storbinary('STOR ' + remotepath, fp, bufsize)  # 上传文件
    ftp.set_debuglevel(0)
    fp.close()


if __name__ == "__main__":
    ftp = ftpconnect("192.168.31.111", "tingnan666", "tingnan666")
    downloadfile(ftp, "/devconfig.txt", "G:/test.txt")
    #uploadfile(ftp, "***", "***")

    ftp.quit()
