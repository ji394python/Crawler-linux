import os
from datetime import datetime
fobj = open(os.path.join(os.pardir,"..","時間序列分析","new.txt"), "w") #雙層父路徑
fobj.write(f"{datetime.now()} 測試父路徑 {os.pardir}")
fobj.close()
print(f"{os.path.join(os.pardir, 'new.txt')}")
os.mkdir('../測試')
#print(f"{datetime.now()} 測試父路徑 {os.path.join(os.pardir,'/ShareDiskE/IB_Project/測試.txt')}","w+")
#fobj2 = open("../ShareDiskE/IB_crawler/測試.txt","w")

