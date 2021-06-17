import os
from datetime import datetime
fobj = open(os.path.join(os.pardir,"..","new.txt"), "w")
fobj.write(f"{datetime.now()} 測試父路徑 {os.pardir}")
print(f"{os.path.join(os.pardir, 'new.txt')}")
#print(f"{datetime.now()} 測試父路徑 {os.path.join(os.pardir,'/ShareDiskE/IB_Project/測試.txt')}","w+")
#fobj2 = open("../ShareDiskE/IB_crawler/測試.txt","w")

