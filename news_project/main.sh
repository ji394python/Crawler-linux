# 宣告使用 /bin/bash
#!/bin/bash
#手動操作時可用此
cd news_crawler
python3 news_crawler.py -st 2021/01/01 -et 2021/08/18
cd ..
python3 parsing.py
cd news_crawler
python3 TWSE_crawler.py 
rm -f twse_future.csv
cd ..
python3 transform.py
