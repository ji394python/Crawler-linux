# 宣告使用 /bin/bash
#!/bin/bash
#手動操作時可用此
cd news_crawler
python TWSE_crawler.py 
rm -f twse_future.csv
cd ..
python transform.py
