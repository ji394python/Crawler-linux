# 宣告使用 /bin/bash
#!/bin/bash

cd news_crawler
python3 news_crawler.py -st 2020/07/07 -et 2021/07/07
cd ..
python3 parsing.py
python3 transform.py