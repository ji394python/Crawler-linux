# 宣告使用 /bin/bash
#!/bin/bash

cd news_crawler
python news_crawler.py -st 2020/02/20 -et 2021/06/27
cd ..
python parsing.py
python transform.py