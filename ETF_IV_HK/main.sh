#!/bin/bash
#自動排程使用
today=($(date +%Y/%m/%d --date="-1 day"))
python3 crawler.py -st $today -et $today
