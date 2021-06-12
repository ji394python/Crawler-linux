
## 一、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`Shortable\IB\AU\2021-06-12\Base\AU_Shortable_2021-06-12_17-15-03.csv` -> 即為未標準化檔案的路徑
- 舉例：`Shortable\IB\AU\2021-06-12\Timeseries\BBVA_AU_Shortable_2021-06-12.csv` -> 即為標準化檔案的路徑
- 名詞解釋：
    - 未標準化檔案：即根據 <a href="ftp://ftp3.interactivebrokers.com/austria.txt">IB_FTP網址</a> 所爬之原始資料 
    - 標準化檔案：根據未標準化檔案,針對<b>同一檔股票(Ticker)</b>所整理之時間序列資料
<pre>
└Shortable
  └IB
    └ ${CountryCode_兩碼縮寫}
        └ ${Date_依照不同日期}
            └ Base
                └ 未標準化的檔案.csv
            └ TimeSeries
                └ 標準化的檔案.csv
</pre>

<hr>
<hr>
<br>

## 二、程式碼介紹
<hr>

### 1. 前置資料檔
- `country.json`：儲存國家中英文名稱、二碼縮寫、三碼縮寫的json檔,可重複使用與擴充
- `date.csv`：紀錄哪些日子的未標準化檔案已進行過標準化,避免重複轉換 (更新頻率: 1/day)
- `record.txt`：每爬取一次IB的網站即更新一次 (更新頻率: 1/min)

### 2. Py檔功能
- `crawler.py`：執行爬取IB FTP網站的主程式檔  (執行頻率：每日早上七點執行)
- `parsing.py`：執行剖析未標準化檔案至標準化檔案的主程式檔  (執行頻率：每1分鐘爬取一次)
- `ftp.py`：FTP的爬蟲的Module,留著待使用
- `moving.py`：搬運程式的py檔,目前不需用到