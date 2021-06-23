## 一、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`Shortable\IB\AU\2021-06-12\Base\AU_Shortable_2021-06-12_17-15-03.csv` -> 即為未標準化檔案的路徑
- 舉例：`Shortable\IB\AU\2021-06-12\Timeseries\BBVA_AU_Shortable_2021-06-12.csv` -> 即為標準化檔案的路徑
- 名詞解釋：
    - 未標準化檔案：即根據 <a href="ftp://ftp3.interactivebrokers.com/austria.txt">IB_FTP網址</a> 所爬之原始資料 (ftp://ftp3.interactivebrokers.com/austria.txt) 
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

### 2. 輸出檔案結構

- 未標準化檔案：如 `Shortable\IB\AU\2021-06-12\Base\AU_Shortable_2021-06-12_17-15-03.csv`

|  SYM  |  CUR  |        NAME     |  CON        |  ISIN          |   REBATERATE  | FEERATE    | AVAILABLE|
|:-----:|:-----:|:---------------:|:-----------:|:--------------:|--------------:|-----------:|---------:|
|  0B2E |  UR   |  BAWAG GROUP AG |  294207034  |  AT0000BAWAG2  |  -1.1247      |  0.6467    |  2600000 |
|  1COV |  EUR  |  COVESTRO AG    |  208908717  |  DE0006062144  |  -1.0519      |  0.5739    |  3700000 |
|  1SI  |  EUR  |   SNAP INC - A  |  268862337  |  XXXXXXXA1060  |  -0.8780      |  0.4000    |    17    |

<br>

- 標準化檔案：如 `Shortable\IB\AU\2021-06-12\Timeseries\BBVA_AU_Shortable_2021-06-12.csv` 
    - 新增兩個主要欄位：Machine Time 、 UnixTime
    - 表格單位：每檔股票(Ticker)各自的Table

| Machine Time| Unix Time |  SYM  |  CUR  |        NAME     |  CON        |  ISIN          |   REBATERATE  | FEERATE    | AVAILABLE|
|:-----------:|:---------:|:-----:|:-----:|:---------------:|:-----------:|:--------------:|--------------:|-----------:|---------:|
|2021/06/12 17:15:03|1623489303.0|1COV|EUR|COVESTRO AG|208908717.0|DE0006062144|-1.0519|0.5739|3700000|
|2021/06/12 17:30:03|1623482343.0|1COV|EUR|COVESTRO AG|208901111.0|DE0006062144|-1.0719|0.6739|3800000|

- 未標準化/標準化檔案皆為big5編碼

<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、程式碼介紹
<hr>

### 1. 前置資料檔
- `country.json`：儲存國家中英文名稱、二碼縮寫、三碼縮寫的json檔,可重複使用與擴充
- `date.csv`：紀錄哪些日子的未標準化檔案已進行過標準化,避免重複轉換 (更新頻率: 1/day)
- `record.txt`：每爬取一次IB的網站即更新一次 (更新頻率: 1/min)
- `parsing.txt`：記錄每天標準化檔案的執行時間 (更新頻率: 1/day)

### 2. Py檔功能
- `crawler.py`：執行爬取IB FTP網站的主程式檔  (執行頻率：每1分鐘爬取一次)
- `parsing.py`：執行剖析未標準化檔案至標準化檔案的主程式檔  (執行頻率：每日早上7點執行)
- `ftp.py`：FTP的爬蟲的Module,留著待使用
- `moving.py`：搬運程式的py檔,目前不需用到
- `ETL.py`：優化處理速度的module

### 3. 執行範例
- 爬取IB FTP網站：於終端機輸入 `python crawler.py`，即可在路徑 `../../ShareDiskE/Shortable/IB/` 看到爬取之未標準化檔案
- 標準化檔案：於終端機輸入 `python parsing.py`，即可在路徑  `../../ShareDiskE/Shortable/IB/` 看到標準化檔案

<br>
<hr>
<hr>
<br>
<br>
<br>

## 三、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

1. 若該國當天未開市,則會有Base資料夾但無Timeseies資料夾
    - 如2021-06-19的IN,無Timeseries資料夾,而Base中雖有檔案但打開為header+#EOF

