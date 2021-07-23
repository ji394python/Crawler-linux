## 一、如何執行
<hr>

### 1. 執行方法
- 方法1：執行 `python3 crawler.py -st %Y/%m/%d -et %Y/%m/%d` 爬取指定日期區間公告之淨值
- 方法2：執行 `bash main.sh` 爬取昨日公告之ETF淨值

<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`ETF_NAV\HK\{Date}\HK_NAV_{Ticker1}_{Ticker2}…{TickerN}_{Date}.xlsx`

<pre>
└ETF_NAV
  └HK
    └${Date_依照不同日期}
      └ ${Ticker1}_{Ticker2}..._{TickerN}_${Date}_${Time}.xlsx
   

</pre>

### 2. 輸出檔案結構

- 原始檔案：可直接至路徑查看，為爬到的原始檔 (各檔案格式依官方給的額定，故不舉例)
- 剖析檔案：港交所ETF淨值爬蟲目前仍不需剖析檔案

<br>
<hr>
<hr>
<br>
<br>
<br>

## 三、程式碼介紹
<hr>

### 1. 前置資料檔
- `set.json`：路徑設定檔，只要設定路徑，此港交所爬蟲就會長在資料夾下

### 2. 程式檔功能
- `crawler.py`：爬蟲主程式檔 (執行頻率： 1/days)
- `log_manager.py`：程式進程紀錄使用,輸出資料夾(log)置於VM上不納入E槽
- `main.sh`：Shell Scripts,為排程所實際使用的檔案，先計算日期後會自動啟動crawler.py

### 3.各檔案執行範例

- 爬取時間區間公告：`python crawler.py -st 2021/06/01 -et 2021/06/30`
  - 即為爬取6/1~6/30號的港交所ETF淨值公告檔 (xlsx)
  - 可於路徑 `../../../ETF_NAV/HK` 找到爬取檔案

<br>

- 爬取昨日公告：`bash main.sh`  or `python crawler.py`
  - 即為爬取昨日公告，爬取當天日期-1天的公告
  - 可於路徑 `../../../ETF_NAV/HK` 找到爬取檔案

<br>
<hr>
<hr>
<br>
<br>
<br>

## 四、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

### 1. 目前排定每工作日的 03:00a.m. 執行爬蟲,因週六日港交所休假故不爬
### 2. 只爬兩種類型的檔案：
  - Trading Information of Leveraged and Inverse Products
  - Trading Information of Exchange Traded Funds
​​      

### 3. 參考網站

- 本次爬取網站：<a href="https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en">香港證交所ETF</a>

### 4. 更新紀錄
- 0722 新增 `set.json` 作為根路徑控制，只要設定路徑，此IB爬蟲就會長在該資料夾下