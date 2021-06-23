## 一、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`Stocks_News\{Date}\{Ticker}_{Date}_{Time}.txt` -> 重大訊息公告原始檔路徑
- 舉例：`Stocks_News\print\data.csv` -> 重大訊息公告剖析結果檔
- 舉例：`Stocks_News\print\done_date.csv` -> 重大訊息剖析日期紀錄 (更新頻率: 1/days)

<pre>
└Stock_News
  └${Date_依照不同日期}
    └ ${Ticker}_${Date}_${Time}.txt
  └print
    └ data.csv
    └ done_date.csv  

</pre>

### 2. 輸出檔案結構

- 原始檔案：如 `/Stocks_News/20210618/2641_正德_20210618_133714.txt`
  - 即爬取到的原始檔
<pre>
公司名稱：正德
公司代號：2641
...
...
</pre>

<br>

- 剖析檔案：`Stocks_News\print\data.csv` 
  - 使用`utf-8-sig`作為編碼,原因為使用big5在處理網頁原始公告會有問題,故以此為主可避免未來一些衍生狀況

|0.股票代號|1.股票名稱|   2.停券起日   |   3.停券迄日   | 4.原因 |   5.發言日期   | 6.REASON |
|:--------:|:--------:|:--------------:|:--------------:|:-------|:--------------:|:--------:|
| 1460 |宏遠|110/06/24|110/06/29|減資|20210601|11b|
| 2395 |研華|110/07/02|110/07/07|除息|20210601|14b|

<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、程式碼介紹
<hr>

### 1. 前置資料檔
- `stock_index_confirm.csv`：股票代號檔
- `holiday.csv`：台灣例假日資訊,需要手動更新 (更新頻率: 1/years)

### 2. Py檔功能
- `news_crawler.py`：執行twse_mops_crawler.py的主程式檔 (手動使用,未加入排程)
- `twse_mops_crawler.py`：爬取公開資訊觀測站重大訊息公告的主程式檔 (執行頻率：1/days)
  - 爬取網站：https://mops.twse.com.tw/mops/web/t05st02

- `twse_mops_today_crawler.py`：已捨棄此py檔不用,因可直接透過上面的py檔完成
  - 爬取網站：https://mops.twse.com.tw/mops/web/t05sr01_1
- `main.py`：剖析重大訊息公告的主程式檔 (執行頻率: 1/days)

### 3. 執行範例
- 爬取時間區間公告：`python news_crawler.py -st 2021/06/01 -et 2021/06/30`
  - 即為爬取6/1~6/30號的所有公告
  - 可於路徑 `../../../ShareDiskE/Stocks_News` 找到爬取檔案
- 爬取昨日公告：`python news_crawler.py`
  - 即為爬取昨日公告，預設爬取當天日期-1天的公告
  - 可於路徑 `../../../ShareDiskE/Stocks_News` 找到爬取檔案
- 執行剖析檔案：`python main.py`
  - 可於路徑 `../../../ShareDiskE/Stocks_News/print` 找到剖析檔案

<br>
<hr>

## 三、原始說明

#### 1. 以前留下的說明文件,保留於此,若不需要可刪除

- 使用說明
  - 1.直接執行main.py。
  - 2.holiday(放假無交易日)需一年一度自行添加。
  - 3.已自動排除興櫃股。
  - 4.預設為每日執行，會依照new資料夾內資料全部執行，若該日資料已存在則跳過。
  - 5.輸出資料於print/data.csv，print/done_date.csv則為記錄已執行過日期。