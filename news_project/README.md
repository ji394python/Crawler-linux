## 一、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`News_Stocks\{Date}\{Ticker}_{Date}_{Time}.txt` -> 重大訊息公告原始檔路徑
- 舉例：`News_Stocks\print\data.csv` -> 重大訊息公告剖析結果檔
- 舉例：`News_Stocks\print\done_date.csv` -> 重大訊息剖析日期紀錄 (更新頻率: 1/days)

<pre>
└News_Stocks
  └${Date_依照不同日期}
    └ ${Ticker}_${Date}_${Time}.txt
  └print
    └ data.csv
    └ done_date.csv  

</pre>

### 2. 輸出檔案結構

- 原始檔案：如 `/News_Stocks/20210618/2641_正德_20210618_133714.txt`
  - 即爬取到的原始檔
<pre>
公司名稱：正德
公司代號：2641
...
...
</pre>

<br>

- 剖析檔案：`News_Stocks\print\data.csv` 
  - 使用`utf-8-sig`作為編碼,原因為使用big5在處理網頁原始公告會有問題,故以此為主可避免未來一些衍生狀況

|股票代號|股票名稱 |   停券起日   |   停券迄日   | 原因 |   發言日期   | REASON | 主旨 |
|:--------:|:--------:|:--------------:|:--------------:|:-------|:--------------:|:--------:|:-------:|
| 1460 |宏遠|110/06/24|110/06/29|減資|20210601|11b|公告的主旨內容|
| 2395 |研華|110/07/02|110/07/07|除息|20210601|14b|公告的主旨內容|

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
- `workday.csv`：台灣補上班日資訊,經法規修改後已不需要

### 2. 程式檔功能
- `news_crawler.py`：執行twse_mops_crawler.py的主程式檔 (手動使用,未加入排程)
- `twse_mops_crawler.py`：爬取公開資訊觀測站重大訊息公告的主程式檔 (執行頻率：1/days)
  - 爬取網站：https://mops.twse.com.tw/mops/web/t05st02

- `parsing.py`：剖析重大訊息公告的主程式檔 (執行頻率: 1/days)
- `transform.py`：ETL使用,針對剖析完檔案使用 (執行頻率： 1/days)
- `log_manager.py`：程式進程紀錄使用,輸出資料夾(log)置於VM上不納入E槽
- `main.sh`：Shell Scripts,手動使用,未加入排程

### 3. 執行範例
- 爬取時間區間公告：`python news_crawler.py -st 2021/06/01 -et 2021/06/30`
  - 即為爬取6/1~6/30號的所有公告
  - 可於路徑 `../../../ShareDiskE/News_Stocks` 找到爬取檔案
- 爬取昨日公告：`python news_crawler.py`
  - 即為爬取昨日公告，預設爬取當天日期-1天的公告
  - 可於路徑 `../../../ShareDiskE/News_Stocks` 找到爬取檔案
- 執行剖析檔案：`python parsing.py`
  - 可於路徑 `../../../ShareDiskE/News_Stocks/print` 找到剖析檔案

<br>
<hr>
<hr>
<br>
<br>
<br>

## 三、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

### 1. 已驗證過六年上市櫃資料
  - 2015/03/16後為<a href="https://www.twse.com.tw/ch/products/publication/download/0001001837.pdf">停券起訖日新法規</a>的開始日，故從2015/04/01後開始驗證
  - 日期驗證OK (比對停券起迄日與<a href="https://www.twse.com.tw/zh/page/trading/exchange/BFI84U2.html">證交所</a>/<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心</a>的是否相同)
  - 櫃買中心停券預告的問題
    - 缺乏資料1：有的公司有發布<a href="https://mops.twse.com.tw/mops/web/t05st01">新聞公告</a>說明停券日期，但<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心查詢網</a>並沒有 (Dropbox:有新聞公告但網站無.csv)
    - 缺乏資料2：<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心查詢網</a>有公告停券，但<a href="https://mops.twse.com.tw/mops/web/t05st01">新聞公告</a>卻查不到 (Dropbox:上市櫃有停券無新聞公告.xlsx)
  - 會有同一天停券的情形：
    - 如神基(3005)於2021/02/25公告股東常會與除息相關訊息，而停券日則為同一天
    - 此情況在未來Py檔剖析的停券預告表都會<b style="color:#ffaaaa">予以保留</b>
  - 代子公司發布的狀況：
    - 排除規則：若新聞公告的主旨有 "子公司" 的會予以排除 (保留電子公司)
    - 特殊處理："n代n" 的主旨需要記成規則進行排除 (如富邦金控代富邦投信、台新金控代台新投顧)

### 2. 各種參考網站

- 需手動更新資料相關：
  - 假日更新：<a href="https://www.twse.com.tw/zh/holidaySchedule/holidaySchedule">證交所-市場開休日查詢</a>
  - 股票編碼：<a href="https://www.twse.com.tw/zh/page/products/stock-code2.html">證交所-證券編碼公告</a>
<br>
- 停券預告相關
  - 新聞公告：<a href="https://mops.twse.com.tw/mops/web/t05st01">公開資訊觀測站-歷史重大訊息</a>
  - 停券預告1：<a href="https://www.twse.com.tw/zh/page/trading/exchange/BFI84U2.html">證交所-停券預告表</a>
  - 停券預告2：<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心-停券預告表</a>
<br>
- 法規相關
  - 重大訊息歸類：<a href="http://www.selaw.com.tw/LawContent.aspx?LawID=G0100104&Hit=1">臺灣證券交易所股份有限公司對有價證券上市公司重大訊息之查證暨公開處理程序</a>
  - 停券預告縮短：<a href="https://www.twse.com.tw/ch/products/publication/download/0001001837.pdf">取消停止過戶前停資規定
  並縮短停券為 4 日</a>