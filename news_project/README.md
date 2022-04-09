## 一、執行與維護方式
<hr>

### 1. 執行方式
- Step1：修改 `set.json` 將根路徑改為需要的
- Step2：執行 `python news_crawler.py` 爬取昨日~前三十日新聞 (迴圈執行`twse_mops_crawler.py`)
- Step3：執行 `python parsing.py` 剖析新聞
- Step4：執行 `python TWSE_crawler.py` 啟動驗證程序
- Step4：執行 `python transform.py` 對剖析檔做ETL
- Step5：資料預計於 `Step1` 所設定的路徑下，產生 `News_Stocks` 的資料夾
  - 沒有該資料夾的話會自己創建,不須擔心路徑缺失
  - 若要更改輸出資料夾的話可以聯絡我,只需要改一行，但用打的不好說明邏輯
<br>
- <span style="color:#ffaaaa"> 一鍵執行 </span>：也可以執行 `bash main.sh`直接執行全部流程


### 2.維護方式
  - 更新手冊
    - 更新`predata`的資料 (scp本地端傳入)
    - 更新`main.sh` (將python改為python3)
    - 更新`news_crawler.py` (將python改為python3)
    - 更新`set.json`路徑 (測試機：`"../../NasHome"`，正式機：`."./../NasPublic"`)
    - 記住正式機的`set.json`路徑目前恆不一樣，且不上傳，一律pull下來後再改

  - 待觀察
    - 測試機明天停券公告剖析狀況 (檢查done_date、data.csv、news_crawler/log、log)
    - 正式機明天停券公告剖析狀況 (檢查done_date、data.csv、news_crawler/log、log)

  - 可優化方向
    - 根據作業系統不同選擇python3 or python
    - 根據ip位址決定跟路徑
    - 自我檢查機制與推波

<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、輸出檔案介紹
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

## 三、程式碼介紹
<hr>

### 1. 前置資料檔
- `stock_index_confirm.csv`：股票代號檔
- `holiday.csv`：台灣例假日資訊,需要手動更新 (更新頻率: 1/years)
- `workday.csv`：台灣補上班日資訊,經法規修改後已不需要
- `set.json`：路徑設定檔

### 2. 程式檔功能
- `news_crawler.py`：執行twse_mops_crawler.py的主程式檔 
- `twse_mops_crawler.py`：爬取公開資訊觀測站重大訊息公告的主程式檔 (執行頻率：1/days)
  - 爬取網站：https://mops.twse.com.tw/mops/web/t05st02

- `twse_crawler.py`：爬取證交所/櫃買中心停券預告表，並作為驗證機制檢驗parsing.py產生之結果檔，若缺乏就會補上
  - 目標網站：<a href="https://www.twse.com.tw/exchangeReport/BFI84U">證交所停券預告表</a>、<a href="https://www.twse.com.tw/exchangeReport/BFI84U2">證交所停券歷史查詢</a>、<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term_result.php">櫃買中心停券預告與歷史查詢表</a>
- `parsing.py`：剖析重大訊息公告的主程式檔 (執行頻率: 1/days)
- `transform.py`：ETL使用,針對剖析完檔案使用 (執行頻率： 1/days)
- `log_manager.py`：程式進程紀錄使用,輸出資料夾(log)置於VM上不納入E槽
- `main.sh`：Shell Scripts,手動使用,未加入排程

### 3.各檔案執行範例

- 爬取時間區間公告：`python news_crawler.py -st 2021/06/01 -et 2021/06/30`
  - 即為爬取6/1~6/30號的所有公告
  - 可於路徑 `../../../ShareDiskE/News_Stocks` 找到爬取檔案


- 爬取前一個月公告：`python news_crawler.py`
  - 預設爬取當天日期-29天 ~ -1天的公告
  - 可於路徑 `../../../ShareDiskE/News_Stocks` 找到爬取檔案

<br>

- 執行剖析檔案：`python parsing.py`
  - 可於路徑 `../../../ShareDiskE/News_Stocks/print/data.csv` 找到剖析檔案

<br>

- 執行ETL檔案：`python transform.py`
  - 可於路徑 `../../../ShareDiskE/News_Stocks/print/data.csv` 找到剖析檔案


<br>
<hr>
<hr>
<br>
<br>
<br>

## 四、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

### 1. 更新/補爬紀錄
  - 2022/04/09：2022/03/23新聞缺乏的原因不明，猜測和另一專案(IB)的錯誤相同，應該是NAS資料庫的問題，**若未來仍再發生此情況，會再連帶使`parsing.py`和爬蟲一樣也重複30天**
  - 2022/04/08：
    - 修改爬蟲補爬程序`TWSE_crawler.py`，因證交所API的encode由'big5->ms950'
    - 更新預資料`holiday.csv`，納入2022的公告假期，避免`parsing.py`計算T-6交易日時出錯
  - 2021/10/19：新增環境偵測機制，根據環境不同執行不同代碼 (Linux or win32)
  - 2021/10/07：發現data.csv最末段行出現header導致`TWSE_Crawler.py`出現運行錯誤，故本日刪除data.csv最後一行且補執行`TWSE_Crawler.py`
    - 受影響天數為10/01~10/07，但補爬蟲後也僅增加ETF的停券資訊，故未影響主流程
    - 同步刪除news_crawler/ERROR.log
  - 2021/08/20：新增證交所/櫃買中心停券預告表的驗證機制，若我們剖析的檔案有缺漏時，可從證交所/櫃買中心補上 (`TWSE_crawler.py`)
  - 2021/07/23：補充: 每年年末要更新 `stock_index_confirm.csv` 和 `holiday.csv` 以確保股票代碼/假日日期是正確的
  - 2021/07/22：將預設爬取日期改為 -29天 ~ -1天、新增爬蟲的log，置於news_crawler資料夾下

### 2. 規則補充
  - 2015/03/16後為<a href="https://www.twse.com.tw/ch/products/publication/download/0001001837.pdf">停券起訖日新法規</a>的開始日，故從2015/04/01後開始驗證
  - 日期驗證OK (比對停券起迄日與<a href="https://www.twse.com.tw/zh/page/trading/exchange/BFI84U2.html">證交所</a>/<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心</a>的是否相同)
  - 櫃買中心停券預告的問題
    - 缺乏資料1：有的公司有發布<a href="https://mops.twse.com.tw/mops/web/t05st01">新聞公告</a>說明停券日期，但<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心查詢網</a>並沒有 (Dropbox:有新聞公告但網站無.csv)
    - 缺乏資料2：<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心查詢網</a>有預告停券，但<a href="https://mops.twse.com.tw/mops/web/t05st01">新聞公告</a>卻查不到 (Dropbox:上市櫃有停券無新聞公告.xlsx)
  - 會有同一天停券的情形：
    - 如神基(3005)於2021/02/25公告股東常會與除息相關訊息，而停券日則為同一天
    - 此情況在未來Py檔剖析的停券預告表都會<b style="color:#ffaaaa">予以保留</b>
  - 代子公司發布的狀況：
    - 排除規則：若新聞公告的主旨有 "子公司" 的會予以排除 (保留電子公司)
    - 特殊處理："n代n" 的主旨需要記成規則進行排除 (如富邦金控代富邦投信、台新金控代台新投顧)
  - 存託憑證(DR)：
    - 不按照一般格式走，有的是自己給pdf連結，有的是使用文字不同(停止過戶日期起始)
    - 此類仍須增加許多規則才能完成
  - 驗證程序(`TWSE_crawler.py`) 對應回 `data.csv` (由`parsing.py`產生)的方式為 mapping 「股票代號、停券起日、停券迄日」三欄位，若不相同就會append至`data.csv`
  - 證交所將"結算交割日"視為停券起迄計算之工作日，而目前程式將其當作假日，停券起迄日會比預告表來的更早 (已確認過目前不用改此狀況)
  - 

### 3. 各種參考網站

- 需手動更新資料相關：
  - 假日更新：<a href="https://www.twse.com.tw/zh/holidaySchedule/holidaySchedule">證交所-市場開休日查詢</a>
  - 股票編碼：<a href="https://www.twse.com.tw/zh/page/products/stock-code2.html">證交所-證券編碼公告</a>

<br>

- 停券預告相關
  - 新聞公告：<a href="https://mops.twse.com.tw/mops/web/t05st01">公開資訊觀測站-歷史重大訊息</a>
  - 停券預告1：<a href="https://www.twse.com.tw/zh/page/trading/exchange/BFI84U2.html">證交所-停券預告表</a>
  - 停券預告2：<a href="https://www.tpex.org.tw/web/stock/margin_trading/term/term.php?l=zh-tw">櫃買中心-停券預告表</a>
  - 停券歷史1：<a href="https://www.twse.com.tw/exchangeReport/BFI84U2">證交所停券歷史查詢</a>

<br>

- 法規相關
  - 重大訊息歸類：<a href="http://www.selaw.com.tw/LawContent.aspx?LawID=G0100104&Hit=1">臺灣證券交易所股份有限公司對有價證券上市公司重大訊息之查證暨公開處理程序</a>
  - 停券預告縮短：<a href="https://www.twse.com.tw/ch/products/publication/download/0001001837.pdf">取消停止過戶前停資規定
  並縮短停券為 4 日</a>
