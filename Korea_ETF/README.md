## 一、執行與維護方式
<hr>

### 1. 執行方式：<span style="color:#ffaaaa;font-size:14px">目前韓國爬蟲共有四支，3支ETF、1支Stock</span>
- Step1：修改 `set.json` 將根路徑改為需要的 (預設 `"../../NasHome"` )
- Step2：根據需求決定要執行哪個專案
  - 股票日內交易價格爬蟲：執行 `KOR_Crawler_Stock_Trade.py`，爬取當日股票交易訊息
  - ETF日內交易價格爬蟲：執行 `KOR_Crawler_ETF_Trade.py`，爬取當日Etf交易訊息
  - ETF成分表爬蟲：執行 `KOR_Crawler_ETF_PCF.py -d 2021/10/07`，爬取10月7號Etf成分表
  - ETF收盤淨值爬蟲：執行 `KOR_Crawler_ETF_NAV.py -st 2021/01/01 -et 2021/10/07`，爬取0101-1007的ETF淨值
- Step3：資料產生路徑
  - 依據`set.json`設的路徑下產生`KRX/ETF`、`KRX/Stock`兩種路徑，可直接於此查看
<br>
<br>


### 2.維護方式
  - 更新手冊
    - 更新`pre/code.json`的ETF資料 (該資料是由觀察網站直接複製回應而得)
    - 更新`pre/etf_code.csv`的ETF資料 (執行`pre/etf_code.py`剖析json而得)
    - 更新`pre/KR_stk_list.csv`的Stock資料 (該資料由dropbox資料夾提供)
    - 更新`set.json`路徑 (測試機：`"../../NasHome"`，正式機：`."./../NasPublic"`)
    - 記住正式機的`set.json`路徑目前恆不一樣，且不上傳，一律pull下來後再改



<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 爬蟲一：`KRX\Stock\{Date}\{Ticker}_Trades_{Date}.csv ` -> 股票日內交易價格路徑
- 爬蟲二：`KRX\ETF\Trade\{Date}\{Ticker}_Trades_{Date}.csv ` -> ETF日內交易價格路徑
- 爬蟲三：`KRX\ETF\PCF\{Date}\{Ticker}_Trades_{Date}.csv  ` -> ETF成分表路徑
- 爬蟲四：`KRX\ETF\NAV\{Ticker}_NAV.csv ` -> ETF淨值路徑

<pre>
└KRX
  └Stock
    └${Date}
      └ ${Ticker}_Trades_${Date}.csv 
  └ETF
    └ Trade
      └ ${Date}  
        └ ${Ticker}_Trades_${Date}.csv  
    └ PCF 
      └ ${Date}
        └ ${Ticker}_Trades_${Date}.csv
    └ NAV 
      └ {Ticker}_NAV.csv 

</pre>

### 2. 輸出檔案結構

- 爬蟲一：如 `KRX\Stock\20211007\024110_Trades_20211007.csv ` -> 股票日內交易價格 (5欄位)
  - 欄位名稱格式與<a href='https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp#'>爬蟲網站</a>相同未調整

    |Time|Price|Change|Trading Valume (shr.)|Trading Value (KRW)|
    |:--------:|:--------:|:--------------:|:--------------:|:-------|
    |09:00:11|10,600|50|1|10,600|
    |09:00:11|10,600|50|208|2,204,800|
    |...|...|...|...|...|
    |13:47:35|10,600|50|101|1,070,600|

- 爬蟲二：如 `KRX\Stock\20211007\024110_Trades_20211007.csv ` -> ETF日內交易價格 (7欄位)
  - 欄位名稱格式與<a href='https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp'>爬蟲網站</a>相同未調整

    |Time|Price|Change|iNAV|Underlying Index|Trading Volume (share)|Trading Volume at the time (share)|
    |:--------:|:--------:|:--------------:|:--------------:|:-------|:-------|:-------|
    |09:00:30|9,205|70|9,308.14|323.34|22|19|
    |09:02:05|9,250|115|9,310.48|323.34|44|22|
    |...|...|...|...|...|...|...|
    |14:16:06|9,275|140|9,331.59|323.34|12,463|20|

- 爬蟲三：如 `KRX\ETF\PCF\20211006\91170_PCF_20211006 ` -> ETF成分表 (5欄位)
  - 欄位名稱格式與<a href='https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.jsp'>爬蟲網站</a>相同未調整

    |Name|No of Shares|Par Value|Amount|Weight(%)|
    |:--------:|:--------:|:--------------:|:--------------:|:-------|
    |KBFinancialGroup|1,391.00|-|75,392,200|20.47|
    |HANAFINANCIALGR|1,679.00|-|75,387,100|20.47|
    |...|...|...|...|...|
    |Cash(KRW)|-|-|6,118,682|1.66|

- 爬蟲四：如 `KRX\ETF\NAV\114100_NAV` -> ETF淨值路徑 (5欄位)
  - 欄位名稱格式與<a href='https://global.krx.co.kr/contents/GLB/05/0507/0507010304/GLB0507010304.jsp'>爬蟲網站</a>相同未調整

    |Date|NAV|Underlying Index|Multiple of Tracking Return|Tracking Error|
    |:--------:|:--------:|:--------------:|:--------------:|:-------|
    |2021/1/4|113,412.81|10,773.66|1|0.31|
    |2021/1/5|113,500.61|10,781.12|1|0.31|
    |...|...|...|...|...|
    |2021/10/6|111,721.95|10,616.42|1|0.14|
<br>
<hr>
<hr>
<br>
<br>
<br>

## 三、程式碼介紹
<hr>

### 1. 前置資料檔
- `KR_stk_list.csv`：韓國股票代號檔 (需要手動更新)
- `code.json`：韓國ETF代號回應資訊 (由網站上開發者模式複製而得)
- `etf_code.py`：解析`code.json`使用並存成`etf_code.csv`
- `etf_code.csv`：韓國ETF代號檔 (需要手動更新)
- `log_manager.py`：log module檔
- `set.json`：路徑設定檔 (需要手動更新)

<br>

### 2. 程式檔功能
- `KOR_Crawler_Stock_Trade.py`：依據`KR_stk_list.csv`，爬取所有股票日內交易資訊
  - 目標網站：<a href="https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp#">KOR_Every hour interval</a>
  - 執行頻率：1/days (爬取當日資訊)
  - 遺漏資訊：若有股票抓不到或無資料的會存入同路徑下之`noExist.csv`，並記錄未被爬取原因
  
  <br>

- `KOR_Crawler_ETF_Trade.py`：依據`etf_code.csv`，爬取所有ETF日內交易資訊
  - 目標網站：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp">Intraday Price Chart</a>
  - 執行頻率：1/days (爬取當日資訊)
  - 遺漏資訊：若有股票抓不到或無資料的會存入同路徑下之`noExist.csv`，並記錄未被爬取原因
  
  <br>

- `KOR_Crawler_ETF_PCF.py`：依據`etf_code.csv`，爬取指定日期所有ETF所含成分資訊
  - 目標網站：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.jsp">PDF</a>
  - 執行頻率：1/days (爬取當日資訊or可指定日期)
  
  <br>
 
- `KOR_Crawler_ETF_NAV.py`：依據`etf_code.csv`，爬取指定日期內所有ETF收盤淨值資訊
  - 目標網站：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010304/GLB0507010304.jsp">Tracking Error</a>
  - 執行頻率：1/days (爬取當日資訊or可指定日期or可指定時間區間內) 
  
  <br>

- `KOR_ETF_PCF_Procedure.sh`：依據`etf_code.csv`，爬取指定日期內所有ETF所含成分資訊
  - 目標網站：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.jsp">PDF</a>
  - 執行頻率：手動執行的程式，不納入排程
  
  <br>

### 3.各檔案執行範例

- 爬取當日股票交易訊息：`python KOR_Crawler_Stock_Trade.py`

- 爬取當日ETF交易訊息：`python KOR_Crawler_ETF_Trade.py`

- 爬取當日ETF成分表訊息：`python KOR_Crawler_ETF_PCF.py`
  - 預設爬取當天資料，要改成時間區間的話可以使用sh檔`bash KOR_ETF_PCF_Procedure.sh`
    - 將該sh檔內的 `input_start` 、 `input_end` 改為指定的開始與結束日期即可

- 爬取近一年ETF淨值訊息：`python KOR_Crawler_ETF_NAV.py`
  - 預設爬近一年的交易資料，官方會給出一個csv
  - 爬取特定時間可改成此：`python KOR_Crawler_ETF_NAV.py -st 2021/01/01 -et 2021/10/07`
  
<br>
<hr>
<hr>
<br>
<br>
<br>

## 四、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

### 1. 更新/補爬紀錄
  - 10/12：將日期格式由`"%Y%m%d"`更改為`"%Y-%m-%d"`
  - 10/08：ETF、股票交易爬蟲新增遺漏資料(`noExist.csv`)紀錄
  - 10/07：將ETF、股票交易價的爬蟲url由圖片/表格改為下載csv
  - 10/03：初版完成，並開始測試

<br>

### 2. 規則補充
  - 股票、ETF日內交易價格有三種爬取方式，圖表/表格/下載Csv，三種
  - PCF、NAV可以反覆補爬過去的日期，但Stock/NAV僅能當日爬完，若過了即無法補爬

<br>

### 3.參考網站

- 股票相關：
  - 日內交易：<a href="https://global.krx.co.kr/contents/GLB/05/0503/0503010400/GLB0503010400.jsp#">KOR_Every hour interval</a>
- ETF相關：
  - 日內交易：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp">Intraday Price Chart</a>
  - 淨值：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010304/GLB0507010304.jsp">Tracking Error</a>
  - 成分表：<a href="https://global.krx.co.kr/contents/GLB/05/0507/0507010302/GLB0507010302.jsp">PDF</a>
  