
#  橋新科技會計帳務系統

<hr>

## 索引大綱
- [一、執行與維護方式](#一、執行方式)
- [二、運作機制補充](#二、運作機制補充)

<hr>

## 一、執行與維護方式

### 0.詳細參考資料：

- **交接影片**：<a href="https://www.youtube.com/watch?v=4NTIVdW_k80">橋新科技-帳務程式交接影片</a>
- **輸出格式**：<a href="https://paper.dropbox.com/doc/_--BejCohHszCEfw6ecQuW52T~3Ag-ycTPFjxUEkvEoHrvjtBNm">帳務工程_標準化報表</a>，4張報表詳細輸出格式請參考此文件 
- **主要文件**：<a href="https://paper.dropbox.com/doc/--BejADg87N9NcOZkvmJ5zoPv1Ag-gN6MgE2evMVOXQUJMsku2">帳務工程_主要文件</a>，大致上與交接影片相同
- **前置檔案**：<a href="https://paper.dropbox.com/doc/_--BegCJuPAzTCVXLx52G_QgvDdAg-vI9bvKTvS6SKR3dAbnyAl">帳務系統_說明文件</a>，此文件主要說明前置檔案的格式與來源


### 1. 執行方式：<span style="color:#ffaaaa;font-size:14px">橋新交易明細 + 前1日永豐庫存資料 => 當日4張報表 </span>
- Step1：將'交易明細' + '前1日永豐庫存'置於 `rawdata/${Stocks,Futures}` 中，以下為例子
  - 證券：如 `rawdata/Stocks` 已將永豐證券庫存0117、橋新證券交易明細0118放入
  - 期貨：如 `rawdata/Futures` 已將永豐期貨庫存0117、橋新期貨交易明細0118放入
- Step2：執行`Python main.py`
- Step3：報表將產於 `output/${Stocks,Futures}` 資料夾中，共有4張(成交明細/損益明細/明細庫存/合併庫存)
  - 成交表：`Trades_${Stocks,Futures}_${date}.csv`
  - 損益報表：`PnL_${Stocks,Futures}_${date}.csv`
  - 庫存明細報表：`Position_Detailed_${Stocks,Futures}_${date}.csv`
  - 庫存合併報表：`Position_Consolidated_${Stocks,Futures}_${date}.csv`

<br>
<br>


### 2.維護方式
    
- 期貨結算表：**1個月更新1次**
    - 法一：需下載"<a href="https://www.taifex.com.tw/cht/5/sSFFSP">股票期貨</a>"、"<a href="https://www.taifex.com.tw/cht/5/futIndxFSP">指數期貨</a>"後調整成對應格式，放置於 `utils/doc/期貨_最後結算價一覽表`，對應格式可以參考裡面的檔案
    - 法二：以撰寫一支Py爬蟲程式(`utils\crawler\Download_Future_ClosePositionDoc.py`)，將裡面的參數改成對應年月後執行即可
- 除權息表：**1個月更新1次**
    - 前往<a href="https://mops.twse.com.tw/mops/web/t108sb27">公開資訊觀測站</a>，根據"市場別"下載「上市、上櫃、興櫃」資料後，置於`utils\doc\證券_除權除息預告表`後，到`utils`資料夾執行`Python 除權息總表.py`即可，會在該路徑產生 除權息總表.csv 供程式使用
- 證券與期貨代碼對照表：**有需要才更新**
    - 於<a href="https://www.taifex.com.tw/cht/2/stockLists">此網站</a>下載csv後，依照對應格式(可以參考 `utils\doc\證券與期貨代碼對照表.csv` )進行整理，完成後直接取代該檔案即可
- 股票名稱檔：**有需要才更新**
    - 置於 `utils\doc\StockName.csv`，為證券名稱與證券代碼的對照檔，由老王提供，若有更改才需要更新
        
<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、運作機制補充 
<hr>

### 1. 更新紀錄
  - 2022/03/25：Version 1上線，提供4種報表產生

<br>

### 2. 各式補充

- 程式檔案關聯圖：交流影片有講解運作流程與邏輯
![](https://i.imgur.com/jfrnMKx.png)

- 除權息事件處理方式：請參考此文件最下面 <a href="https://paper.dropbox.com/doc/_--BegCJuPAzTCVXLx52G_QgvDdAg-vI9bvKTvS6SKR3dAbnyAl">帳務系統_說明文件</a>

- 期貨結算事件處理方式：請參考此文件最下面 <a href="https://paper.dropbox.com/doc/_--BegCJuPAzTCVXLx52G_QgvDdAg-vI9bvKTvS6SKR3dAbnyAl">帳務系統_說明文件</a>
