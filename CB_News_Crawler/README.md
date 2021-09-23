## 一、執行與維護方式
<hr>

### 1. 執行方式
- <span style="font-weight:bold;color:#ffaaaa">[專案一] </span> 公司債公告爬蟲&分類程序
  - Step1：修改 `set.json` 將根路徑改為需要的
  - Step2：執行 `python CB_News_Crawler.py` 爬取昨日公司債新聞 
  - Step3：執行 `python CB_News_Report.py` 分類新聞
  - Step4： 資料預計於 `Step1` 所設定的路徑下，產生 `News_CB/{Date}` 與 `News_CB/Standard` 的資料夾
    - `{Date}`：將爬取的公告按照日期分類
    - `Standard`：分類的新聞結果將置於此

<br>

- <span style="font-weight:bold;color:#ffaaaa">[專案二] </span> 公司債基本資料爬蟲&剖析
  - Step1：修改 `set.json` 將根路徑改為需要的
  - Step2：執行 `python CB_Specs_Crawler.py` 爬取當前公司債基本資料並進行剖析
  - Step4： 資料預計於 `Step1` 所設定的路徑下，產生 `CB_Specs/CB_OTC_{Date}`的剖析資料
    - 原先會產生爬取的基本資料，但在程序過程中已刪除
<br>



### 2.維護方式
  - 更新手冊：
    - 更新`set.json`路徑 (測試機：`"../../NasHome"`，正式機：`"../../NasPublic"`)


<br>
<hr>
<hr>
<br>
<br>
<br>

## 二、輸出檔案介紹
<hr>

###  1. 輸出目錄結構
- 舉例：`News_CB\{Date}\{Ticker}_{Date}_{index}.txt` -> 公司債公告原始檔路徑
<br>
<br>
- 舉例：`News_CB\Standard\` -> 公司債公告分類結果檔
  - `CB_Conversion_Halt.csv`：停止轉(交)換公告
  - `CB_Strike_Adjust.csv`：價格變更公告
  - `CB_Redemption.csv`：強制贖回公告
<br>
<br>
- 舉例：`CB_Specs\CB_OTC_{Date}.csv` -> 公司債基本資料剖析結果檔

<pre>
└News_CB
  └${Date}
    └ ${Ticker}_${Date}_${Time}.txt

└CB_Specs
  └CB_OTC_${Date}.csv 

</pre>

### 2. 輸出檔案結構

- 原始檔案：如 `News_CB\{Date}\{Ticker}_{Date}_{index}.txt`
<pre>
一、公告序號：1
二、主旨：公告京鼎股份有限公司第一次無擔保轉換公司債轉換公司債(簡稱：京鼎一，代碼：34131)因於110年01月23日到期暨訂於110年01月25日終止櫃檯買賣等相關事宜。
三、依據：依京鼎一發行及轉換辦法規定辦理。
...
...
</pre>

<br>

- 剖析檔案：
  - <span style="font-weight:bold;color:#ffaaaa">[專案一] </span> 公司債公告爬蟲&分類程序
    - 停止轉換：`News_CB\Standard\CB_Conversion_Halt.csv` 

        | 公告日 | 可轉債名稱 | 可轉債代號 | 停止轉換起日 | 停止轉換迄日 |
        |:--------:|:--------:|:--------------:|:--------------:|:-------|
        | 2021/1/6 | 麗清二 | 33462 | 2021/1/14 | 2021/2/8 |
        | 2021/1/8 | 凡甲四 | 35264 | 2021/1/18 | 2021/3/26 |
        | 2021/1/8 | 鈺齊五KY | 98025 | 2021/2/3 | 2021/3/11 |
        |  ...    |  ...    |  ...    |  ...    |  ...    |

    - 強制贖回：`News_CB\Standard\CB_Redemption.csv` 
      | 公告日 | 可轉債名稱 | 可轉債代號 | 強制贖回日 | 最後轉換日 |
      |:--------:|:--------:|:--------------:|:--------------:|:-------|
      | 2021/1/4 | 京鼎一 | 34131 | 2021/1/23 | 2021/1/22 |
      | 2021/1/11 | 智伸科一 | 45511 | 2021/2/5 | 2021/2/4 |
      | 2021/1/13 | 湧德二 | 36892 | 2021/2/5 | [] |
      |  ...    |  ...    |  ...    |  ...    |  ...    |

    - 價格調整：`News_CB\Standard\CB_Strike_Adjust.csv` 

      | 公告日 | 可轉債名稱 | 可轉債代號 | 調整生效日 | 原訂轉換價 | 調整後轉換價 |
      |:--------:|:--------:|:--------------:|:--------------:|:-------|:-------|
      | 2021/1/4 | 久陽二 | 50112 | 2021/1/20 | 18.9 | 18 |
      | 2021/1/4 | 久陽三 | 50113 | 2021/1/20 | 18.9 | 18 |
      |  ...    |  ...    |  ...    |  ...    |  ...    | ... |

  - <span style="font-weight:bold;color:#ffaaaa">[專案二] </span> 公司債基本資料爬蟲&剖析 (共69個欄位)

      |債券代碼|債券簡稱|債券中文名稱|債券英文名稱|債券期別|募集方式| ... |
      |:--------:|:--------:|:--------------:|:--------------:|:-------|:-------|:-------|
      |12582|其祥二KY|其祥...|Kee...|2|委託承銷商公開銷售| ... |
      |13163|上曜三|上曜...|SUN...|3|委託承銷商公開銷售| ... |
      |  ...    |  ...    |  ...    |  ...    |  ...    | ... | ... |





<br>
<hr>
<hr>
<br>
<br>
<br>

## 三、程式碼介紹
<hr>

### 1. 前置資料檔
- `set.json`：路徑設定檔
- `rule.txt`：公告種類檔

### 2. 程式檔功能
- `CB_News_Crawler.py`：爬取公司債公告的主程式檔 (執行頻率：1/days)
  - 爬取網站：https://mops.twse.com.tw/mops/web/t108sb08_1_q2
- `CB_News_Report.py`：分類爬取的公司債公告之主程式檔 (執行頻率：1/days)
- `CB_Specs_Crawler.py`：爬取與剖析公司債基本資料的主程式檔 (執行頻率：1/days)
  - 爬取網站：https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw


### 3.各檔案執行範例

- 爬取時間區間公告：`python CB_News_Crawler.py -st 2021/06/01 -et 2021/06/30`
  - 即為爬取6/1~6/30號的所有公告
  - 可於路徑 `${root}/News_CB` 找到爬取檔案


- 爬取昨日公告：`python CB_News_Crawler.py`
  - 預設爬取前一日之公告
  - 可於路徑 `${root}/News_CB`找到爬取檔案

<br>

- 執行分類檔案：`python CB_News_Report.py`
  - 可於路徑 `${root}/News_CB/Standard` 找到分類檔案

<br>

- 爬取與剖析公司債基本資料：`python CB_Specs_Crawler.py`
  - 可於路徑 `${root}/CB_Specs` 找到剖析檔案


<br>
<hr>
<hr>
<br>
<br>
<br>

## 四、運作機制補充 <span style="font-size:12px"> 記錄各種補充事項 </span>
<hr>

### 1. 更新紀錄
  -

### 2. 規則補充
  - 強制贖回公告 (`CB_Redemption.csv`)的最後轉換日因並無固定的格式，目前僅以Regex之 <code>"最遲應於([0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日"</code>進行文字篩選

### 3. 各種參考網站

- 目標網站：
  - 公開資訊觀測站：<a href="https://mops.twse.com.tw/mops/web/t108sb08_1_q2">轉換(附認股權)公司債公告彙總表</a>
  - 櫃買中心：<a href="https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw">公司債基本資料網站</a>
