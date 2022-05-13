#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import time


# ## 期貨


stockName_df = pd.read_csv("./utils/doc/StockName.csv", encoding="utf-8-sig")
chinese2stockcode_dict = dict(zip(stockName_df["股票名稱"].tolist(), stockName_df["股票代碼"].tolist())) # str to str

from utils.read_futureinfo import read_futureinfo
futureinfo_df = read_futureinfo()
# 先忽略小型期貨的情況，一律視作大型期貨(ex. 看到"2330"就對到"CD"，而非"QF")
stockcode2futurecode_dict = dict()
for i, row in futureinfo_df.iterrows():
    if row["證券代號"] not in stockcode2futurecode_dict.keys():
        stockcode2futurecode_dict[row["證券代號"]] = row["商品代碼"]

# 待處理: 不確定券商提供的庫存報表中，指數期貨的中文名稱為何，以下只有確定「小型台指」而已。
indexfuture_dict = {"台指期":"TX",
                    "小型台指":"MX",
                    "電子期":"TE",
                    "小型電子":"ZE",
                    "金指期":"TF"}
diff_dict = {"日月光投控": "日月光投控",
             "中租KY": "中租-KY",
             "F-TPK": "TPK-KY",
             "台灣高鐵期": "台灣高鐵",
             "京元電": "京元電子",
             "F-亞德": "亞德客-KY",
             "F-臻鼎":"臻鼎-KY"}
def chinese2futurecode(text):
    # print(text)
    flag = "F"
    if("期貨一" in text or "期一" in text):
        flag = "1"
    text = text.replace("期貨一", "").replace("期一", "").replace("期貨", "")
    if text in indexfuture_dict.keys():
        return indexfuture_dict[text] + flag
    elif text in diff_dict.keys():
        return stockcode2futurecode_dict[chinese2stockcode_dict[diff_dict[text]]] + flag
    else:
        try:
            return stockcode2futurecode_dict[chinese2stockcode_dict[text]] + flag
        except:
            print(f"出現沒見過的標的{text}，納入例外表 (./utils/doc/new.csv) 中")
            with open("./utils/doc/new.csv",'a+',newline='',encoding='utf-8-sig') as f:
                f.write(text)
                f.write('\n')
                f.close()


def SinoPac_Future_preprocess(futureInventory_day0):
    # 讀檔
    temp = pd.read_csv(futureInventory_day0, encoding = "cp950")
    temp = temp.dropna()
    # 產生一般欄位
    output = pd.DataFrame()
    output['買/賣'] = temp["買賣"].apply(lambda x: "sSell" if x == "賣" else "sBuy")
    Future_startDate = futureInventory_day0.split("_")[-1].replace(".csv", "")
    output["成交日期"] = Future_startDate
    month_dict = {"01":"A", "02":"B", "03":"C", "04":"D", "05":"E", "06":"F", "07":"G", "08":"H", "09":"I", "10":"J", "11":"K", "12":"L"}
    output['商品代碼'] = temp["商品名稱"].apply(lambda x: chinese2futurecode(x[:-3])+month_dict[x[-2:]]+str(time.gmtime().tm_year)[-1]) #打的日期有問題 
    output['成交單價'] = temp["成交均價/成交價"].astype(str).apply(lambda x: float(x.replace(",","")))
    output['商品名稱'] = temp["商品名稱"]
    output["庫存數量"] = temp["買賣"] + temp["口數"].astype(str).apply(lambda x: x.replace(",",""))
    output["庫存數量"] = output["庫存數量"].apply(lambda x: int(float(x.replace("賣", "-").replace("買", ""))))
    # count = 0
    # for v in output["庫存數量"].values:
    #     try:
    #         int(v.replace("賣", "-").replace("買", ""))
    #     except:
    #         print(count,v,v.replace('賣','-'),v.replace('買',''),type(v.replace('賣','-')))
    #     count+=1
    # 產生空白欄位
    for col_name in ['成交時間', '倉別', '委託價格型態', '委託有效日', '委託型態', 'NID', '委託書號', 'LeavesQty', '手續費', '交易稅', "UNIX時間", "保留碼"]:
        output[col_name] = "無此資訊"
    # 調整欄位順序
    output = output[["成交日期", "成交時間", "委託型態", "商品代碼", "商品名稱", "買/賣", "倉別", "成交單價", "手續費", "交易稅", "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", "庫存數量"]]
    # 存成csv檔
    new_string = futureInventory_day0[:-4]+"(改).csv"
    output.to_csv(new_string, encoding = "cp950", index=False)
    return output, new_string


# ## 證券


def SinoPac_Stock_preprocess(stockInventory_day0,export=True):
    # 讀檔
    temp = pd.read_csv(stockInventory_day0, encoding = "cp950")
    temp = temp.dropna()
    # 產生一般欄位
    output = pd.DataFrame()
    output['交易類別'] = temp["類別"]
    output['買/賣'] = output['交易類別'].apply(lambda x: "sSell" if x == "融券" else "sBuy")
    Stock_startDate = stockInventory_day0.split("_")[-1].replace(".csv", "")
    output["成交日期"] = Stock_startDate
    output['商品代碼'] = temp["股票代碼"].astype(str).apply(lambda x: "00" + x if len(x) < 4 else x) # 少於四位數就補兩個0
    output['成交單價'] = temp["成本均價"].astype(str).apply(lambda x: float(x.replace(",","")))
    output['商品名稱'] = temp["股票名稱"]
    output["庫存數量"] = output["買/賣"] + temp["即時庫存"].astype(str).apply(lambda x: int(x.replace(",",""))//1000).astype(str) # 待處理: 零股交易
    output["庫存數量"] = output["庫存數量"].apply(lambda x: int(x.replace("sSell", "-").replace("sBuy", "")))
    # 產生空白欄位
    for col_name in ['成交時間', '委託價格型態', '委託有效日', '委託型態', '委託書號', '手續費', '交易稅', '融券費', "UNIX時間", "保留碼"]:
        output[col_name] = "無此資訊"
    # 調整欄位順序
    output = output[["成交日期", "成交時間", "委託型態", "商品代碼", "商品名稱", "買/賣", "交易類別", "成交單價", "手續費", "交易稅", "融券費", "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", "庫存數量"]]
    # 存成csv檔
    new_string = stockInventory_day0[:-4]+"(改).csv"
    if export:
        output.to_csv(new_string, encoding = "cp950", index=False)    
    return output, new_string

