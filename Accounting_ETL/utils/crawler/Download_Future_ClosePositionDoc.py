#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import os.path
from datetime import datetime

# In[2]:


def Download_Future_ClosePositionDoc(year, month): # ex. Download_Future_ClosePositionDoc("2021", "07")
    # 指數期貨
    parm_dict = {'start_year': year,
                 'start_month': month,
                 'end_year': year,
                 'end_month': month,
                 'button': "送出查詢" }
    r = requests.post('https://www.taifex.com.tw/cht/5/futIndxFSP', data = parm_dict)
    IndexFuture_df = pd.read_html(r.text)[3]
    IndexFuture_df.columns = [col_name.replace(" ", "").replace("<br>", "") for col_name in IndexFuture_df.columns]
    # IF_path = "../doc/期貨_最後結算價一覽表/指數期貨結算_{}.csv".format(parm_dict["end_year"] + parm_dict["end_month"])
    # IndexFuture_df.to_csv(IF_path, encoding = "cp950", index=False)
    
    # 股票期貨
    parm_dict2 = {'queryYear': year,
                  'queryMonth': month,
                  'button': "送出查詢" }
    r = requests.post('https://www.taifex.com.tw/cht/5/sSFFSP', data = parm_dict2)
    StockFuture_df = pd.read_html(r.text)[1]
    # SF_path = "../doc/期貨_最後結算價一覽表/股票期貨結算_{}.csv".format(parm_dict2["queryYear"] + parm_dict2["queryMonth"])
    # StockFuture_df.to_csv(SF_path, encoding = "cp950", index=False, errors = "ignore")
    
    # 合併
    temp = IndexFuture_df[(IndexFuture_df["契約月份"]==(year+month)) & (IndexFuture_df["臺股期貨/小型臺指期貨（TX/MTX）"]!="-")].iloc[0]
    IndexFuture_dict = dict({
        "商品名稱": ["台指期", "小台指期", "電子期", "小電子期", "金指期"],    
        "商品代號(兩碼)": ["TX", "MX", "TE", "ZE", "TF"],
        "到期日": [temp["最後結算日"]] * 5,
        "契約月份": [temp["契約月份"]] * 5,
        "最後結算價": [
            temp["臺股期貨/小型臺指期貨（TX/MTX）"], 
            temp["臺股期貨/小型臺指期貨（TX/MTX）"], 
            temp["電子期貨/小型電子期貨（TE/ZEF）"], 
            temp["電子期貨/小型電子期貨（TE/ZEF）"], 
            temp["金融期貨/小型金融期貨（TF/ZFF）"]
        ]
    })
    IndexFuture_df2 = pd.DataFrame(IndexFuture_dict).astype({"最後結算價": float})

    StockFuture_df["商品代號(兩碼)"] = StockFuture_df["商品代號"].apply(lambda x: x[:2])
    StockFuture_df2 = StockFuture_df[["商品名稱", "商品代號(兩碼)", "到期日", "契約月份", "最後結算價"]]    
    
    AllFuture_df = pd.concat([IndexFuture_df2, StockFuture_df2])

    AF_path = "../doc/期貨_最後結算價一覽表/期貨_最後結算價一覽表_{}.csv".format(year + month)
    AllFuture_df.to_csv(AF_path, encoding = "cp950", index=False, errors = "ignore")
    
    return IndexFuture_df2, StockFuture_df2, AllFuture_df

n = datetime.now().strftime('%m')
try:
    IndexFuture_df2, StockFuture_df2, AllFuture_df = Download_Future_ClosePositionDoc("2022", n)
except:
    pass
