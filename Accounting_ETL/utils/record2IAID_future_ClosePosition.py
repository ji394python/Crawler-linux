from utils.record2IAID_future_IncomedfRow import record2IAID_future_IncomedfRow
from datetime import datetime
import pandas as pd
import os.path

def record2IAID_future_ClosePosition(InventoryDetail_df, date):
    today = date.replace("-","/")
    month_dict = {"A":"01", "B":"02", "C":"03", "D":"04", "E":"05", "F":"06", "G":"07", "H":"08", "I":"09", "J":"10", "K":"11", "L":"12"}
    year_dict = {"0":"2020", "1":"2021", "2":"2022", "3":"2023", "4":"2024", "5":"2025", "6":"2026", "7":"2027", "8":"2028", "9":"2029"}
    drop_list = []
    Income_df = pd.DataFrame()
    year, month = "", ""
    for index, row in InventoryDetail_df.iterrows():
        if (year != year_dict[row["商品代碼"][4]]) or (month != month_dict[row["商品代碼"][3]]): # 避免每次都要重新讀取資料
            year = year_dict[row["商品代碼"][4]]
            month = month_dict[row["商品代碼"][3]]
            file_path = "./utils/doc/期貨_最後結算價一覽表/期貨_最後結算價一覽表_{}.csv".format(year + month)
            if os.path.isfile(file_path):
                AllFuture_df = pd.read_csv(file_path, encoding = "cp950", keep_default_na=False, na_values=['']).astype({"最後結算價": float})
            else: # 期交所尚未公告該(year, month)的期貨結算資訊 or 該資料尚未被下載到./utils/doc裡面(可使用./Accounting/utils/crawler/Download_Future_ClosePositionDoc.py)
                AllFuture_df = pd.DataFrame()
        if AllFuture_df.empty:
            continue
        # print("商品代碼: ", row["商品代碼"])
        match = AllFuture_df[AllFuture_df["商品代號(兩碼)"]==row["商品代碼"][:2]].iloc[0]
        if datetime.strftime(datetime.strptime(match["到期日"],'%Y/%m/%d'),'%Y/%m/%d') == today:
            # print('結算：',year)
            drop_list.append(index)
            inventory_row = pd.Series({
                "買/賣": row["買/賣"],            
                "成交單價": row["成交單價"],
                "商品代碼": row["商品代碼"], 
                "成交日期": row["成交日期"],
                "商品名稱": row["商品名稱"],
                "委託書號": row["委託書號"]
            })
            close_row = pd.Series({
                "買/賣": "sBuy" if row["買/賣"] == "sSell" else "sSell",
                "成交單價": match["最後結算價"],
                "成交日期": match["到期日"],
                "委託書號": "無"
            })  
            incomedf_row = record2IAID_future_IncomedfRow(inventory_row, close_row)
            deal_num = abs(row["庫存數量"])
            for j in range(deal_num):
                Income_df = Income_df.append(incomedf_row)
    InventoryDetail_df = InventoryDetail_df.drop(drop_list)  
    return Income_df, InventoryDetail_df