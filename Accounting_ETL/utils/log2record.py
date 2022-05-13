import pandas as pd
from utils.log2record_both_CurretInventory import log2record_both_CurretInventory
from utils.log2record_future_position import log2record_future_position
from utils.read_futureinfo import read_futureinfo
futureinfo_df = read_futureinfo()
future_ratio_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["連結比例"].tolist()))
future_commision_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["手續費"].tolist()))
def log2record(logpath, logtype, base_df):
    # ---------------------- 新增所有基礎欄位 ---------------------- #    
    with open(logpath, 'r' , encoding='utf-8') as f:
        log_list = f.readlines()
    log_list = [line for line in log_list if "osPartiallyFilled" in line]
    log_list = [line.split(',') for line in log_list]
    if logtype == 'Futures':
        extend = [ line[7].split(' ') for line  in log_list]
        for line in range(len(log_list)):
            log_list[line].insert(8,extend[line][1])
            log_list[line][7] = extend[line][0]
    log_df = pd.DataFrame(log_list)

    colname_dict = dict()
    if logtype == "Futures":
        colname_dict = {
                        "成交日期": 0, "成交時間": 1, "委託型態": 2, "買/賣": 3, 
                        "成交數量": 4, "成交單價": 5, "商品代碼": 6, "商品簡稱": 7, 
                        "商品月份": 8, "委託價格型態": 9, "委託有效日": 10, "對齊留空": 11,
                        "委託書號": 12, "UNIX時間": 13, '保留碼': 14, '唯一Hash碼': 15
                        }
    elif logtype == "Stocks":
        colname_dict = {"成交日期": 0, "成交時間":1, "委託型態": 2, "買/賣": 3, "成交數量": 4, "成交單價": 5,
                        "商品代碼": 6, "商品名稱": 7, "委託價格型態": 8, "委託有效日": 9, "交易類別": 10,
                        "委託書號": 11, "UNIX時間": 12, '保留碼': 13, '唯一Hash碼': 14}
    Record_df = pd.DataFrame() 
    for key, value in colname_dict.items():
        Record_df[key] = log_df[value]
        
    # ---------------------- 編輯「成交單價」欄位：轉成浮點數 ---------------------- # 
    Record_df = Record_df.astype({"成交單價": float})
    # ---------------------- 編輯「成交時間」欄位：加空白讓它在excel能正常顯示 ---------------------- #  
    Record_df["成交時間"] = Record_df["成交時間"].apply(lambda x: x.replace(".", " "))
    # ---------------------- 編輯「成交數量」欄位：加上正負號 ---------------------- #        
    Record_df["成交數量"] = Record_df["買/賣"] + Record_df["成交數量"]
    Record_df["成交數量"] = Record_df["成交數量"].apply(lambda x: int(x.replace("sSell", "-").replace("sBuy", "")))       
    
    ## 以下為需對期貨與證券分開處理的欄位
    if logtype == "Futures":
        # ---------------------- 新增「商品名稱」欄位 ---------------------- #
        Record_df["商品名稱"] = Record_df["商品簡稱"] + " " + Record_df["商品月份"] 
        # ---------------------- 新增「現時庫存」欄位 ---------------------- #
        Record_df = log2record_both_CurretInventory(Record_df, base_df)
        # ---------------------- 新增「倉別」欄位 ---------------------- #
        Record_df = log2record_future_position(Record_df, base_df)
        # ---------------------- 新增「手續費」&「交易稅」欄位 ---------------------- #
        Record_df["手續費"] = 0.0
        Record_df["交易稅"] = 0.0
        for i, row in Record_df.iterrows():
            ratio = future_ratio_dict[row["商品代碼"][:2]]  
            commision = future_commision_dict[row["商品代碼"][:2]]  
            Record_df.at[i, "手續費"] = commision * abs(row["成交數量"])
            Record_df.at[i, "交易稅"] = round(row["成交單價"] * abs(row["成交數量"]) * ratio * 0.00002, 2) # 四捨五入到小數點後第二位
        # ---------------------- 調整欄位順序 ---------------------- #    
        Record_df = Record_df[['成交日期', '成交時間', "委託型態", '商品代碼', '商品名稱', "買/賣", '成交數量', '現時庫存', '倉別', '成交單價', '手續費', '交易稅', "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", '唯一Hash碼']]
    elif logtype == "Stocks":
        # ---------------------- 編輯「交易類別」欄位 ---------------------- #
        Record_df["交易類別"] = Record_df["交易類別"].apply(lambda x: "現股" if int(x)==0 else "融資" if int(x)==3 else "融券")
        # ---------------------- 新增「現時庫存」 欄位 ---------------------- #
        Record_df = log2record_both_CurretInventory(Record_df, base_df)
        # ---------------------- 新增「被當沖數量」 欄位 ---------------------- #
        target_list = Record_df["商品代碼"].unique()
        Record_df["被當沖數量"] = 0
        result_list = []
        for target in target_list:
            target_df = Record_df[Record_df["商品代碼"] == target].copy()
            buy_number = target_df[target_df["成交數量"]>0]["成交數量"].sum()
            sell_number = target_df[target_df["成交數量"]<0]["成交數量"].sum()
        #     print("buy_number: ", buy_number)
        #     print("sell_number: ", sell_number)
            for i, row in target_df.iterrows():
                if row["成交數量"] < 0:
                    target_df.at[i, "被當沖數量"] = max(-1*buy_number, row["成交數量"])
                    buy_number += target_df.at[i, "被當沖數量"]
                elif row["成交數量"] > 0:
                    target_df.at[i, "被當沖數量"] = min(-1*sell_number, row["成交數量"])
                    sell_number += target_df.at[i, "被當沖數量"]
                else:
                    raise("ERROR")
        #     display(target_df) 
            result_list.append(target_df)
        Record_df = pd.concat(result_list)  
        Record_df.sort_index(inplace = True)        
        # ---------------------- 新增「手續費」&「交易稅」&「融券費」欄位 ---------------------- #
        Record_df["手續費"] = 0.0
        Record_df["交易稅"] = 0.0
        Record_df["融券費"] = 0.0
        for i, row in Record_df.iterrows():
            commision = 0.001425 * 1000 * abs(row["成交數量"]) * row["成交單價"] * 0.3
            tax = 0.003 * 1000 * (abs(row["成交數量"])-abs(row["被當沖數量"])) * row["成交單價"] + \
            0.0015 * 1000 * abs(row["被當沖數量"]) * row["成交單價"] if row["買/賣"] == "sSell" else 0
            lendingExp = 0.0008 * 1000 * (abs(row["成交數量"])-abs(row["被當沖數量"])) * row["成交單價"] if row["交易類別"] == "融券" and row["買/賣"] == "sSell" else 0
            Record_df.at[i, "手續費"] = round(commision, 2) # 四捨五入到小數點後第二位  
            Record_df.at[i, "交易稅"] = round(tax, 2) # 四捨五入到小數點後第二位 
            Record_df.at[i, "融券費"] = round(lendingExp, 2) # 四捨五入到小數點後第二位
        # ---------------------- 調整欄位順序 ---------------------- #    
        Record_df = Record_df[['成交日期', '成交時間', "委託型態", '商品代碼', '商品名稱', "買/賣", "交易類別", '成交數量', '現時庫存', "被當沖數量", '成交單價', '手續費', '交易稅', '融券費', "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", '唯一Hash碼']]     
    return Record_df


