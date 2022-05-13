import pandas as pd
from utils.record2IAID_future_IncomedfRow import record2IAID_future_IncomedfRow
from utils.record2IAID_stock_IncomedfRow import record2IAID_stock_IncomedfRow
from utils.read_futureinfo import read_futureinfo
futureinfo_df = read_futureinfo()
future_ratio_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["連結比例"].tolist()))
future_commision_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["手續費"].tolist()))

def record2IAID_both_FIFO(Record_df, logtype):
#     print("Record_df:")
#     display(Record_df)        
    # colname_list會同時影響到損益報表 & 庫存明細報表
    if logtype == "Futures":
        colname_list = [x for x in Record_df.columns if x not in ['成交數量', '現時庫存', '手續費', '交易稅']]
    elif logtype == "Stocks":
        colname_list = [x for x in Record_df.columns if x not in ['成交數量', '現時庫存', '被當沖數量', '手續費', '交易稅', '融券費']]
#         print("colname_list:", colname_list)        
    
    # 以下為損益報表    
    target_list = Record_df["商品代碼"].unique()
    Income_df = pd.DataFrame()
    InventoryDetail_df = pd.DataFrame()
    for target in target_list:
        target_df = Record_df[Record_df["商品代碼"] == target].copy()
#         print("target_df:")
#         display(target_df)
        buy_queue = []
        sell_queue = []
        for i, row in target_df.iterrows():
            deal_num = abs(row["成交數量"])
            _row = row[colname_list].copy()
            if row["買/賣"] == "sBuy":
                for j in range(deal_num):
                    if len(sell_queue) == 0:
                        buy_queue.append(_row)
                    else:
                        if logtype == "Futures" and row["商品代碼"][2] != "1": # 無論今日委託明細或是昨日庫存報表出現除權後的期貨(XX1XX)，一律整行忽略，需等到結算平倉，亦即不會與任何交易沖銷。
                            incomedf_row = record2IAID_future_IncomedfRow(sell_queue.pop(0), _row)
                            Income_df = Income_df.append(incomedf_row)
                        elif logtype == "Stocks":
                            incomedf_row = record2IAID_stock_IncomedfRow(sell_queue.pop(0), _row)
                            Income_df = Income_df.append(incomedf_row)
            elif row["買/賣"] == "sSell":
                for j in range(deal_num):
                    if len(buy_queue) == 0:
                        sell_queue.append(_row)
                    else:
                        if logtype == "Futures" and row["商品代碼"][2] != "1": # 同上
                            incomedf_row = record2IAID_future_IncomedfRow(buy_queue.pop(0), _row)
                            Income_df = Income_df.append(incomedf_row)
                        elif logtype == "Stocks":
                            incomedf_row = record2IAID_stock_IncomedfRow(buy_queue.pop(0), _row)
                            Income_df = Income_df.append(incomedf_row)
        buy_queue = [x.to_frame().T for x in buy_queue]
        sell_queue = [x.to_frame().T for x in sell_queue]
        # print(buy_queue)
        # print(sell_queue)
        InventoryDetail_df = InventoryDetail_df.append(buy_queue + sell_queue)
     
    # 以下為庫存明細報表
    InventoryDetail_df["庫存數量"] = InventoryDetail_df["買/賣"].apply(lambda x: 1 if x == "sBuy" else -1)
    InventoryDetail_df = InventoryDetail_df.groupby([x for x in list(InventoryDetail_df.columns) if x not in ["唯一Hash碼", "庫存數量"]], sort=False, as_index=False).agg({'庫存數量':'sum'}).sort_values(by=["商品代碼","成交日期","成交時間"]) # groupby唯一Hash碼會有bug
    if logtype == "Futures":
        InventoryDetail_df["手續費"] = 0.0
        InventoryDetail_df["交易稅"] = 0.0
        for i, row in InventoryDetail_df.iterrows():
            ratio = future_ratio_dict[row["商品代碼"][:2]]  
            commision = future_commision_dict[row["商品代碼"][:2]]  
            InventoryDetail_df.at[i, "手續費"] = commision * abs(row["庫存數量"])
            InventoryDetail_df.at[i, "交易稅"] = round(row["成交單價"] * abs(row["庫存數量"]) * ratio * 0.00002, 2) # 四捨五入到小數點後第二位  
    elif logtype == "Stocks":
        InventoryDetail_df["手續費"] = 0.0
        InventoryDetail_df["交易稅"] = 0.0
        InventoryDetail_df["融券費"] = 0.0
        for i, row in InventoryDetail_df.iterrows():
            commision = 0.001425 * 1000 * abs(row["庫存數量"]) * row["成交單價"] * 0.3
            tax = 0.003 * 1000 * abs(row["庫存數量"]) * row["成交單價"] if row["買/賣"] == "sSell" else 0
            lendingExp = 0.0008 * 1000 * abs(row["庫存數量"]) * row["成交單價"] if row["交易類別"] == "融券" and row["買/賣"] == "sSell" else 0
            InventoryDetail_df.at[i, "手續費"] = round(commision, 2) # 四捨五入到小數點後第二位  
            InventoryDetail_df.at[i, "交易稅"] = round(tax, 2)# 四捨五入到小數點後第二位 
            InventoryDetail_df.at[i, "融券費"] = round(lendingExp, 2)# 四捨五入到小數點後第二位        
    return Income_df, InventoryDetail_df