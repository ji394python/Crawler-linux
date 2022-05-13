import pandas as pd
def log2record_future_position(Record_df, base_df, verbose = False):
    target_list = Record_df["商品代碼"].unique()
    result_list = []
    for target in target_list:
        target_df = Record_df[Record_df["商品代碼"] == target].copy()
        previous_stock = 0
        if not base_df.empty:
            previous_stock = base_df[base_df["商品代碼"] == target]["庫存數量"].sum()        
        target_df["倉別"] = "" 
        row2_list = []
        for i, row in target_df.iterrows():
            current_stock = row["現時庫存"]
            if previous_stock * current_stock < 0: # 情況一: 現時庫存由正轉負or由負轉正，代表同時發生平倉&新倉，需拆成兩筆交易
                row2 = target_df.loc[[i]].copy()
                if verbose:
                    verbose_col = ['成交日期','成交時間','商品代碼','買/賣','成交數量','現時庫存','倉別','成交單價','委託書號']
                    display(target_df.loc[:i,verbose_col].iloc[-2:,])
                target_df.at[i, "成交數量"] = 0 - previous_stock
                target_df.at[i, "現時庫存"] = 0
                target_df.at[i, "倉別"] = "平倉"
                row2.at[i, "成交數量"] = current_stock - 0
                row2.at[i, "現時庫存"] = current_stock
                row2.at[i, "倉別"] = "新倉"
                row2 = row2.rename(index={i: i+0.5})
                row2_list.append(row2)
                if verbose:
                    verbose_col = ['成交日期','成交時間','商品代碼','買/賣','成交數量','現時庫存','倉別','成交單價','委託書號']
                    display(pd.concat([
                        target_df.loc[:i,verbose_col].iloc[-2:,], 
                        row2.loc[:,verbose_col]
                    ]))
            elif abs(previous_stock) < abs(current_stock): # 情況二: 現時庫存的絕對值增加，代表為新倉
                target_df.at[i, "倉別"] = "新倉"
            elif abs(previous_stock) > abs(current_stock): # 情況三: 現時庫存的絕對值減少，代表為平倉
                target_df.at[i, "倉別"] = "平倉"
            previous_stock = current_stock
        for row2 in row2_list: # 將需被新增的row2插入target_df
            target_df = target_df.append(row2)
        result_list.append(target_df)
            
    Record_df = pd.concat(result_list)
    Record_df.sort_index(inplace = True)
    Record_df.reset_index(drop = True, inplace = True) 
    return Record_df