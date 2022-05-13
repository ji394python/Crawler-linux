import pandas as pd
def log2record_both_CurretInventory(Record_df, base_df):
    target_list = Record_df["商品代碼"].unique()
    result_list = []
    for target in target_list:
        target_df = Record_df[Record_df["商品代碼"] == target].copy()
        previous_stock = 0
        if not base_df.empty:
            previous_stock = base_df[base_df["商品代碼"] == target]["庫存數量"].sum()
        target_df["現時庫存"] = target_df["成交數量"].cumsum() + previous_stock
        result_list.append(target_df)
    Record_df = pd.concat(result_list)
    return Record_df