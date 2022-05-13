import pandas as pd
from utils.InventoryDetail2CorEvent import InventoryDetail2CorEvent
from utils.read_futureinfo import read_futureinfo
futureinfo_df = read_futureinfo()
futurecode2stockcode_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["證券代號"].tolist()))
futurecode2ratio_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["連結比例"].tolist()))

def InventoryDetail2Inventory(InventoryDetail_df, logtype, date):
    # 處理除權息事件
    CorEvent_df = InventoryDetail2CorEvent(InventoryDetail_df, logtype, date) # 除權息事件會影響到庫存明細報表的期貨代碼(XXFXX -> XX1XX)以及中文名稱(XXX期貨->XXX期一)
    # display(CorEvent_df)
    
    if logtype == "Stocks":
        temp_df = InventoryDetail_df.groupby(["商品代碼", "商品名稱","交易類別"]).agg({'庫存數量':'sum'})
        temp_df = temp_df[temp_df["庫存數量"]!=0].reset_index()        
        Inventory_df = temp_df[["商品代碼", "商品名稱"]].drop_duplicates()
        Inventory_df["現股"] = 0 
        Inventory_df["融資"] = 0 
        Inventory_df["融券"] = 0 
        Inventory_df["庫存數量"] = 0 
        for i, row in Inventory_df.iterrows():
            for t in ["現股", "融資", "融券"]:
                Inventory_df.at[i, t] = temp_df[(temp_df['交易類別'] == t) & (temp_df['商品代碼'] == row["商品代碼"])]["庫存數量"].sum() # 可以是空series.sum()
            Inventory_df.at[i, "庫存數量"] = Inventory_df.at[i, "現股"] + Inventory_df.at[i, "融資"] + Inventory_df.at[i, "融券"]        
    elif logtype == "Futures":
        Inventory_df = InventoryDetail_df.groupby(["商品代碼", "商品名稱"]).agg({'庫存數量':'sum'})
        Inventory_df = Inventory_df[Inventory_df["庫存數量"]!=0].reset_index()
        Inventory_df["證券代碼"] = Inventory_df['商品代碼'].apply(lambda x: futurecode2stockcode_dict[x[:2]])
        Inventory_df["連結比例"] = Inventory_df['商品代碼'].apply(lambda x: futurecode2ratio_dict[x[:2]])
        Inventory_df = Inventory_df[["商品代碼", "商品名稱", "證券代碼", "連結比例", "庫存數量"]]
    # display(Inventory_df)
    Inventory_df = Inventory_df.merge(CorEvent_df, how='left', on='商品代碼')
    # display(Inventory_df)
    return Inventory_df