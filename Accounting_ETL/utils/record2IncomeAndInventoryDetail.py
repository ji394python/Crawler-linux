import pandas as pd
from utils.record2IAID_both_FIFO import record2IAID_both_FIFO
from utils.record2IAID_future_ClosePosition import record2IAID_future_ClosePosition

def record2IncomeAndInventoryDetail(Record_df, logtype, base_df, date):
    if logtype == "Futures":
        # step1: 先進先出(昨日庫存明細報表(base_df)+今日成交報表(Record_df))
        if not base_df.empty:
            base_df = base_df.rename(columns={"庫存數量": "成交數量"})
            Record_df = Record_df.drop(columns=['現時庫存'])
            Record_df = pd.concat([base_df, Record_df])
        Income_df, InventoryDetail_df = record2IAID_both_FIFO(Record_df, logtype)
        # step2: 結算平倉
        Income_df2, InventoryDetail_df2 = record2IAID_future_ClosePosition(InventoryDetail_df, date)
        Income_df, InventoryDetail_df = [pd.concat([Income_df, Income_df2]), InventoryDetail_df2]
        # step3: 合併相同交易
        Income_df = Income_df.groupby([x for x in Income_df.columns if x not in ["進出場口數", "平倉損益", "手續費", "交易稅", "淨損益"]], sort=False, as_index=False).agg(lambda x: round(sum(x), 2))
        # step4: 調整欄位順序
        Income_df = Income_df[["進場日期", "出場日期", "進場買賣別", "出場買賣別", "出場類別：交易/結算", "商品代碼", "商品名稱", "進出場口數", "進場成交價", "出場成交價", "平倉損益", "手續費", "交易稅", "淨損益", "進場委託書號", "出場委託書號"]]
        InventoryDetail_df = InventoryDetail_df[["成交日期", "成交時間", "委託型態", "商品代碼", "商品名稱", "買/賣", "倉別", "成交單價", "手續費", "交易稅", "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", "庫存數量"]]
    elif logtype == "Stocks":
        # step1: 先進先出-處理當日沖銷(今日成交報表(Record_df))
        Income_df, InventoryDetail_df = record2IAID_both_FIFO(Record_df, logtype)
        # step2: 先進先出-處理非當日沖銷(昨日庫存明細報表(base_df)+今日當沖完庫存明細報表(InventoryDetail_df))
        if not base_df.empty:
            base_df = base_df.rename(columns={"庫存數量": "成交數量"})
            InventoryDetail_df = InventoryDetail_df.rename(columns={"庫存數量": "成交數量"})
            Record_df = pd.concat([base_df, InventoryDetail_df])
            Income_df2, InventoryDetail_df2 = record2IAID_both_FIFO(Record_df, logtype)
            Income_df, InventoryDetail_df = [pd.concat([Income_df, Income_df2]), InventoryDetail_df2]
        # step3: 合併相同交易
        Income_df = Income_df.groupby([x for x in Income_df.columns if x not in ['進出場張數', '平倉損益', '手續費', '交易稅', '融券費','淨損益']], sort=False, as_index=False).agg(lambda x: round(sum(x), 2))
        # step4: 調整欄位順序
        Income_df = Income_df[['進場日期', '出場日期', '是否為當日沖銷', '進場買賣別', '出場買賣別', "進場交易類別", "出場交易類別", '商品代碼', '商品名稱', '進出場張數', '進場成交價', '出場成交價', '平倉損益', '手續費', '交易稅', '融券費', '淨損益', '進場委託書號', '出場委託書號']]
        InventoryDetail_df = InventoryDetail_df[["成交日期", "成交時間", "委託型態", "商品代碼", "商品名稱", "買/賣", "交易類別", "成交單價", "手續費", "交易稅", "融券費", "委託價格型態", "委託有效日", "委託書號", "UNIX時間", "保留碼", "庫存數量"]]

    return Income_df, InventoryDetail_df