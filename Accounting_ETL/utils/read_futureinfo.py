import pandas as pd
def read_futureinfo():
    # 股票期貨
    stockfutureinfo_df = pd.read_csv("./utils/doc/證券與期貨代碼對照表.csv", encoding="cp950", keep_default_na=False, na_values=['']).astype({"連結比例": int}).astype({"證券代號": int}).astype({"證券代號": str}) # 證券代號原先會被讀成float，故要先轉int再轉str。 ex. 2330.0(float) -> 2330(int) -> "2330"(str)
    FutureSetting = pd.read_csv("./utils/doc/FutureSetting.csv", encoding="cp950")
    stockfutureinfo_df = stockfutureinfo_df.merge(FutureSetting.loc[0:2, ["連結比例", "手續費"]], how='left', on='連結比例')
    
    # 指數期貨
    indexfutureinfo_df = FutureSetting.loc[3:7, ['商品代碼', '連結比例', '手續費']]
    indexfutureinfo_df["證券代號"] = ""

    # 合併
    futureinfo_df = pd.concat([stockfutureinfo_df, indexfutureinfo_df])
    return futureinfo_df