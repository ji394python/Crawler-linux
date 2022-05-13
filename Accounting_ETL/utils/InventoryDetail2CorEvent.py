import pandas as pd
from datetime import datetime
from utils.read_futureinfo import read_futureinfo
from utils.InventoryDetail2CorEvent_genRow import InventoryDetail2CorEvent_genRow

def InventoryDetail2CorEvent(InventoryDetail_df, logtype, date):
    # # 上櫃公司
    # otc_df = pd.read_csv("./utils/doc/證券_除權除息預告表/Exright_STK_2021_08_31.csv", encoding="cp950")
    # otc_df = otc_df[~otc_df["除權息"].isna()]
    
    # # 上市公司
    # listed_df = pd.read_csv("./utils/doc/證券_除權除息預告表/TWT48U.csv", encoding="cp950")
    # listed_df = listed_df[~listed_df["除權息"].isna()]
    # listed_df.rename(columns={"除權除息日期": "除權息日期"}, inplace=True)
    # listed_df["除權息日期"] = listed_df["除權息日期"].apply(lambda x: x.replace("年","/").replace("月","/").replace("日",""))
    # listed_df["股票代號"] = listed_df["股票代號"].apply(lambda x: x.replace("=","").replace("\"",""))
    # listed_df["除權息"] = listed_df["除權息"].apply(lambda x: "除"+x)
    
    # # 上市/上櫃公司合併
    # colname_list = ["除權息日期", "股票代號", "名稱", "除權息", "無償配股率", "現金股利"]
    # octMlisted_df = pd.concat([otc_df[colname_list], listed_df[colname_list]])
    # octMlisted_df["除權息日期"] = octMlisted_df["除權息日期"].apply(lambda x: str(int(x[:3])+1911)+x[3:])
    # octMlisted_df = octMlisted_df.astype({"無償配股率": str, "現金股利": str})
    # octMlisted_df = octMlisted_df[octMlisted_df["無償配股率"]!="尚未公告"]
    # octMlisted_df = octMlisted_df[octMlisted_df["現金股利"]!="尚未公告"]
    # octMlisted_df = octMlisted_df.astype({"股票代號": str, "無償配股率": float, "現金股利": float})

    # # 結合期貨代碼資訊
    # futureinfo_df = read_futureinfo().rename(columns={"商品代碼":"期貨代碼", "證券代號":"股票代號"})
    # octMlisted_df = octMlisted_df.merge(futureinfo_df, how='left', on='股票代號')
    
    # octMlisted_df.to_csv("./utils/doc/證券_除權除息預告表/上市和上櫃公司除權除息預告表.csv", encoding = "cp950", index=False)
    
    octMlisted_df = pd.read_csv('utils\doc\證券_除權除息預告表\上市和上櫃公司除權除息預告表.csv',encoding='ms950')
    octMlisted_df["除權息日期"] = octMlisted_df["除權息日期"].apply(lambda x: datetime.strftime(datetime.strptime(x,'%Y/%m/%d'),'%Y-%m-%d'))
    octMlisted_df['股票代號'] =  octMlisted_df['股票代號'].astype(str)
    octMlisted_df = octMlisted_df[octMlisted_df["除權息日期"]==date]
    result_list = []
    for i, row in octMlisted_df.iterrows():
        if logtype == "Stocks":
            matchs = InventoryDetail_df[InventoryDetail_df["商品代碼"]==row["股票代號"]]
            for idx, match in matchs.iterrows():
        #         display(match)
                adjust_cash = row["現金股利"] * match["庫存數量"] * 1000
                adjust_stock = row["無償配股率"] * match["庫存數量"] * 1000
                if row["除權息"] == "除息":
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(match["商品代碼"], adjust_cash, 0)
                elif row["除權息"] == "除權":
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(match["商品代碼"], 0, adjust_stock)           
                elif row["除權息"] == "除權息":
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(match["商品代碼"], adjust_cash, adjust_stock)
                result_list.append(corpEventdf_row)
        elif logtype == "Futures":  
            matchs = InventoryDetail_df[InventoryDetail_df["商品代碼"].apply(lambda x: x[:2])==row["期貨代碼"]]
            for idx, match in matchs.iterrows():
        #         display(match)
                adjust_cash = row["現金股利"] * match["庫存數量"] * row["連結比例"]
                adjust_stock = row["無償配股率"] * match["庫存數量"] * row["連結比例"]
                if row["除權息"] == "除息":
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(match["商品代碼"], adjust_cash, 0)
                elif row["除權息"] == "除權":
                    InventoryDetail_df.at[idx, "商品代碼"] = InventoryDetail_df.at[idx, "商品代碼"][:2]+"1"+InventoryDetail_df.at[idx, "商品代碼"][3:]
                    InventoryDetail_df.at[idx, "商品名稱"] = InventoryDetail_df.at[idx, "商品名稱"].replace("期貨", "期一")
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(InventoryDetail_df.at[idx, "商品代碼"], 0, adjust_stock)
                elif row["除權息"] == "除權息":
                    InventoryDetail_df.at[idx, "商品代碼"] = InventoryDetail_df.at[idx, "商品代碼"][:2]+"1"+InventoryDetail_df.at[idx, "商品代碼"][3:]
                    InventoryDetail_df.at[idx, "商品名稱"] = InventoryDetail_df.at[idx, "商品名稱"].replace("期貨", "期一")
                    corpEventdf_row = InventoryDetail2CorEvent_genRow(InventoryDetail_df.at[idx, "商品代碼"], adjust_cash, adjust_stock)
                result_list.append(corpEventdf_row)
    CorEvent_df = pd.DataFrame(columns=("商品代碼", "現金股利調整數", "股票股利調整數"))
    if len(result_list) != 0:
        CorEvent_df = pd.concat(result_list)
        CorEvent_df = CorEvent_df.groupby([x for x in CorEvent_df.columns if x not in ["現金股利調整數", "股票股利調整數"]], sort=False, as_index=False).agg(lambda x: round(sum(x), 2)) # 合併相同交易        
    # display(InventoryDetail_df[condition2])
    return CorEvent_df