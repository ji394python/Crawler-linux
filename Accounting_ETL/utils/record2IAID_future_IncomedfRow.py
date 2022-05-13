import pandas as pd
from utils.read_futureinfo import read_futureinfo
futureinfo_df = read_futureinfo()
future_ratio_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["連結比例"].tolist()))
future_commision_dict = dict(zip(futureinfo_df["商品代碼"].tolist(), futureinfo_df["手續費"].tolist()))

def record2IAID_future_IncomedfRow(stock, daytrade): # stock為進場交易(倉別為新倉)，daytrade為出場交易(倉別為平倉)
#     print("stock: ")
#     display(stock.to_frame().T)
#     print("daytrade: ")
#     display(daytrade.to_frame().T) 
    if stock["買/賣"]=="sSell":
        price_diff = stock["成交單價"] - daytrade["成交單價"]
    elif stock["買/賣"]=="sBuy":
        price_diff =  daytrade["成交單價"] - stock["成交單價"]
#     print("price_diff: ", price_diff)
#     print("商品代碼: ", stock["商品代碼"][:2])
    ratio = future_ratio_dict[stock["商品代碼"][:2]]  
    commision = future_commision_dict[stock["商品代碼"][:2]]       

    income = round(price_diff * ratio, 3) # 四捨五入到小數點後第三位
    total_commision = commision * 2
    total_tax = round((stock["成交單價"] + daytrade["成交單價"]) * ratio * 0.00002, 3) # 四捨五入到小數點後第三位          
    net_income = income - total_commision - total_tax
#     print("income: ", income)
#     print("total_commision: ", total_commision)
#     print("total_tax: ", total_tax)
#     print("netincome: ", netincome)

    incomedf_row = pd.Series(data={"進場日期": stock["成交日期"],
                                   "出場日期": daytrade["成交日期"],
                                   "進場買賣別": stock["買/賣"],
                                   "出場買賣別": daytrade["買/賣"],
                                   "出場類別：交易/結算": "結算" if daytrade["委託書號"] == "無" else "交易",
                                   "商品名稱": stock["商品名稱"],
                                   "商品代碼": stock["商品代碼"],
                                   "進出場口數": 1,
                                   "進場成交價": stock["成交單價"],
                                   "出場成交價": daytrade["成交單價"],
                                   "平倉損益": income,
                                   "手續費": total_commision,
                                   "交易稅": total_tax,
                                   "淨損益": net_income,
                                   "進場委託書號": stock["委託書號"],
                                   "出場委託書號": daytrade["委託書號"]})
    incomedf_row = incomedf_row.to_frame().T
#     print("incomedf_row: ")
#     display(incomedf_row)    
    return incomedf_row