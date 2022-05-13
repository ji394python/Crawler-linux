import pandas as pd

def record2IAID_stock_IncomedfRow(stock, daytrade):
#     print("stock: ")
#     display(stock.to_frame().T)
#     print("daytrade: ")
#     display(daytrade.to_frame().T) 
    sameDay_bool = stock["成交日期"] == daytrade["成交日期"]
    if stock["買/賣"]=="sSell":
        price_diff = stock["成交單價"] - daytrade["成交單價"]
        price_sellItem = stock["成交單價"]
        type_sellItem = stock["交易類別"]
    elif daytrade["買/賣"]=="sSell":
        price_diff =  daytrade["成交單價"] - stock["成交單價"]
        price_sellItem = daytrade["成交單價"]
        type_sellItem = daytrade["交易類別"]
#     print("price_diff: ", price_diff)
    
    total_commision = 0.001425 * 1000 * (stock["成交單價"] + daytrade["成交單價"]) * 0.3
    total_tax = 0.0015 * 1000 * price_sellItem if sameDay_bool else 0.003 * 1000 * price_sellItem
    total_lendingExp = 0.0008 * 1000 * price_sellItem if type_sellItem == "融券" and not sameDay_bool else 0
    
    income = round(price_diff * 1000, 3) # 四捨五入到小數點後第三位
    total_commision = round(total_commision, 3) # 四捨五入到小數點後第三位
    total_tax = round(total_tax, 3) # 四捨五入到小數點後第三位
    total_lendingExp = round(total_lendingExp, 3) # 四捨五入到小數點後第三位
    net_income = income - total_commision - total_tax - total_lendingExp
#     print("income: ", income)
#     print("total_commision: ", total_commision)
#     print("total_tax: ", total_tax)
#     print("total_lendingExp: ", total_lendingExp)
#     print("netincome: ", netincome)

    incomedf_row = pd.Series(data={"進場日期": stock["成交日期"],
                                   "出場日期": daytrade["成交日期"],
                                   "是否為當日沖銷": sameDay_bool, 
                                   "進場買賣別": stock["買/賣"],
                                   "出場買賣別": daytrade["買/賣"],
                                   "進場交易類別": stock["交易類別"],
                                   "出場交易類別": daytrade["交易類別"],                                   
                                   "商品名稱": stock["商品名稱"],
                                   "商品代碼": stock["商品代碼"],
                                   "進出場張數": 1,
                                   "進場成交價": stock["成交單價"],
                                   "出場成交價": daytrade["成交單價"],
                                   "平倉損益": income,
                                   "手續費": total_commision,
                                   "交易稅": total_tax,
                                   "融券費": total_lendingExp,
                                   "淨損益": net_income,                                   
                                   "進場委託書號": stock["委託書號"],
                                   "出場委託書號": daytrade["委託書號"]})
    incomedf_row = incomedf_row.to_frame().T
#     print("incomedf_row: ")
#     display(incomedf_row)    
    return incomedf_row