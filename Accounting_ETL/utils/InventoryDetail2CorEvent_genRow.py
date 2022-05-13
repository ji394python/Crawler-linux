import pandas as pd
def InventoryDetail2CorEvent_genRow(code, adjust_cash, adjust_stock):
    row = pd.Series(data={"商品代碼": code,
                          "現金股利調整數": adjust_cash, 
                          "股票股利調整數": adjust_stock})
#     display(row.to_frame().T)
    return row.to_frame().T
    