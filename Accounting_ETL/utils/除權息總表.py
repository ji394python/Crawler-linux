import pandas as pd 
df1 = pd.read_csv('doc\證券_除權除息預告表\除權息表\上市.csv',encoding='ms950').iloc[:-1,:]
df2 = pd.read_csv('doc\證券_除權除息預告表\除權息表\上櫃.csv',encoding='ms950').iloc[:-1,:]
df3 = pd.read_csv('doc\證券_除權除息預告表\除權息表\興櫃.csv',encoding='ms950').iloc[:-1,:]
# df4 = pd.read_csv('公開發行.csv',encoding='ms950').iloc[:-1,:]

df = pd.concat([df1,df2,df3])
df.to_csv('doc\證券_除權除息預告表\除權息表\除權息總表.csv',encoding='ms950',index=False)
df = pd.read_csv('doc\證券_除權除息預告表\除權息表\除權息總表.csv',encoding='ms950')


columns = ['除權息日期','公司代號','公司名稱','除權息','無償配股率','現金股利']

for col in ['股票股利-盈餘轉增資配股(元/股)',
            '股票股利-資本公積轉增資配股(元/股)',
            '現金股利-盈餘分配之股東現金股利(元/股)',
            '現金股利-法定盈餘公積、資本公積發放之現金(元/股)']:
    df[col] = df[col].apply(lambda x: float(x.replace('-','0.0')) if ((type(x) != type(2.5)) & (x=='-')) else x)
    df[col] = df[col].astype(float)
    df[col] = df[col].fillna(0.0)

df['無償配股率'] = (df['股票股利-盈餘轉增資配股(元/股)']+df['股票股利-資本公積轉增資配股(元/股)']) /10
df['現金股利'] = (df['現金股利-盈餘分配之股東現金股利(元/股)']+df['現金股利-法定盈餘公積、資本公積發放之現金(元/股)'])
df[['現金股利-除息交易日','股票股利-除權交易日']] = df[['現金股利-除息交易日','股票股利-除權交易日']].astype(str)
date = []
ignore_index = []
for i in range(len(df)):
    if df.at[i,'現金股利-除息交易日'] != 'nan':
        date.append(df.at[i,'現金股利-除息交易日'])
    else:
        if df.at[i,'股票股利-除權交易日'] != 'nan':
            date.append(df.at[i,'股票股利-除權交易日'])
        else:
            if (df.at[i,'無償配股率'] + df.at[i,'現金股利']) == 0.0:
                ignore_index.append(i)
                print('正常：除權息日期全空，且無現金/股票股利',i)
            else:
                print('錯誤：無除權息日期，且有現金/股票股利',i)
                
df.drop(ignore_index,inplace=True)
df['除權息日期'] = date
df.index = [ i for i in range(len(df))]

ignore2 = []
s = []
for i in range(len(df)):
    if (df.at[i,'現金股利'] > 0) and   (df.at[i,'無償配股率'] > 0):
        s.append('除權息')
    elif (df.at[i,'現金股利'] > 0) and   (df.at[i,'無償配股率'] == 0):
        s.append('除息')
    elif (df.at[i,'現金股利'] == 0) and   (df.at[i,'無償配股率'] > 0):
        s.append('除權')
    else:
        print('移除無償配股率、現金股利皆為0的標的',i)
        ignore2.append(i)
df.drop(ignore2,inplace=True)
df['除權息'] = s
df.index = [ i for i in range(len(df))]
df = df[columns]
df.columns = ['除權息日期','股票代號','名稱','除權息','無償配股率','現金股利']

def read_futureinfo():
    # 股票期貨
    stockfutureinfo_df = pd.read_csv("./doc/證券與期貨代碼對照表.csv", encoding="cp950", keep_default_na=False, na_values=['']).astype({"連結比例": int}).astype({"證券代號": int}).astype({"證券代號": str}) # 證券代號原先會被讀成float，故要先轉int再轉str。 ex. 2330.0(float) -> 2330(int) -> "2330"(str)
    FutureSetting = pd.read_csv("./doc/FutureSetting.csv", encoding="cp950")
    stockfutureinfo_df = stockfutureinfo_df.merge(FutureSetting.loc[0:2, ["連結比例", "手續費"]], how='left', on='連結比例')
    
    # 指數期貨
    indexfutureinfo_df = FutureSetting.loc[3:7, ['商品代碼', '連結比例', '手續費']]
    indexfutureinfo_df["證券代號"] = ""

    # 合併
    futureinfo_df = pd.concat([stockfutureinfo_df, indexfutureinfo_df])
    return futureinfo_df


# 結合期貨代碼資訊
futureinfo_df = read_futureinfo().rename(columns={"商品代碼":"期貨代碼", "證券代號":"股票代號"})
df['股票代號'] = df['股票代號'].astype(str)
temp = df.merge(futureinfo_df, how='left', on='股票代號')

temp.to_csv('doc\證券_除權除息預告表\上市和上櫃公司除權除息預告表.csv',encoding='ms950',index=False)
