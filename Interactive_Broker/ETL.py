import numpy as np
import pandas as pd
import sys
import seaborn
import time 
import matplotlib.pyplot as plt 
import matplotlib.patches as patches
'''
<-> list_useful 範例：
    x = [i for i in range(10)]
    ob = list_userful(x)
    ob

<2> dataframe_useful 範例：
    x = []
'''
class listUseful():
    def __init__(self,list) -> list:
        self.list = list
    def unique(self):
        d = dict()
        for i in self.list:
            d[i] = d.get(i,0) + 1
        key = list(d.keys())
        value = list(d.values())
        self.dataframe = pd.DataFrame({'種類':key,'數量':value}).sort_values('數量',ascending=False)
        print(self.dataframe)
    def __repr__(self) -> str:
        return(self.__class__.__name__)

class dataframeUseful():
    def __init__(self,data:pd.DataFrame):
        self.data = data
        self.reduceMemory()
        self.dict_na = {}
    def CheckNA(self):
        len_Nums,NA_Nums,ratio_Nums = [],[],[]
        for i in self.data.columns.tolist():
            na = self.data[i].isna().sum()
            le = len(self.data[i])
            NA_Nums.append(na)
            len_Nums.append(le)
            ratio_Nums.append(na/le)
        self.na_ratio = ratio_Nums
        na_col_20,na_col_50,na_col_80,na_col_100 = [],[],[],[]
        for i in range(len(self.na_ratio)):
            x = self.na_ratio[i]
            col =self.data.columns.tolist()[i]
            if x < 0.2:
                na_col_20.append(col)
            elif ( (x>=0.2) & (x<0.5)):
                na_col_50.append(col)
            elif ( (x>=0.5) & (x<0.8) ):
                na_col_80.append(col)
            else:
                na_col_100.append(col)
        self.dict_na['<20%'] = na_col_20
        self.dict_na['<50%'] = na_col_50
        self.dict_na['<80%'] = na_col_80
        self.dict_na['<100%'] = na_col_100
        self.dict_na['RawData'] = ratio_Nums
    def CheckCorr(self):
        matrix = self.data.corr()
        for col in range(len(matrix)):
            printf = []
            for row in range(len(matrix)):
                if matrix.iloc[row,col] >=0.7:
                    printf.append(matrix.index.tolist()[row])
            f" (高)-【{matrix.columns.tolist()[col]}】：{printf}"
    def reduceMemory(self):
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        start_mem = self.data.memory_usage().sum() / 1024**2
        for col in self.data.columns:
            col_type = self.data[col].dtypes
            if col_type in numerics:
                c_min = self.data[col].min()
                c_max = self.data[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        self.data[col] = self.data[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        self.data[col] = self.data[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        self.data[col] = self.data[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        self.data[col] = self.data[col].astype(np.int64)
                else:
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        self.data[col] = self.data[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        self.data[col] = self.data[col].astype(np.float32)
                    else:
                        self.data[col] = self.data[col].astype(np.float64)
        end_mem = self.data.memory_usage().sum() / 1024**2
        #print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        #print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))

def record_time(object)->str:
    start = time.perf_counter_ns()
    object
    end = time.perf_counter_ns()
    return f'{(end-start)/10**9}'
