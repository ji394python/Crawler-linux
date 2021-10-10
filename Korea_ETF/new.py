

import pandas as pd
df = pd.DataFrame({'組別':['A','B','A','B','A'],'成績':[10,20,30,40,50]})
grouped = df.groupby('組別')
import tqdm
for k,v in tqdm.tqdm(grouped):
    print(k,v)

v
k