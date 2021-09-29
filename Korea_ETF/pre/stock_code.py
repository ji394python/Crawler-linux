#code.json是由官網觀察的到，複製回應進此py檔進行解析，尚未找出直接post的方法

import json
import pandas as pd
d = json.load(open('code.json','r+',encoding='utf-8'))
isu_cd,kor_shrt_isu_nm,tgt_indx_nm = [],[],[]
for row in d['block1']:
    isu_cd.append(row['isu_cd'])
    kor_shrt_isu_nm.append(row['kor_shrt_isu_nm'])
    tgt_indx_nm.append(row['tgt_indx_nm'])

df = pd.DataFrame({'isu_cd':isu_cd,'kor_shrt_isu_nm':kor_shrt_isu_nm,'tgt_indx_nm':tgt_indx_nm})
df.to_csv('etf_code.csv',encoding='utf-8-sig',index=False)