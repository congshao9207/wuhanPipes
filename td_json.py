#jason解析
import pandas as pd
import json as simplejson
from_df=pd.read_excel('C:/data/td_risk.xlsx')
index_name=from_df.index.name
row_list=[]
for index,col in from_df.iterrows():
    row_str = dict(col).get('item_detail','{}')
    row_dict=simplejson.loads(row_str)
    row_dict[index_name]=str(index)
    row_list.append(row_dict)
df=pd.DataFrame(row_list)
df.columns
new_lst = []
for _, row in df.iterrows():
    new_dct = dict()
    new_dct['platform_count'] = row.platform_count
    new_dct['platform_detail'] = row.platform_detail
    new_dct['platform_detail_dimension'] = row.platform_detail_dimension
    new_dct['type'] = row.type
    for i in range(len(row.platform_detail)):
        new_dct[row.platform_detail[i].split(':')[0]] = row.platform_detail[i].split(':')[1]
    new_lst.append(new_dct)
    
new_df = pd.DataFrame(new_lst)