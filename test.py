
import pandas as pd
from db import *
import emp_class
from keys.key import *
"""
# 1. load data
#df=db.TB_REVIEW_qa(from_date,to_date)
df=pd.read_csv('./etc/data/1123_test_copy.csv')

# 4. anal00(property+empathy result) insert
'''gpu 사용'''
anal00=emp_class.cos_model_pt(df)

'''api_url 사용'''
#anal00=emp_class.cos_model_url(df,model_id_dic,property_id_dic)
print(anal00)

#anal00_df = anal00()
#not_anal_df =TB_join(anal00_df)


anal03=total(df)
#anal02=emo(not_anal_df)
anal03.to_csv('anal03.csv',encoding='utf-8-sig')

######################################################################
"""