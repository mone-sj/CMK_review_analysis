#-*- coding: utf-8 -*-
from numpy.lib.function_base import select
import pandas as pd
#import emp_class
import db
from keys.key import *
import argparse

# recieve part_id_list to analyze
p = argparse.ArgumentParser()
p.add_argument('--part_id', type=list)
args = p.parse_args()
part_id_list=args.part_id

# 1. load data
#df=db.TB_REVIEW_qa(from_date,to_date) # insert date 별 review select
#df=pd.read_csv('./data/1123_test_copy.csv')
part_id_list_ex=['7469286149','27790718522']
#part_id_list_ex=['9879239685','27790718522']
df=db.TB_review_part_id(part_id_list_ex)
print(df)
print('총 {}개 상품/총 리뷰 {}건 분석'.format(len(part_id_list_ex),len(df)))


# 2. anal00(property+empathy result) insert
'''gpu 사용'''
# anal00=emp_class.cos_model_pt(df)
'''api_url 사용'''
# anal00=emp_class.cos_model_api(df)
# print(anal00)
# anal00.to_csv('./etc/result/anal00_result.csv')
# db.TB_anal00_insert(anal00)


# 3. anal03/anal02 -> keyword/sentence
# 3-1. create ['part_sub_id','part_id'] list
df_id=df[['PART_SUB_ID','PART_ID']]
df_id=df_id.drop_duplicates(ignore_index=True)


id_list=[]
for index, row in df_id.iterrows(): # dataframe -> list로 전환
    id_list.append(row.tolist())

# 3-2. anal03(total_keyword/sentence) insert 
# df columns : part_sub_id / part_id / review
# anal03=total(df)
# print(anal03)
# db.TB_anal03_insert(anal03)

#3-3. anal02(emo_keyword/sentence) insert
anal02=emo(id_list)
print(anal02)
db.TB_anal02_insert(anal02)

# 4. anal01, anal04 insert & update count
db.TB_anal01_count()
db.TB_anal04_count()