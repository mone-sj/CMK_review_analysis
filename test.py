import pandas as pd
import db
import emp_class
from keys.key import *

# 1. load data
#df=db.TB_REVIEW_qa(from_date,to_date)
df=pd.read_csv('1123_test_copy.csv')

# 2. sub_id:model_id
model_id_dic=db.TB_model_id()
# 3. property_id : property_name
property_id_dic=db.TB_property_id()

# 4. anal00(property+empathy result) insert
'''gpu 사용'''
anal00=emp_class.cos_model_pt(df,model_id_dic,property_id_dic)

'''api_url 사용'''
#anal00=emp_class.cos_model_url(df,model_id_dic,property_id_dic)
print(anal00)
db.TB_anal00_insert(anal00)

# 5. keyword/sentence
# 5-1. ['part_sub_id','part_id'] list
df_id=df[['part_sub_id','part_id']]
df_id=df_id.drop_duplicates(ignore_index=True)

id_list=[]
for index, row in df_id.iterrows():
    id_list.append(row.tolist())

# 5-2. 

# 5-3.

'''
df2=pd.read_csv('1123_test.csv')
df2_id=df2[['part_sub_id','part_id']]
df2_id=df2_id.drop_duplicates(ignore_index=True)

id_list=[]
for index, row in df2_id.iterrows():
    id_list.append(row.tolist())

print(id_list)
'''
#anal03=total(id_list)
#anal02=emo(id_list)


######################################################################
'''
211201_exe.py 내용
# data load
# test
#from_date='20211116'
#to_date='202111116'

#try:
#    select_conn=db.select_conn_cmk()
    # 날짜별
#    df = db.TB_REIVEW_qa(select_conn,from_date,to_date)
#    print(df)
#except Exception as e:
#    select_conn.close()

data = pd.read_csv('1123_test_copy.csv')
data = data.dropna(axis=0)

print(data)

# 속성분류 모델찾기

#감성,분류모델

#data['model']=''
properties_list=[]
'''
try:
    select_conn=db.select_conn_cmk()
    properties=db.TB_REVIEW_properties(select_conn,'C','P02','S10')
    properties_bb=properties.iloc[0,0]
    
except Exception as e:
    print(e)
finally :
    select_conn.close()
'''



for i in range(len(data)):
    site_gubun = data.iloc[i,0]
    group_id = data.iloc[i,1]
    sub_id = data.iloc[i,2]
    try:
        select_conn = db.select_conn_cmk()
        properties = db.TB_REVIEW_properties(select_conn,site_gubun,group_id,sub_id)
        properties_bb=properties.iloc[0,0]
        properties_list.append(properties_bb)
    except Exception as e:
        print(e)
    finally :
        select_conn.close()

data['model']=properties_list
print(data)


df = data
classify = empathy.gpu(df)
print(classify)




#anal00넣기





##anal03



##anal02감성




'''