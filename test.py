import pandas as pd
from db import *
#import emp_class
from keys.key import *
from gp import *



#today_path=db.today_path()
#now=datetime.now().strftime('%y%m%d_%H%M')

#to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#from_date = db.last_isrt_dttm()

# 분석완료된 날짜 이후 리뷰 수집
#data, isrt_dttm = db.TB_GLOWPICK_DATA(from_date,to_date)
#data=ori_df[['SITE_GUBUN',"PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]]

#print(data,isrt_dttm)

#split_review = kss_split(data)
#split_review =["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW_DOC_NO_CNT","REVIEW","LABELING"]




# # 4. anal00(property+empathy result) insert
# '''gpu 사용'''
#anal00=emp_class.cos_model_pt(split_review)

# '''api_url 사용'''
#anal00=emp_class.cos_model_url(df,model_id_dic,property_id_dic)
#print(anal00)
#db.TB_anal00_insert(anal00)





# 에러 테스트

# import time
# import os
# import traceback
# from datetime import datetime
# from etc.log import error_time
# err=''

# class TestError(Exception):
#     def __init__(self, e:str):
#         now = datetime.now()
#         current_time = time.strftime("%Y.%m.%d/%H:%M:%S", time.localtime(time.time()))
#         now = now.strftime("%Y%m%d-%H-%M")
#         self.value=e
#         with open(f"./etc/log/{now} Log.txt", "a") as f: # 경로설정
#             f.write(f"[{current_time}] - {self.value}\n")
#         print("실행됨")
    
#     def __str__(self):
#         return self.value


# def ErrorLog(error: str):
#     now = datetime.now()
#     current_time = time.strftime("%Y.%m.%d/%H:%M:%S", time.localtime(time.time()))
#     now = now.strftime("%Y%m%d-%H-%M")

#     with open(f"./etc/log/{now} Log.txt", "a") as f: # 경로설정
#         f.write(f"[{current_time}] - {error}\n")

# def test2():
#     #global err
#     #e = ''
#     # error=TestError()
#     try:
#         df=pd.read_csv('1123_test_copy.csv')
#         return df
#     except Exception as e:
#         error_content='error내용'
#         error.TestError(error_content)
#         print(e)
#     #err = err+e

# df=test2()

# def test():
#     global err
#     e = ''
#     try:
#         df=pd.read_csv('1123_test_copy.csv')
#     except Exception as e:
#         print(e)
#     err = err+e
#     return df


# try:
#     df=test2()
    
#     if err!='':
#         raise TestError(err)
#     print(df)
# except Exception:
#         err = traceback.format_exc()
#         ErrorLog(str(err))
#         print("log 저장")



# # 1. load data
# #df=db.TB_REVIEW_qa(from_date,to_date)
# df=pd.read_csv('1123_test_copy.csv')

# # 2. sub_id:model_id
# model_id_dic=db.TB_model_id()
# # 3. property_id : property_name
# property_id_dic=db.TB_property_id()

# # 4. anal00(property+empathy result) insert
# '''gpu 사용'''
# anal00=emp_class.cos_model_pt(df,model_id_dic,property_id_dic)

# '''api_url 사용'''
# #anal00=emp_class.cos_model_url(df,model_id_dic,property_id_dic)
# print(anal00)
# db.TB_anal00_insert(anal00)

# # 5. keyword/sentence
# # 5-1. ['part_sub_id','part_id'] list
# df_id=df[['part_sub_id','part_id']]
# df_id=df_id.drop_duplicates(ignore_index=True)

# id_list=[]
# for index, row in df_id.iterrows():
#     id_list.append(row.tolist())

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
anal00_df = db.anal00_G()
print(anal00_df)
key_df =db.TB_join_G(anal00_df)
print(key_df)
key_df.to_csv('220302_anal00_df.csv',encoding='utf-8-sig')
anal03=total(key_df)
#anal02=emo(not_anal_df)
anal03.to_csv('220302_GLOWPICK_anal03.csv',encoding='utf-8-sig')

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
# try:
#     select_conn=db.select_conn_cmk()
#     properties=db.TB_REVIEW_properties(select_conn,'C','P02','S10')
#     properties_bb=properties.iloc[0,0]
    
# except Exception as e:
#     print(e)
# finally :
#     select_conn.close()
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