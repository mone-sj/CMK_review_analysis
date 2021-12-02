#-*- coding: utf-8 -*-
from typing_extensions import final
import pymssql
import pandas as pd

server = 'ASC-AI.iptime.org'
database = 'cosmeca'
username = 'cosmeca'
password = 'asc1234pw!'

# DB 연결
def conn_utf8():
    try:
      conn = pymssql.connect(server, username, password, database, charset="utf8")
    except Exception as e:
        print("Error: ", e)
    return conn

def conn_cp949():
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
    except Exception as e:
        print("Error: ", e)
    return conn

# TB_REVIEW select per date (cp949)
def TB_REIVEW_qa(from_date, to_date):
    print('db_data_loading')
    try:
        conn=conn_cp949()
        cursor = conn.cursor()
        sql="exec dbo.P_MNG_CRW004 @section = 'QA', @from_date=%s, @to_date=%s,@site_gubun='N'"
        cursor.execute(sql,(from_date, to_date))
        row=cursor.fetchall()
        col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","ISRT_DATE","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_AGE","REVIEW_SEX","REVIEW_SKIN_TYPE","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
        ori_df=pd.DataFrame(row, columns=col_name)
        df=ori_df[['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW']]
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return df

# TB_REVIEW select per part_id(cp949)
def TB_review_part_id(part_id_list):
    print('db_data_loading')
    try:
        conn=conn_cp949()
        df_concat=pd.DataFrame()
        for part_id in part_id_list:
            cursor=conn.cursor()
            sql="exec dbo.P_MNG_CRW004 @section = 'QC', @part_id=%s, @site_gubun='N'"
            cursor.execute(sql,(part_id))
            row=cursor.fetchall()
            col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","ISRT_DATE","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_AGE","REVIEW_SEX","REVIEW_SKIN_TYPE","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
            ori_df=pd.DataFrame(row, columns=col_name)
            df_concat=pd.concat([df_concat,ori_df],ignore_index=True)
        df=df_concat[['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW']]
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return df

# 딕셔너리(part_sub_id : model_id) 반환 (cp949)
def TB_model_id():
    try:
        conn=conn_cp949()
        cursor=conn.cursor()
        sql='select PART_SUB_ID, MODEL_ID from TB_PART_GROUP'
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['part_sub_id','model_id']
        df=pd.DataFrame(row, columns=col_name)
        df_dict=dict(df.values.tolist())
    except Exception as e:
        print("Error : ",e)
    finally:
        conn.close()
    return df_dict

'''
# model_id : model_name dictionary 반환 (utf8)
def TB_model_name():
    try:
        conn=conn_utf8()

        #######################################################체크 필요!!
        df=pd.DataFrame()
        df_dict=dict(df.values.tolist())
    except Exception as e:
        print("Error : ",e)
    finally:
        conn.close()
    return df_dict
'''

# stopwords list 반환(utf8)
def TB_stopwords():
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql='select * from dbo.TB_UNUSE_KEYWORD'
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return row

# property_code - name dict 반환(cp949)
def TB_property_id():
    try:
        conn=conn_cp949()
        cursor=conn.cursor()
        sql="select CD_NM, SUB_CD from dbo.TB_CODE where GROUP_CD='PROPERTY_ID'"
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['cd_nm','sub_cd']
        df=pd.DataFrame(row,columns=col_name)
        df_dict=dict(df.values.tolist())
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return df_dict

#### 키워드/센텐스 결과를 위한 리뷰 select
def TB_join(sub_id, part_id):
    print('db_data_loading for keyword/sentence')
    try:
        conn=conn_cp949()
        cursor = conn.cursor()
        sql="select SITE_GUBUN, PART_GROUP_ID, PART_SUB_ID, PART_ID, REVIEW_DOC_NO, REVIEW from TB_REVIEW (nolock) where PART_SUB_ID=%s and PART_ID=%s"
        cursor.execute(sql, (sub_id,part_id))
        tb_review=cursor.fetchall()
        review_col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]
        df_review=pd.DataFrame(tb_review, columns=review_col_name)    
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()

    try:
        conn=conn_cp949()
        cursor = conn.cursor()
        sql="select REVIEW_DOC_NO, PART_ID, RLT_VALUE_03 from TB_REVIEW_ANAL_00 (nolock) where PART_ID=%s"
        cursor.execute(sql, (part_id))
        row=cursor.fetchall()
        anal00_col_name=["REVIEW_DOC_NO","PART_ID","RLT_VALUE_03"]
        df_anal00=pd.DataFrame(row, columns=anal00_col_name)
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    df=pd.merge(df_review,df_anal00,on=['REVIEW_DOC_NO','PART_ID'])
    # df_columns=site_gubun, part_group_id, part_sub_id, part_id, review_doc_no, review, rlt_value_03
    return df



'''DB insert'''

def TB_anal00_insert(df):
    try:
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            sql="exec dbo.P_MNG_ANA000 @section='SA', @review_doc_no=%s, @part_id=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s"
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("anal00_insert완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal02_insert(df):
    try:
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()

            if row[4]=='0': #키워드
                sql="exec dbo.P_MNG_ANA002 @section='SA', @site_gubun=%s, @part_group_id=%s,@part_sub_id=%s, @part_id=%s, @keyword_gubun=%s, @keyword_positive=%s,@rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
                cursor.execute(sql,tuple(row))
                print("anal02_recode inserted")
                conn.commit()

            elif row[4]=='1': #핵심문장
                sql="exec dbo.P_MNG_ANA002 @section='SA', @site_gubun=%s, @part_group_id=%s,@part_sub_id=%s, @part_id=%s, @keyword_gubun=%s, @keyword_positive=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
                cursor.execute(sql, tuple(row))
                print("anal02_recode inserted")
                conn.commit()
        print("anal02_insert_완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal03_insert(df):
    try:
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            if row[4]=='0': #키워드
                sql="exec dbo.P_MNG_ANA003 @section='SA',@site_gubun=%s,@part_group_id=%s,@part_sub_id=%s,@part_id=%s,@keyword_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
                cursor.execute(sql,tuple(row))
                print("anal03_record inserted")
                conn.commit()

            elif row[4]=='1': #핵심문장
                sql="exec dbo.P_MNG_ANA003 @section='SA',@site_gubun=%s,@part_group_id=%s,@part_sub_id=%s,@part_id=%s, @keyword_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
                cursor.execute(sql, tuple(row))
                print("anal03_record inserted")
                conn.commit()
        print("anal03_insert_완료")
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()