from multiprocessing import process
import pandas as pd
import pymssql
import os, csv
from classify.base_classify import predict as base_classify
import time
import multiprocessing

#base_classify.base_predict()

def makeconn():
    host = '211.223.132.46'
    database = 'cosmeca'
    user = 'cosmeca '
    password = 'asc1234pw!'

    conn = pymssql.connect(
        host , user, password, database,charset='utf8'
        )
    return conn

def db_insert(data_list):

    conn = makeconn()
    cs = conn.cursor()
    
    state = ''
    try:
        # 수정
        # sql = "insert into TB_REVIEW_ANAL_01 (site_gubun,part_group_id,part_sub_id,part_id,property_id, rlt_value_01,rlt_value_02, rlt_value_03,rlt_value_04,rlt_value_05) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)  ;"
        sql = "update TB_REVIEW_ANAL_01 set site_gubun=%s,part_group_id=%s,part_sub_id=%s,part_id=%s, rlt_value_01=%s,rlt_value_02=%s, rlt_value_03=%s,rlt_value_04=%s,rlt_value_05=%s where property_id='13829495628'"
        sql = "update TB_REVIEW_ANAL_01 set site_gubun=%s,part_group_id=%s,part_sub_id=%s,part_id=%s, rlt_value_01=%s,rlt_value_02=%s, rlt_value_03=%s,rlt_value_04=%s,rlt_value_05=%s where property_id='14760725899'"
        sql = "update TB_REVIEW_ANAL_01 set site_gubun=%s,part_group_id=%s,part_sub_id=%s,part_id=%s, rlt_value_01=%s,rlt_value_02=%s, rlt_value_03=%s,rlt_value_04=%s,rlt_value_05=%s where property_id='15005024613'"
        sql = "update TB_REVIEW_ANAL_01 set site_gubun=%s,part_group_id=%s,part_sub_id=%s,part_id=%s, rlt_value_01=%s,rlt_value_02=%s, rlt_value_03=%s,rlt_value_04=%s,rlt_value_05=%s where property_id='22848335426'"
        
        cs.executemany(sql,(data_list))  #part_group_id,part_sub_id,part_id,writer,regist_date,grade,review
        state='1st'
    except pymssql.DatabaseError as e:
        state='fail'
        print(f' insertproc{e}')
    
    conn.commit()
    conn.close()
    
    return state      
                                         

if __name__ == "__main__":
    start_time = time.time()
    df = pd.read_csv('pp.csv')
    review_list = df.values.tolist()
    
    data_list=[]
    for review in review_list:
        site_gubun = review[0]
        part_group_id = review[1]          #수정
        part_sub_id = review[2]            #수정
        part_id = review[3]                #수정
        classify = review[4]                 #수정
        counting = review[5]       #수정
        pos = review[6]                  #수정
        neg = review[7]
        neu = review[8]
        all = review[9]                 #수정
        data =  (site_gubun,part_group_id,part_sub_id, classify,counting,pos,neg,neu,all)
        data_list.append(data)
        
    
    state = db_insert(data_list)
    
    end_time = (time.time() - start_time)/60
    print(' 종료 --- {0:0.2f} 분 소요'.format(end_time)) 