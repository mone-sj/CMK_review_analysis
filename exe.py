#-*- coding: utf-8 -*-
from numpy.lib.function_base import select
import emp_class
import db
from keys.key import *
import argparse, os, time, traceback
from datetime import datetime
import review_split
from gp import *


today_path=db.today_path()
now=datetime.now().strftime('%y%m%d_%H%M')
to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
from_date = db.last_isrt_dttm()

def naver_analysis():
    # 1. load data
    part_id_list=db.TB_CRAW_top5_pid() # 카테고리별 top 5에 대한 part_id
    df=db.TB_review_part_id(part_id_list)
    


    if len(df)!=0:
        # 2. anal00(property+empathy result) insert
        '''gpu 사용'''
        anal00=emp_class.cos_model_pt(df)
        '''api_url 사용'''
        # anal00=emp_class.cos_model_api(df)

        # anal00 insert
        db.TB_anal00_insert(anal00)

    # 3. anal02/anal03(keyword/keysentence analysis) insert
    # anal00의 part_id 리스트

    part_id_df=db.anal00_N()
    key_df=db.TB_join(part_id_df)
    anal03=total(key_df)
    anal02=emo(key_df)

    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    # 4. anal01/04 review count insert
    db.TB_anal01_count_N()
    db.TB_anal04_count_N()

    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")

def glowpick_review_analysis():
    #1. 분석완료된 날짜 이후 리뷰 수집
    data, isrt_dttm = db.TB_GLOWPICK_DATA(from_date,to_date)
    #data=ori_df[['SITE_GUBUN',"PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]]
    print(data,isrt_dttm)
    
    #2. 리뷰 split후 labeling
    split_review = kss_split(data)
    #split_review =["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","DOC_PART_NO"]
    

    #3. anal00(property+empathy result) insert
    # '''gpu 사용'''
    anal00=emp_class.cos_model_pt(split_review)
    # '''api_url 사용'''
    #anal00=emp_class.cos_model_url(df,model_id_dic,property_id_dic)
    print(anal00)
    db.TB_anal00_G_insert(anal00)

    #4. keyword/keysentence 분석 및 insert
    anal00_g = db.anal00_G()
    #anl00_g=['SITE_GUBUN','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']
    key_df=db.TB_join_G(anal00_g)
    #key_df=['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']

    anal03=total(key_df)
    anal02=emo(key_df)
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    db.TB_anal01_count_G()
    db.TB_anal04_count_G()
    
    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")


 
def test():
    df = pd.read_csv('gp_test.csv',header=None)
    df.columns =["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","ISRT_DTTM"]
    data = df[["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]]
    df_G = data[0:10]
    split_review = kss_split(df_G)
    print(split_review)

    anal00 = emp_class.cos_model_pt(split_review)
    print(anal00)

def keyword_sentence():
    anal00_g = db.anal00_G()
    anal00_n = db.anal00_N()
    #anl00_g 컬럼 : ['SITE_GUBUN','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']
    
    return('keyword_sentence_완료')



if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        #naver_analysis() #네이버 리뷰분석
        #naver_end_time=time.time()
        #all_time=naver_end_time-start_time

        #glowpick_review_analysis() #글로우픽 리뷰분석
        #keyword_sentence()#키워드키센텐스
        test()
        glow_end_time=time.time()
        all_time=glow_end_time-start_time
        
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"all_analy","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')
        db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        now=datetime.now().strftime('%Y%m%d %H:%M')
        e=f'{now}\n{err}'
        error_list.append(e)
        db.save_txt(error_list,f'{today_path}/errorList')
        db.fail_sendEmail(e)
        #analysis()