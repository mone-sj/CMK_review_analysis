#-*- coding: utf-8 -*-
#from numpy.lib.function_base import select
from keys.key import *
import time, traceback, db, emp_class
from datetime import datetime
from multi_process.multi import *

def naver_analysis():
    # 1. load data
    part_id_list=db.TB_CRAW_top5_pid() # 카테고리별 top 5에 대한 part_id
    df=db.TB_review_addTop5Review(part_id_list)
    
    classy_num_cores=2                         # multiprocessing의 process 개수

    if len(df)!=0:
        # 2. anal00(property+empathy result) insert
        #anal00=cos_model_pt_multi(df, classy_num_cores) # multi_process - PT파일 사용
        # anal00=cos_model_api_multi(df, classy_num_cores) # API 사용
        anal00=emp_class.cos_model_pt(df) # single _process - PT 파일 사용

        # anal00 insert
        db.TB_anal00_N_insert(anal00)
        
    # 3. anal02/anal03(keyword/keysentence analysis) insert
    # anal00의 part_id 리스트
    key_part_id_list=db.anal00_part_id_list('N')

    key_num_cores=3
    anal03=total_multi(key_part_id_list,key_num_cores)
    anal02=emo_multi(key_part_id_list,key_num_cores)

    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    # 4. anal01/04 review count insert
    db.TB_anal00_N_delete()
    db.TB_anal01_count_N()
    db.TB_anal04_count_N()

    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")

if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        naver_analysis() # 네이버 리뷰분석
        all_time = time.time() - start_time
        
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"naver_total_analy","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        now=datetime.now().strftime('%Y%m%d %H:%M:%S')
        e=f'{now}\n{err}'
        error_list.append(e)
        db.save_txt(error_list,f'{today_path}/errorList')
        #db.fail_sendEmail(e)
        #analysis()
