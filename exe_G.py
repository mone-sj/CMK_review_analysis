#-*- coding: utf-8 -*-
#from numpy.lib.function_base import select
import db, time, traceback, glowpick_split, emp_class
from keys.key import *
from multi_process.multi import *
from datetime import datetime

def glowpick_review_analysis():
    #1. 분석완료된 날짜 이후 리뷰 수집
    to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    from_date = db.last_isrt_dttm()
    print(f'to_date: {to_date} / from_date: {from_date}')
    data, isrt_dttm = db.TB_GLOWPICK_DATA(from_date,to_date)
    #data=[['SITE_GUBUN',"PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]]
    
    #2. 리뷰 split후 labeling
    split_review = glowpick_split.kss_split(data)
    #split_review =["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","DOC_PART_NO"]
    
    classy_num_cores=2 # multiprocessing의 process 개수

    #3. anal00(property+empathy result) insert
    # '''gpu 사용'''
    if len(split_review)!=0:
        #anal00=cos_model_pt_multi(split_review, classy_num_cores) # PT파일 사용
        #anal00=emp_class.cos_model_pt(split_review) # 감정api_pt사용
        anal00=emp_class.cos_model_pt(split_review) # single_process
        with open("./etc/last_isrt_dttm_G.txt","a",encoding='utf8') as f:
                f.write(f'\n{to_date}\t{isrt_dttm}\t분석완료')
        db.TB_anal00_G_insert(anal00)

    #4. keyword/keysentence 분석 및 insert
    part_id_list=db.anal00_part_id_list('G')

    key_num_cores=3
    anal03=total_multi(part_id_list,key_num_cores)
    anal02=emo_multi(part_id_list,key_num_cores)
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    db.TB_anal00_G_delete()
    db.TB_anal01_count_G()
    db.TB_anal04_count_G()
    
    glowpick_finish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{glowpick_finish_time} 글로우픽 완료')
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    # time_list = [datetime.now().strftime('%y%m%d'), "glowpick_total_analy", '-', '-', all_time]
    # db.time_txt(time_list, f'{today_path}/time_check')

    

def naver_key():
    ''' 네이버 키워드/센텐스 분석 '''
    print('네이버 키워드/센텐스 분석 실행')
    key_part_id_list=db.anal00_part_id_list('N')

    key_num_cores=3
    naver_key_start=time.time()
    anal03=total_multi(key_part_id_list,key_num_cores)
    anal02=emo_multi(key_part_id_list,key_num_cores)
    naver_key_finTime = time.time()-naver_key_start
    time_list = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "naver_keys_all", '-', '-', naver_key_finTime]
    db.time_txt(time_list, f'{today_path}/time_check')
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    naver_finish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{naver_finish_time} 네이버 키워드/센텐스 완료')


 
if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        glowpick_review_analysis() #글로우픽 리뷰분석
        all_time = time.time() - start_time
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"glowpick_all","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')

        naver_key_start=time.time()
        naver_key()
        naver_key_all=time.time()-naver_key_start
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"naver_key_all","-","-",naver_key_all]
        db.time_txt(time_list,f'{today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        now=datetime.now().strftime('%Y%m%d %H:%M%S')
        e=f'{now}\n{err}'
        error_list.append(e)
        db.save_txt(error_list,f'{today_path}/errorList')
        #db.fail_sendEmail(e)
        #analysis()
