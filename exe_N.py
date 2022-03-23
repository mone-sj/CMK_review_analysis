#-*- coding: utf-8 -*-

def naver_analysis():
    site='N'
    # 1. 카테고리별 순위 top 5 제품에 대한 리뷰 분석을 위한 리뷰 수집
    part_id_list, crawHist_isrtDate=db.TB_CRAW_top5_pid() # 카테고리별 top 5에 대한 part_id
    df=db.TB_review_addTop5Review(part_id_list)
    
    classy_num_cores=2                         # multiprocessing의 process 개수

    # 2. anal00(property+empathy result) analysis and DB insert
    if len(df)!=0:
        csf_analy=emp_class.classify_analy(site)
        
        # 속성분류를 API 이용하여 분석하려면 how='api', 로컬에서 PT파일로 분석하려면 how='pt'로 지정
        how='pt'
        #how='api'

        start_classify=time.time()
        anal00=csf_analy.empPropertyClassify(df,how)
        anal00=anal00[['SITE_GUBUN','REVIEW_DOC_NO','PART_ID','DOC_PART_NO','CLASSIFY','EMPATHY','EMPATHY_SCORE']]

        now=datetime.now().strftime('%y%m%d_%H%M%S')
        anal00.to_csv(f'{cmnVariables.today_path}/{now}_{site}_anal00_result.csv', index=None)

        #분석날짜 - 분석모델 - 분석제품수 - 총 리뷰수 - 분석시간 - site_gubun - 스플릿된 리뷰수 - 분석 리뷰수
        time_list = [now, f"classify_{crawHist_isrtDate}기준_top5_{how}", len(part_id_list), len(df),time.time()-start_classify, site,'',len(df),'-',f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list, f'{cmnVariables.today_path}/time_check')

        # anal00 insert
        db.TB_anal00_N_insert(anal00)

    # 3. anal02/anal03(keyword/keysentence) analysis and DB insert
    key_num_cores=3
    naver_multi=multi_key(site, key_num_cores)
    
    anal03=naver_multi.total_multi()
    anal03.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal03_result.csv", index=None) #save
    
    anal02=naver_multi.emo_multi()
    anal02.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal02_result.csv", index=None) #save

    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    # 4. anal01/04 review count insert
    db.TB_anal00_N_delete()
    db.TB_anal01_count_N()
    db.TB_anal04_count_N()

    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 네이버 분석 완료")

if __name__=='__main__':
    from keys.key import *
    import time, traceback, db, emp_class
    from datetime import datetime
    from multiProc.multi_keys import *
    from cmn import cmn
    
    
    cmnVariables=cmn()
    error_list=[]
    time_list=[]

    try:
        start_time=time.time()
        naver_analysis() # 네이버 리뷰분석
        all_time = time.time() - start_time
        
        # 분석날짜, 분류, 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"naver_total_analy","-","-",all_time,"-","-","-",f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        e=f"{datetime.now().strftime('%Y%m%d %H:%M:%S')}\n{err}"
        error_list.append(e)
        db.save_txt(error_list,f'{cmnVariables.today_path}/errorList')
        #db.fail_sendEmail(e)
        #analysis()