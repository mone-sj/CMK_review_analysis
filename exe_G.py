#-*- coding: utf-8 -*-

def glowpick_review_analysis():
    site='G'
    #1. 분석완료된 날짜 이후 리뷰 수집
    to_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    from_date = db.last_isrt_dttm()
    print(f'to_date: {to_date} / from_date: {from_date}')
    data, isrt_dttm = db.TB_GLOWPICK_DATA(from_date,to_date)
    print(f'마지막분석일로부터 추가된 리뷰수:{len(data)}')
    
    #2. 리뷰 split후 labeling
    #split_review =["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","DOC_PART_NO"]
    split_review = glowpick_split.kss_split(data)
    print(f'스플릿된 리뷰개수: {len(split_review)}')
    
    classy_num_cores=2 # multiprocessing의 process 개수

    #3. anal00(property+empathy result) analysis and DB insert
    if len(split_review)!=0:
        csf_analy=emp_class.classify_analy(site)
              
        # 속성분류를 API 이용하여 분석하려면 how='api', 로컬에서 PT파일로 분석하려면 how='pt'로 지정
        how='pt'
        #how='api'

        start_classify=time.time()
        anal00=csf_analy.empPropertyClassify(split_review,how)
        with open("./etc/last_isrt_dttm_G.txt","a",encoding='utf8') as f:
            f.write(f'\n{to_date}\t{isrt_dttm}\t분석완료')
        
        now=datetime.now().strftime('%y%m%d_%H%M%S')
        anal00.to_csv(f'{cmnVariables.today_path}/{now}_{site}_anal00_result.csv', index=None)
        
        #분석날짜 - 분석모델 - 분석제품수 - 총 리뷰수 - 분석시간 - site_gubun - 스플릿된 리뷰수 - 분석 리뷰수 - 실행os/hostname
        time_list = [now, f"classify_{how}", '-', len(data),time.time()-start_classify, site,len(split_review),'-',f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list, f'{cmnVariables.today_path}/time_check')

        # anal00 insert
        db.TB_anal00_G_insert(anal00)
    
    # 4. keyword/keysentence 분석 및 insert
    # 분석 시간 단축을 위해 multiprocessing 이용
    key_num_cores=3     # process 개수 변경 가능(cpu 성능에 따라 변경)
    glowpick_multi=multi_key(site, key_num_cores)

    anal03=glowpick_multi.total_multi()
    anal03.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal03_result.csv", index=None) #save
    
    anal02=glowpick_multi.emo_multi()
    anal02.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal02_result.csv", index=None) #save
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    db.TB_anal00_G_delete()
    db.TB_anal01_count_G()
    db.TB_anal04_count_G()
    
    glowpick_finish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{glowpick_finish_time} 글로우픽 완료')

def naver_key():
    ''' 네이버 키워드/센텐스 분석 '''
    print('네이버 키워드/센텐스 분석 실행')
    site='N'

    # multiprocessing을 이용한 분석
    key_num_cores=3
    naver_multi=multi_key(site, key_num_cores)

    naver_key_start=time.time()
    anal03=naver_multi.total_multi()
    anal03.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal03_result.csv", index=None) #save
    
    anal02=naver_multi.emo_multi()
    anal02.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal02_result.csv", index=None) #save
    
    naver_key_finTime = time.time()-naver_key_start
    time_list = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "naver_keys", '-', '-', naver_key_finTime,site,"-","-",f"{cmnVariables.osName}/{cmnVariables.hostName}"]
    db.time_txt(time_list, f'{cmnVariables.today_path}/time_check')
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    naver_finish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{naver_finish_time} 네이버 키워드/센텐스 완료')
 
if __name__=='__main__':
    from multiProc.multi_keys import *
    from datetime import datetime
    import db, time, traceback, glowpick_split, emp_class
    from cmn import cmn
    
    cmnVariables=cmn()
    error_list=[]
    time_list=[]
    
    try:
        # 1. 글로우픽 리뷰 분석
        start_time=time.time()
        print('exe_G.glowpick_review_analysis()시작')
        glowpick_review_analysis()
        print('exe_G.glowpick_review_analysis()끝')
        
        all_time = time.time() - start_time
        # 분석날짜, 분류, 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"glowpick_all","-","-",all_time,"-","-","-",f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')

        # 2. 네이버 키워드/센텐스 분석
        naver_key_start=time.time()
        naver_key()
        naver_key_all=time.time()-naver_key_start
        time_list=[datetime.now().strftime('%y%m%d'),"naver_key_all","-","-",naver_key_all,"-","-","-",f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')

        db.second_DB()
        time_list=[datetime.now().strftime('%y%m%d'),"2차DB_insert","-","-","-","-","-","-",f"{cmnVariables.osName}/{cmnVariables.hostName}"]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        e=f"{datetime.now().strftime('%Y%m%d %H:%M%S')}\n{err}"
        error_list.append(e)
        db.save_txt(error_list,f'{cmnVariables.today_path}/errorList')
        #db.fail_sendEmail(e)
