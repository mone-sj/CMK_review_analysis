#-*- coding: utf-8 -*-

def glowpick_key():
    site='G'
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
    time_list = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "naver_keys", '-', '-', naver_key_finTime]
    db.time_txt(time_list, f'{cmnVariables.today_path}/time_check')
    
    db.TB_anal03_insert(anal03)
    db.TB_anal02_insert(anal02)

    naver_finish_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{naver_finish_time} 네이버 키워드/센텐스 완료')


if __name__=='__main__':
    from multiProc.multi_keys import *
    from datetime import datetime
    import db, time, traceback, glowpick_split
    from cmn import cmn

    cmnVariables=cmn()
    error_list=[]
    time_list=[]

    try:
        # 1. 글로우픽 리뷰 분석
        start_time=time.time()
        print('글로우픽 키워드/센텐스 시작')
        glowpick_key()
        print('글로우픽 키워드/센텐스 끝')
        
        all_time = time.time() - start_time
        # 분석날짜, 분류, 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"glowpick_key_all","-","-",all_time]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')

        # 2. 네이버 키워드/센텐스 분석
        naver_key_start=time.time()
        naver_key()
        naver_key_all=time.time()-naver_key_start
        time_list=[datetime.now().strftime('%y%m%d'),"naver_key_all","-","-",naver_key_all]
        db.time_txt(time_list,f'{cmnVariables.today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'keys_anal.py error\n{err}')
        e=f"{datetime.now().strftime('%Y%m%d %H:%M%S')}\n{err}"
        error_list.append(e)
        db.save_txt(error_list,f'{cmnVariables.today_path}/errorList')
        #db.fail_sendEmail(e)
