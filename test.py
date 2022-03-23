from multiProc.multi_keys import *
from datetime import datetime
import db, time, traceback, glowpick_split
from cmn import cmn
from keys import key

cmnVariables=cmn()
site='G'
keys=key.KeywordSent(site)
error_list=[]
time_list=[]

code_list=keys.anal00_partIdList
anal02=keys.emo(code_list)
anal02.to_csv(f"{cmnVariables.today_path}/{datetime.now().strftime('%y%m%d_%H%M%S')}_{site}_anal02_result.csv", index=None) #save


# start=time.time()
# anal00_g = db.anal00_G()
# df_glowpick=db.TB_join_G(anal00_g)
# print(len(df_glowpick))
# print(f'걸린시간: {time.time()-start}')

#from keys.key import *
#from multiProc.multi_keys import *

# site='N'
# key_num_cores=3
# test_multi=multi_key(site,key_num_cores)
# anal03=test_multi.total_multi()
# print(anal03)


"""
#import emp_class, 
#from multi_process.multi_classify import *
import db
from keys.key import *
import time, traceback
from datetime import datetime
import glowpick_split

# 네이버 테스트
today_path=db.today_path()

def naver():
# 1. load data
    # part_id_list=db.TB_CRAW_top5_pid() # 카테고리별 top 5에 대한 part_id
    # start_join=time.time()
    # df=db.TB_review_addTop5Review(part_id_list)
    # end_join=time.time()
    # print(f'조인하는데 걸리는 시간: {end_join-start_join}') # 약 200초~300초 사이 / 20220315기준 오만건
    
    df=pd.read_csv('./etc/data/1123_test.csv')
    classy_num_cores=2 # multiprocess의 process 개수

    if len(df)!=0:
        # 2. anal00(property+empathy result) insert
        # 시간 테스트
        single_start=time.time()
        anal00_single=cos_model_pt(df)
        single_end=time.time()
        print(anal00_single)

        multi_start=time.time()
        anal00_multi=cos_model_pt_multi(df, num_cores) # PT파일 사용
        multi_end=time.time()
        print(anal00_multi)
        print(f'분석개수: {len(df)}')
        print(f'single_time: {single_end-single_start}초')
        print(f'multi_time: {multi_end-multi_start}초')
                
        # anal00=emp_class.cos_model_api_multi(df, num_cores) # API 사용

        # anal00 insert
        #db.TB_anal00_N_insert(anal00)
        #db.TB_anal00_N_delete()
    
    # 3. anal02/anal03(keyword/keysentence analysis) insert
    # anal00의 part_id 리스트

    # part_id_df=db.anal00_N()
    # key_df=db.TB_join(part_id_df)
    # anal03=total(key_df)
    # anal02=emo(key_df)


    key_part_id_list=db.anal00_part_id_list('N')
    key_num_cores=3
    
    # key_start=time.time()
    # anal03=total_multi(key_part_id_list,key_num_cores)
    # print(anal03)
    # print(f'total_key 시간:{time.time()-key_start}초')
    
    key_start_emo=time.time()
    anal02=emo_multi(key_part_id_list,key_num_cores)
    print(anal02)
    print(f'emo_key 시간:{time.time()-key_start_emo}초')

    #db.TB_anal03_insert(anal03)
    #db.TB_anal02_insert(anal02)

    # 4. anal01/04 review count insert
    #db.TB_anal01_count_N()
    #db.TB_anal04_count_N()

    

    finish_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{finish_time} 분석 완료")


if __name__=='__main__':
    error_list=[]
    time_list=[]
    today_path=db.today_path()      # 백업을 위한 폴더 생성 

    try:
        start_time=time.time()
        
        naver() # 네이버 리뷰분석
                      
        end_time = time.time()
        all_time = end_time - start_time
        
        # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
        time_list=[datetime.now().strftime('%y%m%d'),"naver_test","-","-",all_time]
        db.time_txt(time_list,f'{today_path}/time_check')
        #db.success_sendEmail()

    except Exception:
        err=traceback.format_exc()
        print(f'1번째 error\n{err}')
        # now=datetime.now().strftime('%Y%m%d %H:%M:%S')
        # e=f'{now}\n{err}'
        # error_list.append(e)
        # db.save_txt(error_list,f'{today_path}/errorList')
"""