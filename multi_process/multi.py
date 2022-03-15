from multiprocessing import Pool
import pandas as pd, numpy as np, db
from emp_class import *
from datetime import datetime
from keys.key import *

today_path=db.today_path()

###### 감정/분류 분석 multi
## multiprocessing
def multi_processing(df, func, num_cores):
    '''분석 속도개선을 위한 multiprocessing'''
    df_split = np.array_split(df, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split), ignore_index=True)
    pool.close()
    pool.join()
    return df

def cos_model_pt_multi(df, num_cores):
    ''' 감정분석/분류분석 알고리즘을 multiprocessing으로 실행'''
    check_site=df.iloc[0,0]
    anal03=multi_processing(df,cos_model_pt,num_cores)
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    anal03.to_csv(f'{today_path}/{now}_{check_site}_anal00_result.csv', index=None)
    return anal03

def cos_model_api_multi(df, num_cores):
    ''' 감정분석/분류분석 알고리즘을 multiprocessing으로 실행'''
    check_site=df.iloc[0,0]
    anal02=multi_processing(df,cos_model_api,num_cores)
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    anal02.to_csv(f'{today_path}/{now}_{check_site}_anal00_result.csv', index=None)
    return anal02


###### 키워드/센텐스 분석 multi
## multiprocessing
def multi_processing(code_list, func, num_cores):
    '''분석 속도개선을 위한 multiprocessing'''
    list_split = np.array_split(code_list, num_cores)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, list_split), ignore_index=True)
    pool.close()
    pool.join()
    return df

def total_multi(code_list, num_cores, site):
    ''' 전체리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
    anal03=multi_processing(code_list,total,num_cores)
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    anal03.to_csv(f'{today_path}/{now}_{site}_anal03_result.csv', index=None)
    return anal03

def emo_multi(code_list, num_cores, site):
    ''' 긍/부정 리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
    anal02=multi_processing(code_list,emo,num_cores)
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    anal02.to_csv(f'{today_path}/{now}_{site}_anal02_result.csv', index=None)
    return anal02