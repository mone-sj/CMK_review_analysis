### 수정필요
print('multi_classify.1')
"""
from multiprocessing import Pool
import pandas as pd, numpy as np, db
from emp_class import *
from datetime import datetime

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

"""