#-*- coding: utf-8 -*-
### 수정필요 / TEST 진행 필요
from multiprocessing import Pool
import pandas as pd, numpy as np, os, sys
sys.path.insert(1, os.path.abspath('.'))
import emp_class
print('multi_classify.1')

class multi_classify(emp_class.classify_analy):
    def __init__(self, site, num_cores):
        super().__init__(site)
        self.site=site
        self.num_cores=num_cores

    ###### 분류 분석 multi
    ## multiprocessing
    def multi_processing(self, df, func):
        '''분석 속도개선을 위한 multiprocessing'''
        df_split = np.array_split(df, self.num_cores) #part_id리스트를 process개수 만큼 자름
        pool = Pool(self.num_cores)
        df = pd.concat(pool.map(func, df_split), ignore_index=True)
        pool.close()
        pool.join()
        return df

    def multi_class(self, df):
        ''' 전체리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        anal00=self.multi_processing(df, self.empPropertyClassify)
        return anal00