#-*- coding: utf-8 -*-
print('multi_keys.1')
from multiprocessing import Pool
print('multi_keys.2')
import pandas as pd, numpy as np #, db, cmn
print('multi_keys.3')
#from datetime import datetime
print('multi_keys.4')
import os, sys
sys.path.insert(1, os.path.abspath('.'))
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
#from keys.key import KeywordSent
from keys import key
print('multi_keys.5')

class multi_key(key.KeywordSent):
    def __init__(self, site, num_cores):
        super().__init__(site)
        #self.today_path=db.today_path()
        self.site=site
        self.num_cores=num_cores

    ###### 키워드/센텐스 분석 multi
    ## multiprocessing
    def multi_processing(self, func):
        '''분석 속도개선을 위한 multiprocessing'''
        print('multi_keys.multi_processing.6')
        list_split = np.array_split(self.anal00_partIdList, self.num_cores) #part_id리스트를 process개수 만큼 자름
        pool = Pool(self.num_cores)
        df = pd.concat(pool.map(func, list_split), ignore_index=True)
        pool.close()
        pool.join()
        print('multi_keys.multi_processing.7')
        return df

    def total_multi(self):
        ''' 전체리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        print('multi_keys.total_multi.8')
        anal03=self.multi_processing(self.total)
        print('multi_keys.total_multi.9')
        return anal03

    #def emo_multi(self, code_list):
    def emo_multi(self):
        ''' 긍/부정 리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        print('multi_keys.emo_multi.10')
        anal02=self.multi_processing(self.emo)
        print('multi_keys.emo_multi.11')
        return anal02