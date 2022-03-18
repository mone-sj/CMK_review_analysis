#-*- coding: utf-8 -*-
from multiprocessing import Pool
import pandas as pd, numpy as np
from keys import key

class multi_key(key.KeywordSent):
    def __init__(self, site, num_cores):
        super().__init__(site)
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
        anal03=self.multi_processing(self.total)
        return anal03

    def emo_multi(self):
        ''' 긍/부정 리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        anal02=self.multi_processing(self.emo)
        return anal02