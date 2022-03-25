#-*- coding: utf-8 -*-
from multiprocessing import Pool
import pandas as pd, numpy as np
from keys import key

class multi_key(key.KeywordSent):
    def __init__(self, site, num_cores):
        super().__init__(site)
        self.num_cores=num_cores

    ###### 키워드/센텐스 분석 multi
    ## multiprocessing
    def multi_processing(self, anal00_partIdList, func):
        '''분석 속도개선을 위한 multiprocessing'''
        list_split = np.array_split(anal00_partIdList, self.num_cores) #part_id리스트를 process개수 만큼 자름
        pool = Pool(self.num_cores)
        df = pd.concat(pool.map(func, list_split), ignore_index=True)
        pool.close()
        pool.join()
        return df

    def total_multi(self):
        ''' 전체리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        anal03=self.multi_processing(self.anal00_partIdList, self.total)
        return anal03

    def emo_multi(self):
        ''' 긍/부정 리뷰의 키워드/핵심문장 알고리즘을 multiprocessing으로 실행'''
        df = self.review_join
        code_list=self.anal00_partIdList
        # 글로우픽의 경우 레이블링이 되지 않은 리뷰 제외하고 emo 프로세스 실행
        if self.site=='G':
            df=df[df['DOC_PART_NO']!='0']
            # DOC_PART_NO이 '0'인 리뷰만 있는 제품(PART_ID)이 있을 수 있으므로 해당 제품을 제외(code_list 재정의)
            code_dropDupl=df.drop_duplicates(['PART_SUB_ID','PART_ID'])
            code_list=code_dropDupl[['PART_SUB_ID','PART_ID']].values.tolist()
        anal02=self.multi_processing(code_list,self.emo)
        return anal02