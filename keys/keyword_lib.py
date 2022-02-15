# -*- coding:utf-8 -*-
from krwordrank.word import *
import pandas as pd

# 키워드 top 300 단어 확인
def keywords_common_top300(keywords):
    key_num = 300
    keyword_list = []
    for word, r in sorted(keywords.items(), key=lambda x: -x[1])[:key_num]:
        keyword_list.append((word, r))
    return keyword_list

# 키워드 top 300에서 stopwords 제거한 단어 리스트
def remove_stopwords_keywords(keyword_list, stopwords):
    selected_keywords = []
    for word, r in keyword_list:
        if word in stopwords:
            continue
        selected_keywords.append([word, r])
    #딕셔너리로 변환
    selected_keywords_dic=dict(selected_keywords)
    return selected_keywords_dic

#리뷰 키워드 추출 원본
def keyword_ori(texts, stopwords):
    # 단어랭크 확인
    wordrank_extractor = KRWordRank(  # 단어랭크 추출
    min_count=5,   # 단어의 최소 출현 빈도수 (그래프 생성 시)
    max_length=10,  # 단어의 최대 길이
    verbose=True
    )
    beta = 0.85    
    max_iter = 10
    keywords, rank, graph = wordrank_extractor.extract(texts, beta, max_iter)  # 단어랭크-빈도수를 기반으로 키워드를 추출

    # 키워드 300단어 확인
    keyword_list=keywords_common_top300(keywords) # 리스트로 리턴
    #키워드 300개에서 stopwords 제거
    keywords_sw_apply= remove_stopwords_keywords(keyword_list, stopwords) # 딕셔너리
    return keywords_sw_apply

# 키워드 추출
def keyword(freq,texts,stopwords):
    wordrank_extractor = KRWordRank(  
    min_count=freq,   # 단어의 최소 출현 빈도수 (그래프 생성 시)
    max_length=10, 
    verbose=True
    )
    beta = 0.85    
    max_iter = 10
    keywords, rank, graph = wordrank_extractor.extract(texts, beta, max_iter)
    keyword_list=keywords_common_top300(keywords)
    
    keywords_sw_apply= remove_stopwords_keywords(keyword_list, stopwords)
    return keywords_sw_apply

# min_count에 따른 리뷰 키워드 추출
def keyword_minCount(texts, stopwords):
    min_count_list=[4,2,1]
    for count in min_count_list:
        try:
            keywords_sw_apply=keyword(count,texts,stopwords)
            
            if list(keywords_sw_apply.keys())[0]!='':
                error=f'오류없음 / min_count: {count}'
                return keywords_sw_apply, error
            else:
                raise Exception('공백')
        except Exception as e:
            error=e
            print(f'오류 min_count: {count}')
            keywords_sw_apply={}
            print(error)
            pass
    return keywords_sw_apply,error # 딕셔너리로 리턴됨

def key_df_error(site, part_group_id,part_sub_id,part_id):
    '''키워드_df 오류'''
    all_keyword_result_df=pd.DataFrame({
        'SITE_GUBUN':site,
        'PART_GROUP_ID':part_group_id,
        'PART_SUB_ID':part_sub_id,
        'PART_ID':part_id,
        'KEYWORD_GUBUN':'0',
        'RLT_VALUE_01' : '오류',
        'RLT_VALUE_02' : '',
        'RLT_VALUE_03' : '',
        'RLT_VALUE_04' : '',
        'RLT_VALUE_05' : '',
        'RLT_VALUE_06' : '',
        'RLT_VALUE_07' : '',
        'RLT_VALUE_08' : '',
        'RLT_VALUE_09' : '',
        'RLT_VALUE_10' : ''
    },index=[0])
    return all_keyword_result_df

def noValueToBlank(list1):
    '''값이 없는 곳은 빈칸처리'''
    new_list=[]
    len_list=len(list1)
    if len_list < 6:
        for x in range(5-len_list):
            list1.append('')
        return list1
    else:
        for y in range(5):
            new_list.append(list1[y])
        return new_list

        
def total_key_df_result(site, part_group_id,part_sub_id,part_id,list1):
    '''전체 키워드 결과'''
    all_keyword_result_df=pd.DataFrame({
        'SITE_GUBUN':site,
        'PART_GROUP_ID':part_group_id,
        'PART_SUB_ID':part_sub_id,
        'PART_ID':part_id,
        'KEYWORD_GUBUN':'0',
        'RLT_VALUE_01' : list1[0],
        'RLT_VALUE_02' : list1[1],
        'RLT_VALUE_03' : list1[2],
        'RLT_VALUE_04' : list1[3],
        'RLT_VALUE_05' : list1[4] ,
        'RLT_VALUE_06' : list1[5],
        'RLT_VALUE_07' : list1[6],
        'RLT_VALUE_08' : list1[7],
        'RLT_VALUE_09' : list1[8],
        'RLT_VALUE_10' : list1[9]
    },index=[0])
    return all_keyword_result_df

def pos_key_error(site, part_group_id,part_sub_id,part_id):
    ''' 긍정 리뷰의 키워드 오류'''
    pos_keyword_result_df=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id,      
            'KEYWORD_GUBUN':'0',
            'KEYWORD_POSITIVE':'P',
            'RLT_VALUE_01' : '오류',
            'RLT_VALUE_02' : '',
            'RLT_VALUE_03' : '',
            'RLT_VALUE_04' : '',
            'RLT_VALUE_05' : '',
            'RLT_VALUE_06' : '',
            'RLT_VALUE_07' : '',
            'RLT_VALUE_08' : '',
            'RLT_VALUE_09' : '',
            'RLT_VALUE_10' : '',
        },index=[0])
    return pos_keyword_result_df

def pos_key_result(site, part_group_id,part_sub_id,part_id,list1):
    '''긍정리뷰의 키워드 결과'''
    pos_keyword_result_df=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id,
            'KEYWORD_GUBUN':'0',
            'KEYWORD_POSITIVE':'P',
            'RLT_VALUE_01' : list1[0],
            'RLT_VALUE_02' : list1[1],
            'RLT_VALUE_03' : list1[2],
            'RLT_VALUE_04' : list1[3],
            'RLT_VALUE_05' : list1[4],
            'RLT_VALUE_06' : list1[5],
            'RLT_VALUE_07' : list1[6],
            'RLT_VALUE_08' : list1[7],
            'RLT_VALUE_09' : list1[8],
            'RLT_VALUE_10' : list1[9]
        },index=[0])
    return pos_keyword_result_df

def neg_key_error(site, part_group_id,part_sub_id,part_id):
    ''' 부정 리뷰의 키워드 오류'''
    neg_keyword_result_df=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id, 
            'KEYWORD_GUBUN':'0',
            'KEYWORD_POSITIVE':'N',
            'RLT_VALUE_01' : '오류',
            'RLT_VALUE_02' : '',
            'RLT_VALUE_03' : '',
            'RLT_VALUE_04' : '',
            'RLT_VALUE_05' : '',
            'RLT_VALUE_06' : '',
            'RLT_VALUE_07' : '',
            'RLT_VALUE_08' : '',
            'RLT_VALUE_09' : '',
            'RLT_VALUE_10' : '',
        },index=[0])
    return neg_keyword_result_df

def neg_key_result(site, part_group_id,part_sub_id,part_id,list1):
    '''부정리뷰의 키워드 결과'''
    neg_keyword_result_df=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id, 
            'KEYWORD_GUBUN':'0',
            'KEYWORD_POSITIVE':'N',
            'RLT_VALUE_01' : list1[0],
            'RLT_VALUE_02' : list1[1],
            'RLT_VALUE_03' : list1[2],
            'RLT_VALUE_04' : list1[3],
            'RLT_VALUE_05' : list1[4],
            'RLT_VALUE_06' : list1[5],
            'RLT_VALUE_07' : list1[6],
            'RLT_VALUE_08' : list1[7],
            'RLT_VALUE_09' : list1[8],
            'RLT_VALUE_10' : list1[9]
        },index=[0])
    return neg_keyword_result_df