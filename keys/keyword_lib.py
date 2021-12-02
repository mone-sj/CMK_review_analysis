# -*- coding:utf-8 -*-
from krwordrank.word import *


# 키워드 top 300 단어 확인
def keywords_common_top300(keywords):
    key_num = 300
    keyword_list = []
    for word, r in sorted(keywords.items(), key=lambda x: -x[1])[:key_num]:    
    #    print('%s:\t%.4f' % (word, r))
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


#리뷰 키워드 추출
def keyword(texts, stopwords):
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
