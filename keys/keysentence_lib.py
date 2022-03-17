# -*- coding:utf-8 -*-
from krwordrank.sentence import keysentence
import kss, pandas as pd
from krwordrank.sentence import make_vocab_score, MaxScoreTokenizer

# 3-1 키센텐스 추출하기
def keysentence_list(texts, vocab_score, tokenizer):
    keys_list = []
    for i in range(len(texts)):
        sent_list = []  # 각 리뷰의 문장리스트 돌때마다 초기화
        for sent in kss.split_sentences(texts[i]):  #문장단위로 쪼개기
            sent_list.append(sent)

        if(len(sent_list) > 1):       # 이외의 리스트 범위가 2문장 이상 - 키 센텐스 구하기
            penalty = lambda x: 0 if 25 <= len(x) <= 128 else 1
            key_sents = keysentence(
            vocab_score, sent_list, tokenizer.tokenize,
            penalty=penalty,
            diversity=0.5,
            topk=1  # 리뷰 하나당 1개의 키워드 비중이 높은 한문장만 추출
            )
            keys_list.append(key_sents)
        else:   # 리뷰리스트 범위가 비어있을경우(리뷰 내용이 없는경우)
            continue

#3-2 추출된 리뷰 중복값 제거하기   
    new_keys_list = []
    for key_review in keys_list:
        if key_review not in new_keys_list:
            new_keys_list.append(key_review)  # 한문장만 뽑아냈던 키센텐스가 유일할 경우에만 뉴키센텐스 리스트로
    print(new_keys_list)
    return new_keys_list
# 키센텐스 추출하기(합)
def keys_list(all_keyword,stopword,review):
    ''' 핵심문장 분석 (키워드리스트,불용어,리뷰리스트)
    '''
    try:
        vocab_score=make_vocab_score(all_keyword,stopword,scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        tokenizer = MaxScoreTokenizer(vocab_score)

        new_keys_list = []
        keys_list = []  # 리뷰당 키센텐스모음 리스트
        for i in range(len(review)):
            sent_list = []
            for sent in kss.split_sentences(review[i]):  # 문장단위로 쪼개기
                sent_list.append(sent)
            if(len(sent_list) > 1):       # 이외의 리스트 범위가 2문장 이상 - 키 센텐스 구하기
                penalty = lambda x: 0 if 25 <= len(x) <= 128 else 1
                key_sents = keysentence(
                vocab_score, sent_list, tokenizer.tokenize,
                penalty=penalty,
                diversity=0.5,
                topk=1  # 리뷰 하나당 1개의 키워드 비중이 높은 한문장만 추출
                )
                keys_list.append(key_sents)
            else:   # 리뷰 내용이 없는경우
                continue
                
    #3-2 추출된 리뷰 중복값 제거
        for key_review in keys_list:
            if key_review not in new_keys_list:
                new_keys_list.append(key_review)
        new_keys_list=sum(new_keys_list,[])
        e='에러없음'
    except Exception as e:
        print(e)
    return new_keys_list, e

def keys_df_error(site,part_group_id,part_sub_id,part_id):
    '''센텐스_df 오류'''
    all_keysentece_result_df=pd.DataFrame({
                'SITE_GUBUN':site,
                'PART_GROUP_ID':part_group_id,
                'PART_SUB_ID':part_sub_id,
                'PART_ID':part_id,
                'KEYWORD_GUBUN':'1',
                'RLT_VALUE_01' : '오류',
                'RLT_VALUE_02' : '',
                'RLT_VALUE_03' : '',
                'RLT_VALUE_04' : '',
                'RLT_VALUE_05' : ''
            },index=[0])
    return all_keysentece_result_df

def total_sent(site, part_group_id,part_sub_id,part_id,sent_list):
    total_sentence=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id,
            'KEYWORD_GUBUN':'1',
            'RLT_VALUE_01' : sent_list[0],
            'RLT_VALUE_02' : sent_list[1],
            'RLT_VALUE_03' : sent_list[2],
            'RLT_VALUE_04' : sent_list[3],
            'RLT_VALUE_05' : sent_list[4]
        },index=[0])
    return total_sentence

def emo_pos_sent(site,part_group_id,part_sub_id,part_id,sent_list):
    '''긍정리뷰의 핵심문장 (분석코드, 사이트, 문장 리스트)'''
    pos_sentence=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id,
            'KEYWORD_GUBUN':'1',
            'KEYWORD_POSITIVE':'P',
            'RLT_VALUE_01' : sent_list[0],
            'RLT_VALUE_02' : sent_list[1],
            'RLT_VALUE_03' : sent_list[2],
            'RLT_VALUE_04' : sent_list[3],
            'RLT_VALUE_05' : sent_list[4]
        },index=[0])
    return pos_sentence

def pos_sent_error(site,part_group_id,part_sub_id,part_id):
    ''' 긍정리뷰 센텐스 오류'''
    pos_keysentece_result_df=pd.DataFrame({
                'SITE_GUBUN':site,
                'PART_GROUP_ID':part_group_id,
                'PART_SUB_ID':part_sub_id,
                'PART_ID':part_id,
                'KEYWORD_GUBUN':'1',
                'KEYWORD_POSITIVE':'P',
                'RLT_VALUE_01' : '오류',
                'RLT_VALUE_02' : '',
                'RLT_VALUE_03' : '',
                'RLT_VALUE_04' : '',
                'RLT_VALUE_05' : ''
            },index=[0])
    return pos_keysentece_result_df

def emo_neg_sent(site,part_group_id,part_sub_id,part_id,sent_list):
    '''부정리뷰의 핵심문장 (분석코드, 사이트, 문장 리스트)'''
    neg_sentence=pd.DataFrame({
            'SITE_GUBUN':site,
            'PART_GROUP_ID':part_group_id,
            'PART_SUB_ID':part_sub_id,
            'PART_ID':part_id,
            'KEYWORD_GUBUN':'1',
            'KEYWORD_POSITIVE':'N',
            'RLT_VALUE_01' : sent_list[0],
            'RLT_VALUE_02' : sent_list[1],
            'RLT_VALUE_03' : sent_list[2],
            'RLT_VALUE_04' : sent_list[3],
            'RLT_VALUE_05' : sent_list[4]
        },index=[0])
    return neg_sentence

def neg_sent_error(site,part_group_id,part_sub_id,part_id):
    ''' 부정리뷰 센텐스 오류'''
    neg_keysentece_result_df=pd.DataFrame({
                'SITE_GUBUN':site,
                'PART_GROUP_ID':part_group_id,
                'PART_SUB_ID':part_sub_id,
                'PART_ID':part_id,
                'KEYWORD_GUBUN':'1',
                'KEYWORD_POSITIVE':'N',
                'RLT_VALUE_01' : '오류',
                'RLT_VALUE_02' : '',
                'RLT_VALUE_03' : '',
                'RLT_VALUE_04' : '',
                'RLT_VALUE_05' : ''
            },index=[0])
    return neg_keysentece_result_df