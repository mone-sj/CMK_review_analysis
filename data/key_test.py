# -*- coding:utf-8 -*-
import os, io, time, kss
from kss import split_sentences
from krwordrank.sentence import keysentence, summarize_with_sentences, make_vocab_score, MaxScoreTokenizer
from krwordrank.hangle import normalize
from numpy.core.arrayprint import SubArrayFormat
from numpy.lib.npyio import save
import pandas as pd
from keyword_lib import *
from krwordrank.word import *
from keysentence_lib import *
#import io
#import kss #kss 문장분리기
#import time  # 셀내용의 진행시간을 보기위해
#from krwordrank.word import KRWordRank
#from krwordrank.word import summarize_with_keywords

path='./data/'
file_list=os.listdir(path)

start=time.time()

## stopwords 작성
stopwords_all=stopwords('stopwords_cos_ver3.0.txt')
stopwords_sub=stopwords('stopwords_cos_ver5.0.txt')

for i in file_list:
    fname=path+i
    
    # 전체리뷰 df로 받기 (주제별로 분류하기 위해서)
    f = pd.read_csv(fname, names=[
                    '0', '리뷰', '주제', '감정', '감정분석시간', '감정점수'], encoding='utf-8', header=0)


    #전체리뷰 키워드
    try:
        all_texts=all_get_texts(fname)    #리스트로 받음
        all_keyword=keyword(all_texts, stopwords_all) #딕셔너리 {단어-점수}
        save_txt(all_keyword,'./test_result/keyword_all_'+i+'.txt') #매개변수, {단어-점수}딕셔너리, save_point(저장위치)
        print('=====\n{} 전체리뷰키워드 저장'.format(i))
    except Exception as e:
        print(e)
        print("전체리뷰 키워드 오류")
        pass
    
    
    #전체리뷰 키센텐스
    try:
        vocab_score = make_vocab_score(all_keyword, stopwords_all, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        print('vocab_score 완료')
        tokenizer = MaxScoreTokenizer(vocab_score)
        print('tokenizer 완료')
        keysentence_list_all=keysentence_list(all_texts,vocab_score,tokenizer)
        print('keysentence_list_all 완료')
        #저장
        print(type(keysentence_list_all))
        save_keys_txt(keysentence_list_all, './test_result/keys_all_'+i+'.txt')
        print('=====\n{} 전체리뷰키센텐스 저장'.format(i))
    except Exception as e:
        print(e)
        print("{} 전체리뷰 키센텐스 오류".format(i))
        pass


    #긍정리뷰 키워드
    try:
        all_pos_texts=pos_get_texts(fname)
        all_pos_keyword=keyword(all_pos_texts, stopwords_all) #딕셔너리
        save_txt(all_pos_keyword,'./test_result/keyword_all_pos_'+i+'.txt') #매개변수, {단어-점수}딕셔너리, save_point(저장위치)
        print('=====\n{}긍정리뷰키워드 저장'.format(i))
    except Exception as e:
        print(e)
        print("{} 긍정리뷰 키워드 오류".format(i))
        pass

    #긍정리뷰 키센텐스
    try:
        vocab_score = make_vocab_score(all_pos_keyword, stopwords_all, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        tokenizer = MaxScoreTokenizer(vocab_score)
        keysentence_list_all=keysentence_list(all_pos_texts,vocab_score,tokenizer)
        #저장
        save_keys_txt(keysentence_list_all, './test_result/keys_all_pos_'+i+'.txt')
    except Exception as e:
        print(e)
        print("{} 긍정리뷰 키센텐스".format(i))
        pass

    #부정리뷰 키워드
    try:
        all_nega_texts=nega_get_texts(fname)
        all_nega_keyword=keyword(all_nega_texts,stopwords_all)#딕셔너리
        save_txt(all_nega_keyword,'./test_result/keyword_all_nega_'+i+'.txt')
        print('=====\n{}부정리뷰키워드 저장'.format(i))
    except Exception as e:
        print(e)
        print("{} 부정리뷰 키워드".format(i))
        pass

    #부정리뷰 키센텐스
    try:
        vocab_score = make_vocab_score(all_nega_keyword, stopwords_all, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
        tokenizer = MaxScoreTokenizer(vocab_score)
        keysentence_list_all=keysentence_list(all_nega_texts,vocab_score,tokenizer)
        #저장
        save_keys_txt(keysentence_list_all, './test_result/keys_all_nega_'+i+'.txt')
    except Exception as e:
        print(e)
        print("{} 부정리뷰 키센텐스".format(i))
        pass

    #CMK_catID=None
    CMK_catID=51356

    #주제별
    #생활용품인지 화장품인지 판단
    if CMK_catID is not None:
        properties_list=properties(CMK_catID)
    else:
        properties_list=living_properties()

    # 주제별 키워드
    for property in properties_list:
        # 데이터가 적으면 에러날수 있음
        # ValueError: ('The graph should consist of at least two nodes\n', 'The node size of inserted graph is 0')
        try:
            classify_review=f[f['주제']==property]
            review=classify_review[['리뷰']]
            review_list=sum(review.values.tolist(), []) #이중리스트에서 빼내기, 1차원 리스트

            #키워드
            sub_keyword=keyword(review_list, stopwords_sub) #딕셔너리
            save_txt(sub_keyword, './test_result/keyword_'+property+'_'+i+'.txt')
            print('=====\n{}_{} _키워드 저장'.format(i,property))
        except Exception as e:
            print(e)
            print('{}_{}주제별 키워드 에러'.format(i, property))
            pass

        #키센텐스
        try:
            vocab_score = make_vocab_score(sub_keyword, stopwords_all, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
            tokenizer = MaxScoreTokenizer(vocab_score)
            keysentence_list_all=keysentence_list(review_list,vocab_score,tokenizer)
            #저장
            save_keys_txt(keysentence_list_all, './test_result/keys_{}_{}.txt'.format(property, i))
        except Exception as e:
            print(e)
            print('{}_{}주제별 키센텐스 에러'.format(i, property))
            pass
