# -*- coding:utf-8 -*-
from krwordrank.sentence import keysentence
import kss

# 3-1 키센텐스 추출하기
def keysentence_list(texts, vocab_score, tokenizer):
    keys_list = []  # 리뷰당 키센텐스모음 리스트
    for i in range(len(texts)):  # 리뷰 갯수만큼 돌리기
        sent_list = []  # 각 리뷰의 문장리스트 돌때마다 초기화
        for sent in kss.split_sentences(texts[i]):  # 각 리뷰들을 하나씩 꺼내 문장단위로 쪼개기
            sent_list.append(sent)

        if(len(sent_list) > 1):       # 이외의 리스트 범위가 2문장 이상 - 키 센텐스 구하기
            penalty = lambda x: 0 if 25 <= len(x) <= 128 else 1
            key_sents = keysentence(
            vocab_score, sent_list, tokenizer.tokenize,
            penalty=penalty,
            diversity=0.5,
            topk=1  # 리뷰 하나당 1개의 키워드 비중이 높은 한문장만 추출하겠다
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
