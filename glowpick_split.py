#-*- coding: utf-8 -*-
#from posixpath import split
import db, time, kss
import pandas as pd
from datetime import datetime

today_path=db.today_path()
now=datetime.now().strftime('%y%m%d_%H%M')

def kss_split(data):
    '''
    GLOWPICK리뷰 SPLIT 및 LABELING 동시진행 
    '''
    char_data_col=['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW','DOC_PART_NO']
    char_data_orgin_col=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","LABELING","SPLIT_NO"]
    start_time = time.time()
    char_data = pd.DataFrame(columns=char_data_col)  
    char_data_orgin = pd.DataFrame(columns=char_data_orgin_col)
    split_list = []
    split_list_origin=[]

    id_cnt = data[['PART_ID']]
    id_cnt = id_cnt.drop_duplicates()
    error_list=[]

    try:
        for idx, row in data.iterrows():

            site = data.iloc[idx,0]
            group_id = data.iloc[idx,1]
            sub_id = data.iloc[idx,2]
            part_id = data.iloc[idx,3]
            review_doc_no = data.iloc[idx,4]
            review = data.iloc[idx,5]

            split_review = kss.split_sentences(review)
            
            cnt = 0
            for k in range(len(split_review)):
                kth_review = split_review[k]
                label =total_labeling_review(kth_review)
                kth_list_origin= (site, group_id, sub_id, part_id, review_doc_no,kth_review,label,k+1)
                split_list_origin.append(kth_list_origin)
                if len(label) !=0:
                    cnt +=1 
                    kth_list= (site, group_id, sub_id, part_id, review_doc_no,kth_review,cnt)
                elif len(label)==0:
                    kth_list= (site, group_id, sub_id, part_id, review_doc_no,kth_review,0)
                split_list.append(kth_list)
        
            kth_data_origin = pd.DataFrame(split_list_origin,columns=char_data_orgin_col)
            kth_data = pd.DataFrame(split_list,columns=char_data_col)
        char_data_origin = pd.concat([char_data_orgin,kth_data_origin])
        char_data = pd.concat([char_data,kth_data])
        end_time = (time.time() - start_time)/60         
        print('split 완료시간 --- {0:0.2f} 분 소요'.format(end_time))
        err=f'{len(char_data)}리뷰_glowpick_split 오류없음'
        error_list.append(err)
    except Exception as e:
        error=e
        error_list.append(error)

    # Time check
    now=datetime.now().strftime('%y%m%d_%H%M%S')
    # 분석날짜, split_review, 제품 총 리뷰수, 스플릿된 리뷰수, 분석 리뷰수, 분석시간
    time_list=[now,"glowpick_split_review",len(id_cnt), len(data),end_time,site,len(char_data_origin),len(char_data)]
    # save
    char_data_origin.to_csv(f'{today_path}/{now}_G_split_original_result.csv', index=None)
    char_data.to_csv(f'{today_path}/{now}_G_split_result.csv', index=None)
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(error_list,f'{today_path}/errorList')
    return char_data

def total_labeling_review(split_review):
    '''
    SPLIT된 문장 LABELING(통합버전)
    '''
    total_care = {
        '발림성' : ['발림','발려','발랐','발라','바르기','부드럽','발리'],
        '지속력' : ['밀착','지속','오래','유지','무너짐','무너지','오랫'],
        '보습력' : ['보습','흡수','스며','당김','매트','수분','촉촉','뻑뻑','유분'],
        '커버력' : ['커버','표현','가려','잡티','밀착','보정','가리'],
        '거품력' : ['거품','버블','풍부','폼','세정','개운','깨끗','지워','세안'],
        '발색력' : ['발색','색상','표현','선명','색깔','혈색','컬러', '컬러감','혈기'],
        '끈적임' : ['끈쩍','끈적','산뜻','달라붙','뽀송','보송','질감','매끈','쫀존'],
        '탈모' : ['숱','주름','머리카락','탈모','빠짐','빠지'],
        '자외선' : ['자외선','차단','햇빛','타지','알러지','타는','기미','uv','UV'],
        '효과' : ['영양','물광','광도','꿀광','광채','번들','광이','밀림','탄력',
        '리프팅','개선','쿨링','시원','땀','미백','물에','백탁','태닝','톤업','블루라이트',
        '두피','비듬','윤기','찰랑','각질','환해','밝아','진정','머릿결','자극','트러블','뾰루지','시림',
        '여드름','블랙헤드','효과','번짐','워터푸르프','성능','풍성','다크닝','번지는','생기', '광택','민감','화사'],
        '향' : ['향이','향기','냄새','향도','잔향','무향','향은','은은한'],
        '기타' : ['편리','간편','편해','깔끔','편하','접착','편한','위생','떨어']
                }
    kk=[]
    for key,ivalue in total_care.items(): #전체카테고리 레이블링 구분
                part_key = key 
                for i in ivalue:
                    if i in split_review:
                        a=(part_key, i)
                        kk.append(a)
    return kk