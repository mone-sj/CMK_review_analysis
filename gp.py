from posixpath import split
import db
import os
import pandas as pd
import time
from datetime import datetime
import pymssql
import kss

today_path=db.today_path()
now=datetime.now().strftime('%y%m%d_%H%M')

def kss_split(data):
    '''
    GLOWPICK리뷰 SPLIT 및 LABELING 동시진행 
    '''
    start_time = time.time()
    char_data = pd.DataFrame(columns=['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW','DOC_PART_NO'])  
    char_data_orgin = pd.DataFrame(columns=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","LABELING","DOC_PART_NO"])
    split_list = []
    split_list_origin=[]

    id_cnt = data[['PART_ID']]
    id_cnt = id_cnt.drop_duplicates()

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
                #label=group_labeling_review(group_id,kth_review)
                k +=1
                kth_list_origin= (site, group_id, sub_id, part_id, review_doc_no,kth_review,label,k)        
                split_list_origin.append(kth_list_origin)
                if len(label) !=0:
                    cnt +=1 
                    kth_list= (site, group_id, sub_id, part_id, review_doc_no,kth_review,cnt)
                    split_list.append(kth_list)
        
            kth_data_origin = pd.DataFrame(split_list_origin,columns=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW","LABELING","DOC_PART_NO"])
            kth_data = pd.DataFrame(split_list,columns=['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW','DOC_PART_NO'])   
        char_data_origin = pd.concat([char_data_orgin,kth_data_origin])
        char_data = pd.concat([char_data,kth_data])
        end_time = (time.time() - start_time)/60         
        print('split 완료시간 --- {0:0.2f} 분 소요'.format(end_time))
    except Exception as e:
        error=e
        error_list =[{error}]

    # Time check
    now=datetime.now().strftime('%y%m%d_%H%M')
    # 분석날짜, split_review, 제품 총 리뷰수, 스플릿된 리뷰수, 분석 리뷰수, 분석시간
    time_list=[now,"glowpick_split_review",len(id_cnt), len(data),end_time,site,len(char_data_origin),len(char_data)]
    
    # save
    char_data_origin.to_csv(f'{today_path}/{now}_GLOWPICK_split_original_result.csv', index=None)
    char_data.to_csv(f'{today_path}/{now}_GLOWPICK_split_result.csv', index=None)
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(error_list,f'{today_path}/GLOWPICK_errorList')

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

def group_labeling_review(group,split_review):
    try:
        if group == 'P02':
            # P02	남성류(male_care)
            categorys = {'발림성' : ['발림','발려','발랐','발라','바르기','부드럽','발리'],
                        '지속력' : ['밀착','지속','오래','유지','무너짐','무너지','오랫'],
                        '보습력' : ['보습','촉촉','흡수','당기','당김','뽀송','보송','스며','매트','수분','유분'],
                        '커버력' : ['커버','표현','가려','잡티','밀착','보정','가리'],
                        '향' : ['향이','냄새','잔향','향도','향은','향기','청량','은은','무향'],
                        '효과' : ['트러블','피부','주름','산뜻','찐득','순하','기름','진정','끈적','밀림','번들','매끄','매끈','쿨링','시원'],
                        '기타' : ['편리','간편','편해','깔끔','편하']
                        }
            category_name = 'male_care'
        elif group == 'P04':
            # P04	마스크/팩(mask_care)
            categorys = {'발림성' : ['발림','발려','발랐','발라','바르기','부드럽','발리'],
                        '수분감' : ['촉촉','수분'],
                        '보습력' : ['보습','흡수','당기','당김','뽀송','보송','스며','매트','유분'],
                        '커버력' : ['커버','표현','가려','잡티','밀착','보정','가리'],
                        '향' : ['향이','냄새','잔향','향도','향은','향기','청량','은은','무향'],
                        '효과' : ['영양','물광','광도','꿀광','광채','진정','윤기','탄력','뽀루지','트러블','피부','주름','산뜻','찐득','순하','기름','진정','끈적','밀림','번들','매끄','매끈','쿨링','시원','탱탱','쫀쫀'],
                        '기타' : ['밀착','접착','얇게','얇았','두껍','부착','두꺼','얇고']
                        }
            category_name = 'mask_care'
        elif group == 'P05' or group=='P06':
            # P05/P06	메이크업(립)/메이크업(아이)(point_makeup)
            categorys = {'발림성' : ['발림','발려','발랐','발라','발리','바르기'],
                        '보습력' : ['촉촉','매트','흡수','보습','수분'],
                        '향' : ['향이','향기','냄새','향도'],
                        '지속력' : ['밀착','지속','오래','유지','무너짐','무너지','오랫'],
                        '발색력' : ['발색','색상','표현','선명','색깔'],
                        '효과' : ['번짐','워터푸르프','효과','성능','풍성','다크닝','번지는']
                        }
            category_name = 'point_makeup'
        elif group == 'P07':
            # P07	메이크업(페이스)(Base_makeup)
            categorys = {'발림성': ['발림','발려','발랐','발라','발리','바르기'],
                        '보습력' : ['보습','수분','촉촉','흡수'],
                        '커버력' : ['커버','표현','가려','잡티','밀착','보정','가리'],
                        '지속력' : ['지속','오래','유지','무너짐'],
                        '향' : ['향이','향도','향기','냄새'],
                        '효과' : ['꿀광','광이','광채','광도','물광','톤업','밝아','환해','자외선','차단','쫀존','다크닝','트러블','뾰루지']
                        }
            category_name = 'Base_makeup'
        elif group == 'P08' or group=='P12':
            # P08/P12	바디케어/헤어케어(hair_body)
            categorys = {'발림성' : ['발림','발려','발랐','발라','보송','바르기'],
                        '수분감' : ['촉촉','수분'],
                        '보습력' : ['보습','흡수','스며','당김'],
                        '거품세정력' : ['거품','버블','풍부','폼','세정','개운'],
                        '탈모' : ['숱','머리카락','탈모','빠짐','빠지'],
                        '효과' : ['영양','두피','비듬','윤기','찰랑','각질','환해','피부','밝아','진정','머릿결','자극','트러블'],
                        '향' : ['향이','향기','냄새','향도','잔향']
                        }
            category_name = 'hair_body'
        elif group == 'P09':
            # P09	선케어(sun_care)
            categorys = {'발림성': ['발림','발려','발랐','발라','발리','바르기'],
                        '효과' : ['톤업톤업','밝아','환해','주름','블루라이트','워터푸르프','시림','밀림','쿨링','시원','땀','물에','백탁','태닝','밝아','환해','주름','블루라이트','워터푸르프','시림','밀림','쿨링','시원','땀','물에'],
                        '보습력' : ['보습','수분','촉촉'],
                        '자외선' : ['자외선','차단','햇빛','타지','알러지','타는','기미','uv','UV'],
                        '향' : ['향이','향도','향기','냄새','무향'],
                        '끈적임' : ['끈적','산뜻','달라붙','보송']
                        }
            category_name = 'sun_care'
        elif group == 'P10':
            # P10	스킨케어(skin_care)
            categorys = {'발림성': ['발림','발려','발랐','발라','질감','발리','밝아','부드럽'],
                        '보습력' : ['보습','흡수','스며'],
                        '수분감' : ['수분','촉촉'],
                        '끈적임' : ['끈적','산뜻','달라붙'],
                        '향' : ['향이','향도','향기','냄새'],
                        '효과' : ['효과','물광','광도','광채','번들','광이','윤기','매끈','톤업','밝아','환해','영양','주름','탄력','리프팅','개선','영양','미백','자극','트러블','여드름','진정']
                        }
            category_name = 'skin_care'
        elif group == 'P11':
            # P11	클렌징(cleanser)
            categorys = {'거품력' : ['거품','버블','풍부','폼'], 
                    '보습력' : ['보습','수분','촉촉'],
                    '세정력' : ['세정','개운','깨끗','지워','세안'],
                    '향' : ['향이','향기','냄새','향도','무향'],
                    '효과' : ['뾰루지','각질','시림','당김','트러블','여드름','블랙헤드','진정','자극','효과']
                    }
            category_name = 'cleanser'
    except:
        pass

    kk=[]
    for key,ivalue in categorys.items(): #그룹별 카테고리 레이블링 구분
                part_key = key 
                for i in ivalue:
                    if i in split_review:
                        a=(part_key, i)
                        kk.append(a)
    return






# db.TB_anal_00_insert(anal00)
# print('--------------------------------------DB insert--------------------------')
# with open("./etc/last_isrt_dttm.txt","a",encoding='utf8') as f:
#     f.write(f'\n{to_date}\t{isrt_dttm}\t분석완료')



# def split_after_in(file_name):
#     start_time = time.time()
#     path='C:/glowpic_sentences/glowpic_sentences_before_'+today+'/' # 고치기

#     data=pd.read_csv(path+file_name)
#     file_name_after = file_name.replace('before','after')
#     data = data.values.tolist()

#     data_list=[]

#     for data_low in data:
#         site = data_low[1]
#         group = data_low[2]
#         sub = data_low[3]
#         part_id = data_low[4]
#         doc_no = data_low[5]
#         isrt_date = data_low[6]
#         nicname = data_low[7]
#         grade = data_low[9]
#         age = data_low[10]
#         gender = data_low[11]
#         types = data_low[12]
#         review = data_low[13]
#         review_cnt = len(review)
        

        
#         review_split_total = kss.split_sentences(review)
#         for review_split_low in review_split_total:
#             review_split = review_split_low.replace("'","")
#             review_split_cnt = len(review_split)
#             kd = (site, group, sub, part_id, doc_no, isrt_date, nicname, grade, age, gender, types, review, review_cnt,review_split, review_split_cnt)
#             data_list.append(kd)
#     end_time = (time.time() - start_time)/60    
#     print('split 완료시간 --- {0:0.2f} 분 소요'.format(end_time))

#     return data_list