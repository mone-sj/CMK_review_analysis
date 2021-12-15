from numpy.lib.function_base import select
from classify import classification
import requests, json
import urllib3
import db

# 감정분석 flightbase url
# acryl_empathy_url = "https://flightbase.acryl.ai/deployment/hd96927473d6603c5fcf8c328e7761b21/"
cmk_empathy_url = "https://192.168.1.28:30001/deployment/h713a8261480609773e335448cf89d226/"

# 감정에 따른 감정스코어
score_5=['황홀함','행복','기쁨','즐거움','홀가분함','자신감']
score_4=['사랑','그리움','감동','고마움','만족','설렘','바람','놀람']
score_3=['중립','무관심']
score_2=['슬픔','미안함','부러움','안타까움','괴로움','불안','창피함','당황','지루함']
score_1=['외로움','후회','실망','두려움','싫음','미워함','짜증남','분노','억울함']


# df=TB_REVIEW_qa()
# pt파일 사용하여 분류 > anal00 table return
def cos_model_pt(df):
    print('property+empathy_analysis')

    model_id_dic=db.TB_model_id() # sub_id:model_id
    property_id_dic=db.TB_property_id() # property_name:property_id

    data = df.copy()
    data['model']=''            #columns:6
    for i in range(len(data)):
        sub_id = data.iloc[i,2]
        model_id=model_id_dic[sub_id]        
        data.iloc[i,6]=model_id

    data['CLASSIFY']=''         #columns:7
    data['EMPATHY']=''          #columns:8
    data['EMPATHY_SCORE']=''    #columns:9
    for cnt in range(len(data)):
        print('{}번째 property+empathy 분석'.format(cnt+1))
        # model_id
        model=data.iloc[cnt,6]
        review=data.iloc[cnt,5]
        
        # property_classification_pt
        property_result=classification.predict_pt(review,model)
        property_id=property_id_dic[property_result]
        data.iloc[cnt,7]=property_id

        # empathy_classification
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response_empathy=requests.post(cmk_empathy_url,json={'text':review},verify=False,timeout=180)
        result_empathy=response_empathy.json()

        output_empathy=result_empathy.get('columnchart')[0].get('output')[0]
        output_first_empathy=list(output_empathy.keys())[0]

        # empathy_score
        if output_first_empathy in score_5:
            score = 5
        elif output_first_empathy in score_4:
            score = 4
        elif output_first_empathy in score_3:
            score = 3
        elif output_first_empathy in score_2:
            score = 2
        elif output_first_empathy in score_1:
            score = 1
        data.iloc[cnt,8]=output_first_empathy
        data.iloc[cnt,9]=score
    data=data[['REVIEW_DOC_NO','PART_ID','CLASSIFY','EMPATHY','EMPATHY_SCORE']]
    return data

# df=TB_REVIEW_qa()
# url 사용하여 분류 > anal00 table return
def cos_model_api(df):
    print('property+empathy_analysis')

    model_id_dic=db.TB_model_id() # sub_id:model_id
    property_id_dic=db.TB_property_id() # property_name:property_id

    data = df.copy()
    data['model']=''            #columns:6
    for i in range(len(data)):
        sub_id = data.iloc[i,2]
        model_id=model_id_dic[sub_id]
        data.iloc[i,6]=model_id

    data['CLASSIFY']=''         #columns:7
    data['EMPATHY']=''          #columns:8
    data['EMPATHY_SCORE']=''    #columns:9
    for cnt in range(len(data)):
        print('{}번째 property+empathy 분석'.format(cnt+1))
        # model_id
        model=data.iloc[cnt,6]
        review=data.iloc[cnt,5]
        
        # property_classification_url
        property_result=classification.predict_url(review,model)
        property_id=property_id_dic[property_result]
        data.iloc[cnt,7]=property_id

        # empathy_classification
        response_empathy=requests.post(acryl_empathy_url,json={'text':review},verify=False,timeout=180)
        result_empathy=response_empathy.json()

        output_empathy=result_empathy.get('columnchart')[0].get('output')[0]
        output_first_empathy=list(output_empathy.keys())[0]

        # empathy_score
        if output_first_empathy in score_5:
            score = 5
        elif output_first_empathy in score_4:
            score = 4
        elif output_first_empathy in score_3:
            score = 3
        elif output_first_empathy in score_2:
            score = 2
        elif output_first_empathy in score_1:
            score = 1
        data.iloc[cnt,8]=output_first_empathy
        data.iloc[cnt,9]=score
    data=data[['REVIEW_DOC_NO','PART_ID','CLASSIFY','EMPATHY','EMPATHY_SCORE']]
    return data