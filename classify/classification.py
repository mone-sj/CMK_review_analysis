#-*- coding: utf-8 -*-
from classify import *
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 01:스킨케어, 02:클렌저, 03:선케어, 04:헤어바디, 05:베이스, 06:포인트
def predict_pt(review, model_id):
    '''
    모델파일을 이용하여 주제 분류 반환
    '''
    if model_id=='M001':
        result=skin_predict(review)
    if model_id=='M002':
        result=cleanser_predict(review)
    if model_id=='M003':
        result=suncare_predict(review)
    if model_id=='M004':
        result=hairbody_predict(review)
    if model_id=='M005':
        result=base_predict(review)
    if model_id=='M006':
        result=point_predict(review)
    return result

# 충북 분류 url
# skin_url = "https://192.168.200.83:30001/deployment/h05587bb111ca5fb98be6eee172ec1821/"
# sun_url = "https://192.168.200.83:30001/deployment/hcfdfa952f0958900df115567b5b418df/"
# point_url = "https://192.168.200.83:30001/deployment/h4b8735beb3c28d0d4f9b6d5f4fb4cf7f/"
# hair_url = "https://192.168.200.83:30001/deployment/h1b5d910f0e4a60d2fc7dcd635d9c9dca/"
# cleanser_url = "https://192.168.200.83:30001/deployment/h3133b2c7de58d295eed7dbcb132065b8/"
# base_url = "https://192.168.200.83:30001/deployment/hf42f835622a15511232672cb4b7352a3/"

# 코스메카서버 분류 url- 외부에서 요청할때 사용
# skin_url = "https://192.168.1.28:30001/deployment/h4b1a4e67341a14d6c9cef6deb3e8a5ec/"
# sun_url = "https://192.168.1.28:30001/deployment/h419d070ccf2f505f019e388d290f75b6/"
# point_url = "https://192.168.1.28:30001/deployment/h2a85986c200dc8db67579b91040ebf43/"
# hair_url = "https://192.168.1.28:30001/deployment/h1b133c638c3c8a0eb31fd3647b5a54a9/"
# cleanser_url = "https://192.168.1.28:30001/deployment/h3e20d11da519bfbd59dbbe1dd48e0ceb/"
# base_url = "https://192.168.1.28:30001/deployment/h254e1c45a3ee030f495982ec8e88eeb7/"

# 코스메카서버 분류 url- 서버 내부에서 요청할때 사용
skin_url = "https://localhost/deployment/h4b1a4e67341a14d6c9cef6deb3e8a5ec/"
sun_url = "https://localhost/deployment/h419d070ccf2f505f019e388d290f75b6/"
point_url = "https://localhost/deployment/h2a85986c200dc8db67579b91040ebf43/"
hair_url = "https://localhost/deployment/h1b133c638c3c8a0eb31fd3647b5a54a9/"
cleanser_url = "https://localhost/deployment/h3e20d11da519bfbd59dbbe1dd48e0ceb/"
base_url = "https://localhost/deployment/h254e1c45a3ee030f495982ec8e88eeb7/"

def predict_url(review, model_id):
    '''
    Flask API 서버를 통해 주제 분류 반환
    '''
    if model_id=='M001':
        url=skin_url
    if model_id=='M002':
        url=cleanser_url
    if model_id=='M003':
        url=sun_url
    if model_id=='M004':
        url=hair_url
    if model_id=='M005':
        url=base_url
    if model_id=='M006':
        url=point_url
    
    try:     
        response_classify=requests.post(url,json={'text':review},verify=False,timeout=180)
    except:
        time.sleep(2)
        response_classify=requests.post(url,json={'text':review},verify=False,timeout=180)

    result_classify=response_classify.json()
    output_classify=result_classify.get('text')[0]
    classify_value=list(output_classify.values())[0]
    return classify_value
