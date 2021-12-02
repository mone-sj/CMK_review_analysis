# coding: utf-8
from classify import *
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''model_id : model_name dic으로 모델 체크해야하나...'''

# 01:스킨케어, 02:클렌저, 03:선케어, 04:헤어바디, 05:베이스, 06:포인트
def predict_pt(review, model_id):
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

# 분류 url
skin_url = "https://192.168.200.83:30001/deployment/h05587bb111ca5fb98be6eee172ec1821/"
sun_url = "https://192.168.200.83:30001/deployment/hcfdfa952f0958900df115567b5b418df/"
point_url = "https://192.168.200.83:30001/deployment/h4b8735beb3c28d0d4f9b6d5f4fb4cf7f/"
hair_url = "https://192.168.200.83:30001/deployment/h1b5d910f0e4a60d2fc7dcd635d9c9dca/"
cleanser_url = "https://192.168.200.83:30001/deployment/h3133b2c7de58d295eed7dbcb132065b8/"
base_url = "https://192.168.200.83:30001/deployment/hf42f835622a15511232672cb4b7352a3/"

def predict_url(review, model_id):
    # 01:스킨케어, 02:클렌저, 03:선케어, 04:헤어바디, 05:베이스, 06:포인트
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

    response_classify=requests.post(url,json={'text':review},verify=False,timeout=180)
    result_classify=response_classify.json()
    output_classify=result_classify.get('text')[0]
    classify_value=list(output_classify.values())[0]
    return classify_value
