# -*- coding:utf-8 -*-
from krwordrank.sentence import make_vocab_score, MaxScoreTokenizer
from numpy.core.numeric import NaN
from numpy.lib.npyio import save
import pandas as pd
import numpy as np
from keys.keyword_lib import *
from krwordrank.word import *
from keys.keysentence_lib import *
import db
from datetime import datetime
import time

today_path=db.today_path()

time_list=[]
error_list=[]

def total(df):
    '''
    전체 키워드/핵심문장 추출 
    '''
    anal03_col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","KEYWORD_GUBUN","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal03=pd.DataFrame(columns=anal03_col_name)

    df_id=df[['PART_SUB_ID','PART_ID']]
    df_id=df_id.drop_duplicates(ignore_index=True)

    id_list=[]
    for index, row in df_id.iterrows():
        id_list.append(row.tolist())
    
    total_time_start=time.time()
    review_count=0
    #stopwords
    stopword=db.TB_stopwords()

    for i in id_list: # id_list=[[sub_id,part_id],[sub_id,part_id]]
        sub_id=i[0]
        part_id=i[1]

        df_per_part_id=df[(df['PART_SUB_ID']==sub_id) & (df['PART_ID']==part_id)]
        review_count+=len(df_per_part_id)

        

        df_per_part_id['REVIEW'] = df_per_part_id['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        review_content=df_per_part_id['REVIEW'].tolist()

        site=df_per_part_id.iloc[0,0]
        part_group_id=df_per_part_id.iloc[0,1]
        part_sub_id=df_per_part_id.iloc[0,2]
        part_id=df_per_part_id.iloc[0,3]

        # 전체 키워드
        # 리뷰 5개 이하면 키워드 분석은 하지 않고, 리뷰를 최신순으로 핵심문장으로 출력
        if 0<len(df_per_part_id)<6:
            list5 = df_per_part_id.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
            list5 = list5['REVIEW'].values.tolist()
            
            list_review=[]
            for index in range(5):
                try:
                    list_review.append(list5[index])
                except:
                    list_review.append('')
            
            total_sentence=total_sent(site, part_group_id,part_sub_id,part_id,list_review)
                
            # analysis result add (anal03에 추가)
            data_anal03=pd.concat([data_anal03,total_sentence],ignore_index=True)
        
        # 리뷰가 6개 이상이면, 키워드/핵심문장 분석
        elif len(df_per_part_id)>5:
            try: # 전체 키워드
                all_keyword,error=keyword_minCount(review_content, stopword)
                if '오류없음' not in error:                           # 오류 발생
                    all_keyword_result_df=key_df_error(site, part_group_id,part_sub_id,part_id)
                    error_list.append(f'{part_sub_id}_{part_id} / site: {site} 전체키워드1\t{error}')
                else:
                    All_keywordList=list(all_keyword.keys())
                    All_keywordGradeList=list(all_keyword.values())
                    All_keywordGradeList = (np.round(All_keywordGradeList,2)).tolist() # 소수점 둘째자리까지 반올림

                    # List(빈칸처리)
                    key_list=noValueToBlank(All_keywordList)
                    grade_list=noValueToBlank(All_keywordGradeList)
                    result_list=key_list+grade_list
                    all_keyword_result_df=total_key_df_result(site, part_group_id,part_sub_id,part_id,site,result_list)

                    print(f"part_id: {part_sub_id}_{part_id} / site_no: {site} 전체리뷰 키워드 완료\n총 리뷰수:{len(df_per_part_id)}")

            except Exception as e:
                error_list.append(f"{part_sub_id}_{part_id} / site: {site} 전체 키워드 오류2\t{e}")
                print(f"{e}\n{part_sub_id}_{part_id} / site: {site} 전체리뷰 키워드 오류")
                all_keyword_result_df=key_df_error(site, part_group_id,part_sub_id,part_id)
                all_keyword={}
                pass
        
            #전체핵심문장
            try:
                if not all_keyword: # keyword 가 없을때, 리뷰의 최신순으로 핵심문장 출력
                    list5 = df_per_part_id.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                    list5 = list5['REVIEW'].values.tolist()
                    
                    list_review=[]
                    for index in range(5):
                        try:
                            list_review.append(list5[index])
                        except:
                            list_review.append('')
                    
                    total_sentence=total_sent(site, part_group_id,part_sub_id,part_id,list_review)

                else:
                    print("전체 핵심문장 분석 시작")
                    keysentence_list_all, error=keys_list(all_keyword,stopword,review_content)
                    error_list.append(f"{part_sub_id}_{part_id} / site: {site} 전체 핵심문장 오류3\t{error}")
                    
                    if len(keysentence_list_all)==0:                        # 핵심문장이 출력되지 않으면 리뷰 최신순으로 출력
                        list5 = df_per_part_id.sort_values(by=['REVIEW_DOC_NO'],axis=0, ascending=False)
                        list5 = list5['REVIEW'].values.tolist()
                        
                        list_review=[]
                        for index in range(5):
                            try:
                                list_review.append(list5[index])
                            except:
                                list_review.append('')
                        
                        total_sentence=total_sent(site, part_group_id,part_sub_id,part_id,list_review)
                    else:
                        keys_list_fin=noValueToBlank(keysentence_list_all)
                        all_keysentece_result_df=total_sent(site, part_group_id,part_sub_id,part_id, keys_list_fin)

                    print(f"code: {part_sub_id}_{part_id} / site_no: {site} 전체리뷰 핵심문장 완료\n총 리뷰수: {len(df_per_part_id)}")
            
            except Exception as e:
                print(e)
                error_list.append(f"{part_sub_id}_{part_id} / site: {site} 전체 핵심문장 오류4\t{e}")
                print("{}_{} 전체리뷰 키센텐스 오류".format(sub_id, part_id))
                all_keysentece_result_df=keys_df_error(site, part_group_id,part_sub_id,part_id)
                pass
        
            data_anal03 = pd.concat([data_anal03,all_keyword_result_df,all_keysentece_result_df],ignore_index=True)
            del all_keyword_result_df
            del all_keysentece_result_df

    # Time check
    now=datetime.now().strftime('%y%m%d_%H%M')
    total_time_end=time.time()
    total_time=total_time_end-total_time_start
    # 분석날짜, 분류(total/emo), 분석제품수, 총 리뷰수, 분석시간
    time_list=[now,"total_key",len(id_list),review_count,total_time]
    
    # save
    db.time_txt(time_list,f'{today_path}/time_check')
    db.save_txt(error_list,f'{today_path}/errorList')
    data_anal03.to_csv(f'{today_path}/{now}_anal03_result.csv', index=None)
        
    return data_anal03

def emo(df):
    '''
    긍정/부정리뷰의 키워드/핵심문장 추출
    '''
    col_name2=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","KEYWORD_GUBUN","KEYWORD_POSITIVE","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal02=pd.DataFrame(columns=col_name2)

    df_id=df[['PART_SUB_ID','PART_ID']]
    df_id=df_id.drop_duplicates(ignore_index=True)

    id_list=[]
    for index, row in df_id.iterrows():
        id_list.append(row.tolist())
    
    total_time_start=time.time()
    review_count=0
    # stopwords
    stopword=db.TB_stopwords()

    for i in id_list: # id_list=[[sub_id,part_id],[sub_id,part_id]]
        sub_id=i[0]
        part_id=i[1]
        
        # review+anal00 join data
        # df_columns=site_gubun, part_group_id, part_sub_id, part_id, review_doc_no, review, rlt_value_03
        # df=db.TB_join(sub_id,part_id)

        df_per_part_id=df[(df['PART_SUB_ID']==sub_id) & (df['PART_ID']==part_id)]
        review_count+=len(df_per_part_id)

        df_per_part_id['REVIEW'] = df_per_part_id['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        review_content=df_per_part_id['REVIEW'].tolist()

        site=df_per_part_id.iloc[0,0]
        part_group_id=df_per_part_id.iloc[0,1]
        part_sub_id=df_per_part_id.iloc[0,2]
        part_id=df_per_part_id.iloc[0,3]
        keywords = '0' #0:키워드/1:문장
        sentences ='1'
        neg='N'
        pos='P'
        
        # 긍정 키워드
        pos_df=df_per_part_id[df_per_part_id['RLT_VALUE_03']>3]
        if 0<len(pos_df)<6:
            # 긍정 리뷰 리스트
            pos_review_list=pos_df['REVIEW'].tolist()
            try:
                pos_keyword=keyword(pos_review_list, stopword) #딕셔너리 {단어-점수}
                pos_keywordList=list(pos_keyword.keys())
                
                pos_keywordGradeList=list(pos_keyword.values())
                print(len(pos_keywordList))
                pos_keywordGradeList = np.round(pos_keywordGradeList,2)

                df_x = pd.DataFrame({'0': pos_keywordList})
                df_y = pd.DataFrame(columns={'0'})
                print(df_x)
                x = len(pos_keywordList)
                if len(pos_keywordList)< 6 :
                    for x in range(5-x):
                        df_y.loc[x] = ''
                    df_key = df_x.append(df_y,ignore_index=True)
                    print(df_key)
                else :
                    df_key = df_x.head(5)
                    print(df_key)

                df_a = pd.DataFrame({'0': pos_keywordGradeList})
                df_b = pd.DataFrame(columns={'0'})
                a = len(pos_keywordGradeList)
                if len(pos_keywordGradeList)< 6 :
                    for a in range(5-a):
                        df_b.loc[a] = ''
                    df_sentence = df_a.append(df_b,ignore_index=True)
                    print(df_sentence)
                else:
                    df_sentence = df_a.head(5)
                    print(df_sentence)

                result = df_key.append(df_sentence,ignore_index=True)
                result = result.transpose() 

                result_list = result.values.tolist()
                list1 = sum(result_list, [])
                
                pos_keyword_result_df=pd.DataFrame({
                    'SITE_GUBUN':site,
                    'PART_GROUP_ID':part_group_id,
                    'PART_SUB_ID':part_sub_id,
                    'PART_ID':part_id,
                    'KEYWORD_GUBUN':keywords,
                    'KEYWORD_POSITIVE':pos,
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
                print(pos_keyword_result_df)
                data_anal02 = pd.concat([data_anal02,pos_keyword_result_df],ignore_index=True)
            except Exception as e:
                print(e)
                print("{}_{}_긍정키워드 오류".format(sub_id, part_id))
                pass
            #긍정리뷰 키센텐스
            try:
                pos_keyword=keyword(pos_review_list,stopword)
                vocab_score = make_vocab_score(pos_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
                tokenizer = MaxScoreTokenizer(vocab_score)
                keysentence_list_pos=keysentence_list(pos_review_list,vocab_score,tokenizer) 
                list2 = sum(keysentence_list_pos, [])
                
                df_x = pd.DataFrame({'0': list2})
                df_y = pd.DataFrame(columns={'0'})
                print(df_x)
                x = len(list2)
                if len(list2)< 6 :
                    for x in range(5-x):
                        df_y.loc[x] = NaN
                    df_keysentence = df_x.append(df_y,ignore_index=True)
                else:
                    df_keysentence  = df_x.head(5)
                    print(df_keysentence)
                
                
                result = df_keysentence.transpose()
                result_list = result.values.tolist()
                list1 = sum(result_list, [])

                pos_keys_result_df=pd.DataFrame({
                    'SITE_GUBUN':site,
                    'PART_GROUP_ID':part_group_id,
                    'PART_SUB_ID':part_sub_id,
                    'PART_ID':part_id,
                    'KEYWORD_GUBUN':sentences,
                    'KEYWORD_POSITIVE': pos,
                    'RLT_VALUE_01' : list1[0],
                    'RLT_VALUE_02' : list1[1],
                    'RLT_VALUE_03' : list1[2],
                    'RLT_VALUE_04' : list1[3],
                    'RLT_VALUE_05' : list1[4]   
                },index=[0])
                data_anal02=pd.concat([data_anal02,pos_keys_result_df],ignore_index=True)
            except Exception as e:
                print(e)
                print("{}_{} 긍정리뷰 키센텐스 오류".format(sub_id,part_id))
                pass 
        else:
            print("{}_긍정리뷰가 부족합니다".format(part_id))

        # 부정 키워드        
        neg_df=df[df['RLT_VALUE_03']<3]
        if len(neg_df)>49:
            # 부정 리뷰 리스트
            neg_review_list=neg_df['REVIEW'].tolist()
            try:
                neg_keyword=keyword(neg_review_list, stopword) #딕셔너리 {단어-점수}
                neg_keywordList=list(neg_keyword.keys())
                neg_keywordGradeList=list(neg_keyword.values())
                print(len(neg_keywordList))
                neg_keywordGradeList = np.round(neg_keywordGradeList,2)

                df_x = pd.DataFrame({'0': neg_keywordList})
                df_y = pd.DataFrame(columns={'0'})
                print(df_x)
                x = len(neg_keywordList)
                if len(neg_keywordList)< 6 :
                    for x in range(5-x):
                        df_y.loc[x] = ''
                    df_key = df_x.append(df_y,ignore_index=True)
                    print(df_key)
                else:
                    df_key = df_x.head(5)
                    print(df_key)

                df_a = pd.DataFrame({'0': neg_keywordGradeList})
                df_b = pd.DataFrame(columns={'0'})
                a = len(neg_keywordGradeList)
                if len(neg_keywordGradeList)< 6 :
                    for a in range(5-a):
                        df_b.loc[a] = ''
                    df_sentence = df_a.append(df_b,ignore_index=True)
                    print(df_sentence)
                else:
                    df_sentence = df_a.head(5)
                    print(df_sentence)

                result = df_key.append(df_sentence,ignore_index=True)
                result = result.transpose()
                
                result_list = result.values.tolist()
                list1 = sum(result_list, [])

                neg_keyword_result_df=pd.DataFrame({
                    'SITE_GUBUN':site,
                    'PART_GROUP_ID':part_group_id,
                    'PART_SUB_ID':part_sub_id,
                    'PART_ID':part_id,
                    'KEYWORD_GUBUN':keywords,
                    'KEYWORD_POSITIVE':neg,
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
                print(neg_keyword_result_df)

                
                data_anal02 = pd.concat([data_anal02,neg_keyword_result_df],ignore_index=True)
            except Exception as e:
                print(e)
                print("{}_{}_부정키워드 오류".format(sub_id, part_id))
                pass      

        #부정리뷰 키센텐스
            try:
                neg_keyword=keyword(neg_review_list,stopword)
                vocab_score = make_vocab_score(neg_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
                tokenizer = MaxScoreTokenizer(vocab_score)
                keysentence_list_neg=keysentence_list(neg_review_list,vocab_score,tokenizer)
                list2 = sum(keysentence_list_neg, [])
                
                df_x = pd.DataFrame({'0': list2})
                df_y = pd.DataFrame(columns={'0'})
                print(df_x)
                x = len(list2)
                if len(list2)< 6 :
                    for x in range(5-x):
                        df_y.loc[x] = ''
                    df_keysentence = df_x.append(df_y,ignore_index=True)
                else:
                    df_keysentence  = df_x.head(5)
                    print(df_keysentence)
                
                result = df_keysentence.transpose()
                result_list = result.values.tolist()
                list1 = sum(result_list, [])

                neg_keys_result_df=pd.DataFrame({
                    'SITE_GUBUN':site,
                    'PART_GROUP_ID':part_group_id,
                    'PART_SUB_ID':part_sub_id,
                    'PART_ID':part_id,
                    'KEYWORD_GUBUN':sentences,
                    'KEYWORD_POSITIVE': neg,
                    'RLT_VALUE_01' : list1[0],
                    'RLT_VALUE_02' : list1[1],
                    'RLT_VALUE_03' : list1[2],
                    'RLT_VALUE_04' : list1[3],
                    'RLT_VALUE_05' : list1[4]   
                },index=[0])
                data_anal02=pd.concat([data_anal02,neg_keys_result_df],ignore_index=True)
            except Exception as e:
                print(e)
                print("{}_{} 부정리뷰 키센텐스 오류".format(sub_id, part_id))
                pass 
        else:
            print("{}_부정리뷰가 부족합니다".format(part_id))
    return data_anal02
