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

    for i in id_list: # id_list=[[sub_id,part_id],[sub_id,part_id]]
        sub_id=i[0]
        part_id=i[1]

        df_per_part_id=df[(df['PART_SUB_ID']==sub_id) & (df['PART_ID']==part_id)]

        #stopwords
        stopword=db.TB_stopwords()

        df_per_part_id['REVIEW'] = df_per_part_id['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        review_content=df_per_part_id['REVIEW'].tolist()

        site=df_per_part_id.iloc[0,0]
        part_group_id=df_per_part_id.iloc[0,1]
        part_sub_id=df_per_part_id.iloc[0,2]
        part_id=df_per_part_id.iloc[0,3]
        keywords = '0' #0:키워드/1:문장
        sentences ='1'

        # 전체 키워드
        try:
            all_keyword=keyword(review_content, stopword) #딕셔너리 {단어-점수}
            All_keywordList=list(all_keyword.keys())
            print("================전체키워드========================================")
            print(All_keywordList)
            print("==================================================================")
            All_keywordGradeList=list(all_keyword.values())
            print(len(All_keywordList))
            All_keywordGradeList = np.round(All_keywordGradeList,2)
            print(All_keywordGradeList)

            df_x = pd.DataFrame({'0': All_keywordList})
            df_y = pd.DataFrame(columns={'0'})
            print(df_x)
            x = len(All_keywordList)
            if len(All_keywordList)< 6 :
                for x in range(5-x):
                    df_y.loc[x] = ''
                df_key = df_x.append(df_y,ignore_index=True)
                print(df_key)
            else :
                df_key = df_x.head(5)
                print(df_key)
                
            df_a = pd.DataFrame({'0': All_keywordGradeList})
            df_b = pd.DataFrame(columns={'0'})
            a = len(All_keywordGradeList)
            if len(All_keywordGradeList)< 6 :
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

            all_keyword_result_df=pd.DataFrame({
                'SITE_GUBUN':site,
                'PART_GROUP_ID':part_group_id,
                'PART_SUB_ID':part_sub_id,
                'PART_ID':part_id,
                'KEYWORD_GUBUN':keywords,
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

        except Exception as e:
            print(e)
            print("{}_{}_전체리뷰 키워드 오류".format(sub_id,part_id))
            pass
        
        data_anal03 = pd.concat([data_anal03,all_keyword_result_df],ignore_index=True)
                
        #전체핵심문장
        try:
            vocab_score = make_vocab_score(all_keyword, stopword, scaling=lambda x: 1)  # scailing 1 로 함으로써 유사 비중
            tokenizer = MaxScoreTokenizer(vocab_score)
            keysentence_list_all=keysentence_list(review_content,vocab_score,tokenizer)
            list2 = sum(keysentence_list_all, [])

            df_x = pd.DataFrame({'0': list2})
            print(df_x)
            df_y = pd.DataFrame(columns={'0'})
            print(df_y)
            x = len(list2)
            if len(list2)< 6 :
                for x in range(5-x):
                    df_y.loc[x] = ''
                df_keysentence = df_x.append(df_y,ignore_index=True)
            else:
                df_keysentence = df_x.head(5)
                print(df_keysentence)

            result = df_keysentence.transpose()
            result_list = result.values.tolist()
            list1 = sum(result_list, [])

            all_keysentece_result_df=pd.DataFrame({
                'SITE_GUBUN':site,
                'PART_GROUP_ID':part_group_id,
                'PART_SUB_ID':part_sub_id,
                'PART_ID':part_id,
                'KEYWORD_GUBUN':sentences,
                'RLT_VALUE_01' : list1[0],
                'RLT_VALUE_02' : list1[1],
                'RLT_VALUE_03' : list1[2],
                'RLT_VALUE_04' : list1[3],
                'RLT_VALUE_05' : list1[4],
            },index=[0])
           
        except Exception as e:
            print(e)
            print("{}_{} 전체리뷰 키센텐스 오류".format(sub_id, part_id))
            pass

        data_anal03 = pd.concat([data_anal03,all_keysentece_result_df],ignore_index=True)
        print(data_anal03)
        
    return data_anal03



def emo(id_list):
    '''
    긍정/부정리뷰의 키워드/핵심문장 추출
    '''
    col_name2=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","KEYWORD_GUBUN","KEYWORD_POSITIVE","RLT_VALUE_01","RLT_VALUE_02","RLT_VALUE_03","RLT_VALUE_04","RLT_VALUE_05",
    "RLT_VALUE_06","RLT_VALUE_07","RLT_VALUE_08","RLT_VALUE_09","RLT_VALUE_10"]
    data_anal02=pd.DataFrame(columns=col_name2)

    for i in id_list: # id_list=[[sub_id,part_id],[sub_id,part_id]]
        sub_id=i[0]
        part_id=i[1]
        
        # review+anal00 join data
        # df_columns=site_gubun, part_group_id, part_sub_id, part_id, review_doc_no, review, rlt_value_03
        df=db.TB_join(sub_id,part_id)

        #stopwords
        stopword=db.TB_stopwords()

        site=df.iloc[0,0]
        part_group_id=df.iloc[0,1]
        part_sub_id=df.iloc[0,2]
        part_id=df.iloc[0,3]
        keywords = '0' #0:키워드/1:문장
        sentences ='1'
        neg='N'
        pos='P'
        df['REVIEW'] = df['REVIEW'].str.replace(pat=r'[^\w\s]', repl=r' ', regex=True)
        # 긍정 키워드
        pos_df=df[df['RLT_VALUE_03']>3]
        if len(pos_df) >49:
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
