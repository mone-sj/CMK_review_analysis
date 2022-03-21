#-*- coding: utf-8 -*-
import pymssql, os, smtplib
import pandas as pd
from datetime import datetime
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import time

server = 'ASC-AI.iptime.org'
database = 'cosmeca'
username = 'cosmeca'
password = 'asc1234pw!'
today=datetime.now().strftime('%Y%m%d')


def today_path():
    '''backup folder create'''
    folder_path=os.getcwd()+'/etc/result'
    #folder_path=os.getcwd()+'\\etc\\result_data'
    today_path=os.path.join(folder_path,today)
    if not os.path.exists(today_path):
        os.mkdir(today_path)
    return today_path

# DB 연결
def conn_utf8():
    try:
      conn = pymssql.connect(server, username, password, database, charset="utf8")
    except Exception as e:
        print("Error: ", e)
    return conn

def conn_cp949():
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
    except Exception as e:
        print("Error: ", e)
    return conn

# GLOWPICK select per date(cp949)
def TB_GLOWPICK_DATA(from_date,to_date):
    print('GLOWPICK_DATA_LOADing')
    isrt_dttm=from_date # 초기화
    df=pd.DataFrame()   # 초기화
    try:
        conn = conn_cp949()
        cursor = conn.cursor()
        sql="select * from dbo.TB_REVIEW A (NOLOCK) where convert(char(19),ISRT_DTTM,20) between %s and %s and SITE_GUBUN='G' order by ISRT_DTTM desc"
        cursor.execute(sql,(from_date, to_date))
        row=cursor.fetchall()
        col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","ISRT_DATE","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_AGE","REVIEW_SEX","REVIEW_SKIN_TYPE","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
        ori_df=pd.DataFrame(row, columns=col_name)
        isrt_dttm=ori_df.iloc[0,16]
        isrt_dttm=isrt_dttm.strftime('%Y-%m-%d %H:%M:%S')
        df = ori_df[['SITE_GUBUN',"PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","REVIEW"]]
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return df, isrt_dttm

# 분석 실행날로부터 최근 카테고리별 1~5위까지의 part_sub_id/part_id 리스트
def TB_CRAW_top5_pid():
    print('top5_part_id_loading')
    save_path=today_path()
    try:
        conn=pymssql.connect(server, username, password, database, charset="cp949")
        cursor=conn.cursor()
        sql="select PART_GROUP_ID, PART_SUB_ID, CATEGORY_IDS,CLASS_NAME from dbo.TB_PART_GROUP (nolock) where SITE_GUBUN='N'"
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['PART_GROUP_ID', 'PART_SUB_ID', 'CATEGORY_IDS','CLASS_NAME']
        df_ori=pd.DataFrame(row, columns=col_name)
        df=df_ori.copy()

        # 최근 수집된 날짜 확인
        sql2="select top 10 ISRT_DATE from TB_CRAW_HIST where site_gubun='N' order by ISRT_DATE desc"
        cursor.execute(sql2)
        isrtDate=cursor.fetchone()
        crawHist_isrtDate=isrtDate[0]
    except Exception as e:
        print("error: ",e)
    finally:
        conn.close()
    
    cate_list=df['CATEGORY_IDS'].tolist()

    cate_count=[]
    for cate in cate_list:
        count=cate.count(',')+1
        cate_count.append(count)
    df['cate_num'] = cate_count
    df['num'] = ''

    cate = df[['PART_SUB_ID', 'cate_num', 'num']]
    matching_cate = cate.copy()
    
    num=0       
    for index in range(len(matching_cate)):
        count=matching_cate.iloc[index,1]
        if count==0:
            continue
        elif count==1:
            num=5
        elif count==2:
            num=3
        elif count==3:
            num=2
        elif count>=4:
            num=1
        matching_cate.iloc[index,2]=num
    match_cate=matching_cate[['num','PART_SUB_ID']]
    match_cate['DATE']=crawHist_isrtDate

    part_col=['PART_SUB_ID', 'PART_ID']
    top5_concat=pd.DataFrame(columns=part_col)
    
    try:
        conn=pymssql.connect(server, username, password, database, charset="cp949")
        cursor=conn.cursor()
        for i, df_row  in match_cate.iterrows():
            sql="select distinct part_sub_id, part_id from TB_CRAW_HIST A where PART_ID IN (select PART_ID from TB_CRAW_HIST where site_gubun='N' and CRAW_DATA_ID='05' and RSLT_DATA_01 <= %s and PART_SUB_ID=%s and isrt_date=%s)"
            cursor.execute(sql,(tuple(df_row)))
            row=cursor.fetchall()
            top5=pd.DataFrame(row, columns=part_col)
            top5_concat=pd.concat([top5_concat,top5])
        top5_concat.to_csv(f'{save_path}/{today}_{crawHist_isrtDate}std_top5_product.csv',index=False)
        part_id_list=top5_concat['PART_ID'].values.tolist()
        print(f'part_id 개수: {len(part_id_list)}')
    except Exception as e:
        print("error: ",e)
    finally:
        conn.close()
    return part_id_list, crawHist_isrtDate


# TB_REVIEW add review select per top5 part_sub_id, part_id(cp949)
def TB_review_addTop5Review(part_id_list):
    print('db_data_loading : TB_REVIEW add review select per top5_NAVER')
    try:
        conn=conn_cp949()
        df_concat=pd.DataFrame()
        start_time=time.time()
        for part_id in part_id_list:
            print(f'TB_review_addTop5Review함수_{part_id}')
            cursor=conn.cursor()
            sql="select * from TB_REVIEW C (nolock) where C.REVIEW_DOC_NO NOT IN (select A.REVIEW_DOC_NO from TB_REVIEW A join TB_REVIEW_ANAL_00_N B on A.PART_ID=B.PART_ID and A.REVIEW_DOC_NO=B.REVIEW_DOC_NO) and part_id=%s order by ISRT_DTTM desc"
            cursor.execute(sql,(part_id))
            row=cursor.fetchall()
            col_name=["SITE_GUBUN","PART_GROUP_ID","PART_SUB_ID","PART_ID","REVIEW_DOC_NO","ISRT_DATE","REVIEW_USER","REVIEW_DTTM","REVIEW_GRADE","REVIEW_AGE","REVIEW_SEX","REVIEW_SKIN_TYPE","REVIEW","REMARK","ISRT_USER","UPDT_USER","ISRT_DTTM","UPDT_DTTM"]
            ori_df=pd.DataFrame(row,columns=col_name)
            df_concat=pd.concat([df_concat,ori_df],ignore_index=True)
        df=df_concat[['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','REVIEW']]
        df= df.dropna(axis=0)
        print(f'분석리뷰수: {len(df)}')
        print(f'리뷰 가져오는 시간: {time.time()-start_time}')
        #df.to_csv(f'{save_path}/naver_top5_review.csv',index=False)
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return df

# 딕셔너리(part_sub_id : model_id) 반환 (cp949)
def TB_model_id():
    try:
        conn=conn_cp949()
        cursor=conn.cursor()
        sql='select PART_SUB_ID, MODEL_ID from TB_PART_GROUP (nolock)'
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['part_sub_id','model_id']
        df=pd.DataFrame(row, columns=col_name)
        df_dict=dict(df.values.tolist())
    except Exception as e:
        print("Error : ",e)
    finally:
        conn.close()
    return df_dict

# stopwords list 반환(utf8)
def TB_stopwords():
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql='select * from dbo.TB_UNUSE_KEYWORD (nolock)'
        cursor.execute(sql)
        row=[item[0] for item in cursor.fetchall()]
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return row

# property_code - name dict 반환(cp949)
def TB_property_id():
    try:
        conn=conn_cp949()
        cursor=conn.cursor()
        sql="select CD_NM, SUB_CD from dbo.TB_CODE where GROUP_CD='PROPERTY_ID'"
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['cd_nm','sub_cd']
        df=pd.DataFrame(row,columns=col_name)
        df_dict=dict(df.values.tolist())
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return df_dict


######### 키워드/센텐스 결과를 위한 리뷰 select
# 조인
def TB_REVIEW_join_G():
    '''select TB_REVIEW per part_id for TB_REVIEW & TB_REVIEW_ANAL00_G join'''
    print('----------REVIEW_JOIN_G')
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor=conn.cursor()
        sql="select SITE_GUBUN, PART_GROUP_ID, PART_SUB_ID, PART_ID, REVIEW_DOC_NO from TB_REVIEW (nolock) where SITE_GUBUN='G'"
        cursor.execute(sql)
        row_a=cursor.fetchall()
        col_nameA=["SITE_GUBUN","PART_GROUP_ID", "PART_SUB_ID", "PART_ID","REVIEW_DOC_NO"]
        df_A=pd.DataFrame(row_a,columns=col_nameA)

        sql2="select REVIEW_DOC_NO, PART_ID, DOC_PART_NO, REVIEW, RLT_VALUE_03 from TB_REVIEW_ANAL_00_G (nolock) "
        cursor.execute(sql2)
        row_b=cursor.fetchall()
        col_nameB=['REVIEW_DOC_NO', 'PART_ID', 'DOC_PART_NO','REVIEW','RLT_VALUE_03']
        df_B=pd.DataFrame(row_b,columns=col_nameB)

    except Exception as e:
        print(e)
    finally:
        conn.close()

    df=pd.merge(df_A,df_B,on=['REVIEW_DOC_NO','PART_ID'])
    df=df[['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']]
    return df

def TB_REVIEW_join_N():
    '''select TB_REVIEW per part_id for TB_REVIEW & TB_REVIEW_ANAL00_N join'''
    print('----------REVIEW_JOIN_N')
    try:
        conn = pymssql.connect(server, username, password, database, charset="cp949")
        cursor=conn.cursor()
        sql="select SITE_GUBUN, PART_GROUP_ID, PART_SUB_ID, PART_ID, REVIEW_DOC_NO, REVIEW from TB_REVIEW (nolock) where SITE_GUBUN='N'"
        cursor.execute(sql)
        row_a=cursor.fetchall()
        col_nameA=["SITE_GUBUN","PART_GROUP_ID", "PART_SUB_ID", "PART_ID","REVIEW_DOC_NO","REVIEW"]
        df_A=pd.DataFrame(row_a,columns=col_nameA)
   
        sql2="select REVIEW_DOC_NO, PART_ID, DOC_PART_NO, RLT_VALUE_03 from TB_REVIEW_ANAL_00_N (nolock) "
        cursor.execute(sql2)
        row_b=cursor.fetchall()
        col_nameB=['REVIEW_DOC_NO', 'PART_ID', 'DOC_PART_NO','RLT_VALUE_03']
        df_B=pd.DataFrame(row_b,columns=col_nameB)
    except Exception as e:
        print(e)
    finally:
        conn.close()

    df=pd.merge(df_A,df_B,on=['REVIEW_DOC_NO','PART_ID'])
    df=df[['SITE_GUBUN','PART_GROUP_ID','PART_SUB_ID','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']]
    return df

def anal00_part_id_list(site):
    '''
    키워드/센텐스 프로세스 실행을 위한 ANAL00의 part_sub_id/part_id 리스트 반환
    '''
    try:
        conn=conn_cp949()
        cursor=conn.cursor()
        if site=='N': # 네이버
            sql="select distinct A.PART_SUB_ID, A.PART_ID from TB_REVIEW A (nolock) join TB_REVIEW_ANAL_00_N B on A.REVIEW_DOC_NO=B.REVIEW_DOC_NO and A.PART_ID=B.PART_ID and A.SITE_GUBUN='N'"
        elif site=='G': # 글로우픽
            sql="select distinct A.PART_SUB_ID, A.PART_ID from TB_REVIEW A (nolock) join TB_REVIEW_ANAL_00_G B on A.REVIEW_DOC_NO=B.REVIEW_DOC_NO and A.PART_ID=B.PART_ID and A.SITE_GUBUN='G'"
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=["PART_SUB_ID", "PART_ID"]
        df=pd.DataFrame(row, columns=col_name)
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    part_list=df.values.tolist()
    return part_list

## 키워드/센텐스 결과를 위한 리뷰 select
# 글로우픽 ANAL00
def anal00_G() :
    try:
        conn = conn_cp949()
        cursor=conn.cursor()
        sql = "select SITE_GUBUN,PART_ID,REVIEW_DOC_NO,DOC_PART_NO,REVIEW,RLT_VALUE_03 from TB_REVIEW_ANAL_00_G (nolock) "
        cursor.execute(sql)
        row=cursor.fetchall()
        col_name=['SITE_GUBUN','PART_ID','REVIEW_DOC_NO','DOC_PART_NO','REVIEW','RLT_VALUE_03']
        anal00 = pd.DataFrame(row,columns=col_name)
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()
    return anal00

'''DB insert'''
def TB_anal00_N_insert(df):
    try:
        print('anal00_N insert')
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            sql="exec dbo.P_MNG_ANA000 @section='SN', @site_gubun=%s, @review_doc_no=%s, @part_id=%s, @doc_part_no=%s, @rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s"
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("anal00_N_insert완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal00_G_insert(df):
    try:
        print('anal00_G insert')
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            sql="exec dbo.P_MNG_ANA000 @section='SG', @site_gubun=%s, @review_doc_no=%s, @part_id=%s, @doc_part_no=%s, @review=%s,@rlt_value_01=%s, @rlt_value_02=%s,@rlt_value_03=%s"
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("anal00_G_insert완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal02_insert(df):
    try:
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            if row[4]=='0': #키워드
                sql="exec dbo.P_MNG_ANA002 @section='SA', @site_gubun=%s, @part_group_id=%s,@part_sub_id=%s, @part_id=%s, @keyword_gubun=%s, @keyword_positive=%s,@rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
                cursor.execute(sql,tuple(row))
                
            elif row[4]=='1': #핵심문장
                sql="exec dbo.P_MNG_ANA002 @section='SA', @site_gubun=%s, @part_group_id=%s,@part_sub_id=%s, @part_id=%s, @keyword_gubun=%s, @keyword_positive=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
                cursor.execute(sql, tuple(row))
            print("anal02_recode inserted")
            conn.commit()
        print("anal02_insert_완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal03_insert(df):
    try:
        conn=conn_utf8()
        for i, row in df.iterrows():
            cursor=conn.cursor()
            
            if row[4]=='0': # 키워드
                sql="exec dbo.P_MNG_ANA003 @section='SA',@site_gubun=%s,@part_group_id=%s,@part_sub_id=%s,@part_id=%s,@keyword_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s, @rlt_value_06=%s, @rlt_value_07=%s, @rlt_value_08=%s, @rlt_value_09=%s, @rlt_value_10=%s"
                cursor.execute(sql,tuple(row))
            elif row[4]=='1': #핵심문장
                sql="exec dbo.P_MNG_ANA003 @section='SA',@site_gubun=%s,@part_group_id=%s,@part_sub_id=%s,@part_id=%s, @keyword_gubun=%s, @rlt_value_01=%s, @rlt_value_02=%s, @rlt_value_03=%s, @rlt_value_04=%s, @rlt_value_05=%s"
                cursor.execute(sql, tuple(row))
            print("anal03_record inserted")
            conn.commit()
        print("anal03_insert_완료")
    except Exception as e:
        print("Error: ",e)
    finally:
        conn.close()

def TB_anal00_N_delete():
    '''review테이블에 삭제된 리뷰 ANAL00_N 테이블에서도 삭제'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA000 @section='DN'"
        cursor.execute(sql)
        conn.commit()
        print("anal00_N_delete 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal00_G_delete():
    '''review테이블에 삭제된 리뷰 ANAL00_G 테이블에서도 삭제'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA000 @section='DG'"
        cursor.execute(sql)
        conn.commit()
        print("anal00_G_delete 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal01_count_G():
    '''ANAL01 테이블 insert&update (GLOWPICK count 실행)'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA001 @section='SG'"
        cursor.execute(sql)
        conn.commit()
        print("anal01_count_G 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal04_count_G():
    '''ANAL04 테이블 insert&update (GLOWPICK count 실행)'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA004 @section='SG'"
        cursor.execute(sql)
        conn.commit()
        print("anal04_count_G 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal01_count_N():
    '''ANAL01 테이블 insert&update (NAVER count 실행)'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA001 @section='SN'"
        cursor.execute(sql)
        conn.commit()
        print("anal01_count_N 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def TB_anal04_count_N():
    '''ANAL04 테이블 insert&update (NAVER count 실행)'''
    try:
        conn=conn_utf8()
        cursor=conn.cursor()
        sql="exec dbo.P_MNG_ANA004 @section='SN'"
        cursor.execute(sql)
        conn.commit()
        print("anal04_count_N 완료")
    except Exception as e:
        print("Error: ", e)
    finally:
        conn.close()

def last_isrt_dttm():
    with open('./etc/last_isrt_dttm_G.txt','r',encoding='utf8') as f:
        lines=f.readlines()
        isrt_date=lines[-1].split('\t')[1]
    return isrt_date

def save_txt(content_list,file_path):
    file_name=f'{file_path}.txt'
    with open(file_name,'a',encoding='utf8') as f:
        for line in content_list:
            f.write(f'{line}\n')

def time_txt(content_list,file_path):
    file_name=f'{file_path}.txt'
    if not os.path.exists(file_name):
        with open(file_name,'a',encoding='utf8') as f:
            f.write('분석날짜\t분석모델\t분석제품수\t총 리뷰수\t분석시간\tsite_gubun\t스플릿된 리뷰수\t분석 리뷰수\n')
            
    with open(file_name,'a',encoding='utf8') as f:
        for line in content_list:
            f.write(f'{line}\t')
        f.write("\n")

### send mail infomation
sendEmail="asclhg@naver.com"
recvEmail="seojeong@asc.kr"
mail_pw="sendEmail의 비밀번호"

smtpName="smtp.naver.com" # 보내는 메일이 다른 계정이면 수정필요
smtpPort=587
session=None


def success_sendEmail():
    try:
        path=today_path()
        # SMTP 세션 생성
        session=smtplib.SMTP(smtpName,smtpPort)
        session.set_debuglevel(False)
        
        # SMTP 계정 인증 설정
        session.ehlo()
        session.starttls()
        session.login(sendEmail,mail_pw)

        # 메일컨텐츠 설정
        message=MIMEMultipart("mixed")
        
        # 메일 송/수신 옵션 설정
        message.set_charset('utf-8')
        
        content_path=os.path.join(path,'분석시간체크.txt')
        if not os.path.exists(content_path):
            text=f'{today} 분석완료'
        else:
            with open(content_path,'r',encoding='utf8') as f:
                text=f.read()
        
        message['From']=sendEmail
        message['To']=recvEmail
        message['Subject']=f"[리뷰분석완료]{today} 코스메카 분석완료"
        message.attach(MIMEText(text))
        
        # 메일 첨부파일
        attachments=os.listdir(path) # file_list: 폴더 내 파일 리스트
        
        work_dir=os.getcwd()
        isrt_dttm_path=os.path.join(work_dir,'etc','last_isrt_dttm.txt')
        #isrt_attach=open(isrt_dttm_path,'rb')
        part=MIMEBase('application','octet-stream')
        part.set_payload(open(isrt_dttm_path,'rb').read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", 'attachment',filename=os.path.basename(isrt_dttm_path))
        message.attach(part)

        if len(attachments) > 0:
            for attachment in attachments:
                file_path=os.path.join(path,attachment)
                attach_binary=MIMEBase("application","octet-stream")
                try:
                    attach_file=open(file_path,"rb").read() #open the file
                    attach_binary.set_payload(attach_file)
                    encoders.encode_base64(attach_binary)
                    
                    # 파일이름 지정된 report header추가
                    attach_binary.add_header("Content-Disposition", 'attachment',filename=attachment)
                    message.attach(attach_binary)
                except Exception as e:
                    print(e)

        # 메일 발송
        session.sendmail(sendEmail,recvEmail,message.as_string())
        print("successfully sent the mail")

    except Exception as e:
        print(e)
    finally:
        if session is not None:
            session.quit()


def fail_sendEmail(err):
    msg=MIMEText(err)
    msg['Subject']=f"[리뷰분석오류]{today} 코스메카 분석 오류"
    msg['From']=sendEmail
    msg['To']=recvEmail
    print(msg.as_string())

    s=smtplib.SMTP(smtpName,smtpPort)
    s.starttls()
    s.login(sendEmail,mail_pw)
    s.sendmail(sendEmail,recvEmail,msg.as_string())
    s.close()
    print("successfully sent the mail")



