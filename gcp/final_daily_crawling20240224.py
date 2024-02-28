from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, time, timedelta, date
from airflow.models import Variable
import json
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
import random
import requests
from collections import OrderedDict
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

keywords=['경복궁', '광화문', '덕수궁', '보신각', '암사동', '창덕궁', '종묘', '가산디지털단지역', '강남역', 
'건대입구역', '고덕역', '고속터미널역', '교대역', '구로디지털단지역', '구로역', '군자역', '남구로역', 
'대림역', '동대문역', '뚝섬역', '미아사거리역', '발산역', '북한산우이역', '사당역', '삼각지역', 
'서울대입구역', '서울식물원역', '마곡나루역', '서울역', '선릉역', '성신여대입구역', '수유역', 
'신논현역', '논현역', '신도림역', '신림역', '신촌역', '역삼역', '연신내역', 
'왕십리역', '용산역', '이태원역', '장지역', '장한평역', '천호역', 
'총신대입구(이수)역', '충정로역',  '혜화역', '홍대입구역', '회기역', 
'가락시장', '가로수길', '김포공항', '낙산공원', '이화마을', '노량진', '덕수궁길', 
'정동길', '방배역', '북촌한옥마을', '서촌', '성수카페거리', '수유리', '쌍문동', '압구정로데오거리',
 '여의도', '연남동', '영등포', '외대앞', '용리단길', '이태원', '인사동', '익선동', '창동', 
'청담동', '청량리', '해방촌', '경리단길', 'DDP', 'DMC', '강서한강공원', '고척돔', 
'광나루한강공원', '광화문광장', '국립중앙박물관', '용산가족공원', '난지한강공원', '남산공원', 
'노들섬', '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲', '불광천', '서리풀공원', 
'몽마르뜨공원', '서울숲공원', '시청광장', '아차산', '양화한강공원', '어린이대공원', '여의도한강공원', 
'월드컵공원', '응봉산', '이촌한강공원', '잠실종합운동장', '잠실한강공원', '잠원한강공원', '청계산', 
'청와대']
# '이대역','419카페거리주변','광장시장','오목교역','합정역','양재역',

class CustomXComEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return super().default(obj)

def execute_mysql_query_and_save_result(keyword, now, **kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='google_cloud_sql_conn', schema='foodpaltette')
    result = mysql_hook.get_records(sql=f"SELECT store_id FROM keyword_stores where keyword='{keyword}';")
    # Flatten the result list
    flattened_result = [now, [item for sublist in result for item in sublist]]
    variable_value = json.dumps(flattened_result, cls=CustomXComEncoder)
    Variable.set("daily_store_ids", variable_value)

# def update_store_info_daily(keyword, now, store_id):
#     mysql_hook = MySqlHook(mysql_conn_id='google_cloud_sql_conn', schema='foodpaltette')
#     result = mysql_hook.get_records(sql=f"UPDATE keyword_stores SET daily_update_date = '{now}' WHERE keyword = '{keyword}' AND store_id = {store_id};")
#     logging.info(f"update_store_info {keyword}, {store_id} SUCCESS")

def upload_to_s3(lists, key, bucket_name):
    # 리스트를 JSON 형식으로 변환
    json_content = json.dumps(lists, ensure_ascii=False, indent=4)

    # S3로 JSON 형식의 내용을 업로드
    hook = S3Hook('aws_connections')
    hook.load_string(string_data=json_content,
                     key=key,
                     bucket_name=bucket_name,
                     replace=True)
    logging.info(f"upload to {key}")

def daily_get_visitor_reviews_blogs(keyword, store_id, now_date): ##keyword, c_id
    logging.info(now_date)
    try:
        response = requests.get('https://place.map.kakao.com/main/v/' + str(store_id))
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh}")
        raise Exception(f"{store_id} :get_store_comments : HTTP ERROR")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
        raise Exception(f"{store_id} :get_store_comments : ConnectionError")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
        raise Exception(f"{store_id} :get_store_comments : Timeout")
    except requests.exceptions.RequestException as err:
        logging.error(f"Error: {err}")
        raise Exception(f"{store_id} :get_store_comments : RequestException")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise Exception(f"{store_id} :get_store_comments : UnexpectedError")
    else:
        data = response.json()
        ##Visotors
        visitors_lists=[]
        hrs = []
        store_info2 = OrderedDict()
        s2graph_info = data.get('s2graph', {})
        if s2graph_info:
            store_info2['current_time'] = now_date
            days_of_week = ['sunday', 'monday', 'tuesday',
                        'wednesday', 'thursday', 'friday', 'saturday']
            store_info2['days'] = OrderedDict(
                (day, s2graph_info.get('day', {}).get(day, 0)) for day in days_of_week)
            store_info2['gender'] = {
                'data': s2graph_info.get('gender', {}).get('data', []),
                'labels': s2graph_info.get('gender', {}).get('labels', [])
            }
            store_info2['age'] = {
            'data': s2graph_info.get('age', {}).get('data', []),
            'labels': s2graph_info.get('age', {}).get('labels', [])
            }
            visitors_lists.append(store_info2.copy())  # 복사본 추가
            bucket_path=f"kakao/{keyword}/{now_date}/{store_id}_visitor.json"
            upload_to_s3(visitors_lists, bucket_path, "de-4-1-bucket")
        else:
            logging.info(f"No Comment in {keyword}, {store_id} {now_date}")

        comment_lists = []
        review_info = OrderedDict()
        comment_info = data.get('comment', {})
        if len(comment_info)!=0:
            if "list" in comment_info.keys():
                review_info["comment_num"] = comment_info.get("kamapComntcnt", 0)
                review_info["comment_sum"] = comment_info.get("scoresum", 0)
                review_info["comment_cnt"] = comment_info.get("scorecnt", 0)
                review_info["strengthCounts"] = comment_info.get("strengthCounts", {})
                review_info["comment_list"] = []
                last_date = comment_info.get('list')[-1]['date'].replace('.', '')
                last_idx = comment_info.get('list')[-1]['commentid']
                while last_date >= now_date and comment_info.get('hasNext'):
                    if last_date == now_date:
                        for idx, comment in enumerate(comment_info.get('list')):
                            review_date = comment['date'].replace('.', '')
                            if review_date == now_date:
                                review_info["comment_list"].append(comment)
                            elif review_date < now_date:
                                break
                    response = requests.get(
                        'https://place.map.kakao.com/commentlist/v/' + str(store_id) + '/' + str(last_idx))
                    data = response.json()
                    comment_info = data.get('comment', {})
                    last_date = comment_info.get('list')[-1]['date'].replace('.', '')
                    last_idx = comment_info.get('list')[-1]['commentid']

                for comment in comment_info.get('list'):
                    if comment['date'].replace('.', '') == now_date:
                        review_info["comment_list"].append(comment)
                    elif comment['date'].replace('.', '') < now_date:
                        break
                if len(review_info['comment_list'])!=0:
                    comment_lists.append(review_info.copy())
                    bucket_path=f"kakao/{keyword}/{now_date}/{store_id}_comments.json"
                    upload_to_s3(comment_lists, bucket_path, "de-4-1-bucket")
                else:
                    logging.info(f"No Comment in {keyword}, {store_id} {now_date}")
        else:
            logging.info(f"No Comment in {keyword}, {store_id} {now_date}")
        
        ##blogs
        blogs_lists = []
        store_info4 = OrderedDict()

        blog_info = data.get('blogReview', {})
        if len(blog_info)!=0:
            store_info4["blog_num"] = blog_info["blogrvwcnt"]
            store_info4["blog_list"] = []

            lastdate = blog_info["list"][-1]['date'].replace('.', '')  # 점 제거
            while lastdate >= now_date and "moreId" in blog_info.keys():
                lastidx = blog_info["moreId"]
                if lastdate==now_date:
                    for blog in blog_info["list"]:
                        blog_date = blog['date'].replace('.', '')  # 점 제거하고 마지막 문자열 제거
                        if blog_date == now_date:
                            store_info4["blog_list"].append(blog)
                        elif blog_date < now_date:
                            break
                response = requests.get(
                   'https://place.map.kakao.com/blogrvwlist/v/' + str(store_id) + '/' + str(lastidx))
                data3 = response.json()
                blog_info = data3.get('blogReview', {})
            if len(blog_info)!=0:  
                for blog in blog_info["list"]:
                    blog_date = blog['date'].replace('.', '')  # 점 제거하고 마지막 문자열 제거
                    if blog_date == now_date:
                        store_info4["blog_list"].append(blog)
                    elif blog_date < now_date:
                        break
            if len(store_info4['blog_list'])!=0:
                blogs_lists.append(store_info4.copy())
                upload_to_s3(blogs_lists, f'kakao/{keyword}/{now_date}/{store_id}_blogs.json', 'de-4-1-bucket')  
            else:
                logging.info(f"No blog info in {keyword}, {store_id} {now_date}")
        else:
            logging.info(f"No blog info in {keyword}, {store_id} {now_date}")
    


def set_daily_keyword(**kwargs):
    execution_date = kwargs.get('execution_date')
    # execution_date에서 하루를 뺀 날짜
    previous_day = execution_date - timedelta(days=1)

    # 하루 전 날짜를 원하는 형식으로 포맷팅
    formatted_previous_day = previous_day.strftime('%Y%m%d')
    date_obj = datetime.strptime(formatted_previous_day, "%Y%m%d")
    # 날짜에 시간을 추가하여 23:59:59로 설정
    timestamp_prev_day = datetime.combine(date_obj, time.max)
    logging.info("DAG : daily_set_keyword, TASK : set_daily_keyword ")
    daily_index = int(Variable.get("daily_index"))
    for index in range(daily_index, len(keywords), 1):
        daily_keyword=keywords[index]
        Variable.set("daily_keyword", daily_keyword)
        logging.info(f"now_index : {index}, now_keyword : {daily_keyword}")
        daily_store_index=int(Variable.get("daily_store_index"))
        c_li_dict={}
        if daily_store_index==0:
            execute_mysql_query_and_save_result(daily_keyword,timestamp_prev_day) ## id 받아오기 => [datetime, [c_id_list]]
            logging.info("ENTER")
        c_li_list=json.loads(Variable.get('daily_store_ids')) #string => dict 작업 필요
        store_ids = c_li_list[1]
        now=c_li_list[0]
        for index2 in range(daily_store_index, len(store_ids), 1):
            daily_get_visitor_reviews_blogs(daily_keyword, store_ids[index2], formatted_previous_day) ##keyword, c_id, 20200202
            #mysql update
            #update_store_info_daily(daily_keyword, timestamp_prev_day, store_ids[index2])
            Variable.set("daily_store_index", index2+1)
        Variable.set("daily_index", index+1)
        Variable.set("daily_store_index", 0)
    Variable.set("daily_index", 0)
    logging.info("Success")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2024, 2, 20, 3, 10), #한국시간 기준 12:10
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='daily_crawling_00020240225',
    default_args=default_args,
    description='daily crawling dag',
    schedule_interval="10 3 * * 1-5", #주중에 실행(주말은 trigger)
    catchup=False,
)

daily_get_keyword = PythonOperator(
    task_id='daily_get_keyword_task00020240225',
    python_callable=set_daily_keyword,
    provide_context=True,
    dag=dag,
)
daily_get_keyword