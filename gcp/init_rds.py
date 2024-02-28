import boto3
from datetime import datetime, timezone, timedelta

# AWS 계정의 인증 정보 설정
aws_access_key_id = 
aws_secret_access_key = 
region_name = 'ap-northeast-2'

def get_keyword_store_info(aws_access_key_id, aws_secret_access_key, region_name):
    bucket_name = 'de-4-1-bucket'
    prefix = 'kakao/'
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    
    # S3 객체 목록 가져오기
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    folder_list=[]
    # 가져온 폴더 목록 출력
    if 'CommonPrefixes' in response:
        folders = [prefix.get('Prefix') for prefix in response['CommonPrefixes']]
        folder_full_list=[]
        for folder in folders:
            folder=folder.split('/')
            #print(f"Folder: {folder[1]}")
            folder_list.append(folder[1])       
    else:
        print("No folders found.")

    # 가게별 파일 수 가져오기
    bucket_name = 'de-4-1-bucket'
    store_dict={}
    for folder in folder_list:
        folder_path = 'kakao/' + folder + '/'

        file_count = 0
        store_list=[]
        date=""

        paginator = s3.get_paginator('list_objects_v2')
        list1 = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)

        for page in list1:
            for item in page['Contents']:
                #print(item)
                temp_list=item['Key'].split('/')
                #'kakao/419카페거리주변/20240226/9806820_info.json'
                file_name=temp_list[3]
                store=file_name.split("_")[0]
                file_count += 1
                store_list.append(store)
        store_dict[folder]=[list(set(store_list)), len(set(store_list))]
    return store_dict ##{keyword:[[store_list], 갯수]}

import mysql.connector

# MySQL 연결 설정
config = {
    'user': 'root',
    'password': 
    'host':  # 예: '127.0.0.1' 또는 'your-project-id:your-region:your-instance-name'
    'database': 'foodpaltette',
    'raise_on_warnings': True
}

# MySQL 연결
conn = mysql.connector.connect(**config)
# 커서 생성
cursor = conn.cursor()

init_dict=get_keyword_store_info(aws_access_key_id, aws_secret_access_key, region_name)
print("SUCCESS ")
truncate_query="TRUNCATE keyword_stores"
cursor.execute(truncate_query)


init_dict=(get_keyword_store_info(aws_access_key_id, aws_secret_access_key, region_name))
# 기본값으로 사용할 날짜 및 시간
default_date_str = '2024-01-01 00:00:00'
# '2024-01-01 00:00:00'을 유닉스 타임스탬프로 변환
default_timestamp = int(datetime.strptime(default_date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
for key in init_dict:
    keyword=key
    store_list=init_dict[key][0]
    for store in store_list:
        data_to_insert = (keyword, store, default_timestamp, default_timestamp)
        insert_query = "INSERT INTO keyword_stores (keyword, store_id, monthly_update_date, daily_update_date) VALUES (%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))"
        cursor.execute(insert_query, data_to_insert)
    conn.commit()
#커넥션 닫기
cursor.close()
conn.close()

