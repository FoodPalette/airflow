import boto3
from datetime import datetime

# AWS 계정의 인증 정보 설정
aws_access_key_id = 
aws_secret_access_key =
region_name = 'ap-northeast-2'

#S3 클라이언트 생성
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

## 가게 이름 가져오기
# S3 버킷과 경로 설정
bucket_name = 'de-4-1-bucket'
prefix = 'kakao/'

# S3 객체 목록 가져오기
response = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

# 가져온 폴더 목록 출력
if 'CommonPrefixes' in response:
    folders = [prefix.get('Prefix') for prefix in response['CommonPrefixes']]
    num_folders=0
    folder_list=[]
    folder_full_list=[]
    for folder in folders:
        folder=folder.split('/')
        #print(f"Folder: {folder[1]}")
        folder_list.append(folder[1])
        folder_full_list.append(folder[1:])
        num_folders+=1
    print(num_folders)
    
else:
    print("No folders found.")
print(len(folder_list))
print(len(set(folder_list)))
print("--------------------------------------------")
keywords=['경복궁', '광화문', '덕수궁', '보신각', '암사동', '창덕궁', '종묘', '가산디지털단지역', '강남역', 
'건대입구역', '고덕역', '고속터미널역', '교대역', '구로디지털단지역', '구로역', '군자역', '남구로역', 
'대림역', '동대문역', '뚝섬역', '미아사거리역', '발산역', '북한산우이역', '사당역', '삼각지역', 
'서울대입구역', '서울식물원역', '마곡나루역', '서울역', '선릉역', '성신여대입구역', '수유역', 
'신논현역', '논현역', '신도림역', '신림역', '신촌역', '이대역','역삼역', '연신내역', 
'오목교역', '왕십리역', '용산역', '이태원역', '장지역', '장한평역', '천호역', 
'총신대입구(이수)역', '충정로역', '합정역', '혜화역', '홍대입구역', '회기역', '419카페거리주변', 
'가락시장', '가로수길', '광장시장', '김포공항', '낙산공원', '이화마을', '노량진', '덕수궁길', 
'정동길', '방배역', '북촌한옥마을', '서촌', '성수카페거리', '수유리', '쌍문동', '압구정로데오거리',
 '여의도', '연남동', '영등포', '외대앞', '용리단길', '이태원', '인사동', '익선동', '창동', 
'청담동', '청량리', '해방촌', '경리단길', 'DDP', 'DMC', '강서한강공원', '고척돔', 
'광나루한강공원', '광화문광장', '국립중앙박물관', '용산가족공원', '난지한강공원', '남산공원', 
'노들섬', '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲', '불광천', '서리풀공원', 
'몽마르뜨공원', '서울숲공원', '시청광장', '아차산', '양화한강공원', '어린이대공원', '여의도한강공원', 
'월드컵공원', '응봉산', '이촌한강공원', '잠실종합운동장', '잠실한강공원', '잠원한강공원', '청계산', 
'청와대']
print(len(keywords))



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
                temp_list=item['Key'].split('/')
                date=temp_list[2]
                store_list.append(temp_list[3].split('_')[0])
                file_count += 1
        store_dict[folder]=[date, list(set(store_list)), len(set(store_list))]
    #print(store_dict)
    return store_dict ##{keyword:[date, [store_list]]}

import mysql.connector

# MySQL 연결 설정
config = {
    'user': 
    'password':
    'host':   # 예: '127.0.0.1' 또는 'your-project-id:your-region:your-instance-name'
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


for key in init_dict:
    print(key)
    keyword=key
    date=init_dict[key][0]
    store_list=init_dict[key][1]
    print(date)
    date_object = datetime.strptime(date, "%Y%m%d")
    now_date_str = int(date_object.timestamp())
    for store in store_list:
        data_to_insert = (keyword, store, now_date_str, now_date_str)
        insert_query = "INSERT INTO keyword_stores (keyword, store_id, monthly_update_date, daily_update_date) VALUES (%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))"
        cursor.execute(insert_query, data_to_insert)
    conn.commit()
#커넥션 닫기
cursor.close()
conn.close()

