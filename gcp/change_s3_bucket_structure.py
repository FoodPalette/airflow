import boto3
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
    info=[]
    visitor=[]
    reviews=[]
    blogs=[]
    for folder in folder_list:
        folder_path = 'kakao/' + folder + '/'

        file_count = 0
        store_list=[]
        date=""

        paginator = s3.get_paginator('list_objects_v2')
        list1 = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)

        for page in list1:
            for item in page['Contents']:
                # temp_list=item['Key'].split('/')
                # date=temp_list[2]
                #print("!!")
                if "info.json" in item["Key"]:
                    info.append(item['Key'])
                elif "visitor.json" in item["Key"]:
                    visitor.append(item['Key'])
                elif "reviews.json" in item["Key"]:
                    reviews.append(item['Key'])
                elif "blogs.json" in item["Key"]:
                    blogs.append(item["Key"])
                else:
                    print("???????????????")
                    print(temp_list[3])
    print(len(info))
    print(len(visitor))
    print(len(reviews))
    print(len(blogs))


    for element in info:
        print(element)
        #kakao/419카페거리주변/20240226/1008441729_info.json
        copy_source = {'Bucket': 'de-4-1-bucket', 'Key': element}
        #source=/info_type=/year=/month=/day=/location=/{restaurant_id}.json
        element_list=element.split('/')
        year=element_list[2][:4]
        month=element_list[2][4:6]
        day=element_list[2][6:]
        location=element_list[1]
        c_id=element_list[3].split("_")[0]
        new_destination_key=f'spark/info_type=info/year={year}/month={month}/day={day}/location={location}/{c_id}.json'
        destination = {'Bucket': 'de-4-1-bucket', 'Key': new_destination_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination['Bucket'], Key=destination['Key'])
    print("info_succeed")
    
    for element in info:
        print(element)
        #kakao/419카페거리주변/20240226/1008441729_visitor.json
        copy_source = {'Bucket': 'de-4-1-bucket', 'Key': element}
        #source=/info_type=/year=/month=/day=/location=/{restaurant_id}.json
        element_list=element.split('/')
        year=element_list[2][:4]
        month=element_list[2][4:6]
        day=element_list[2][6:]
        location=element_list[1]
        c_id=element_list[3].split("_")[0]
        new_destination_key=f'spark/info_type=visitor/year={year}/month={month}/day={day}/location={location}/{c_id}.json'
        destination = {'Bucket': 'de-4-1-bucket', 'Key': new_destination_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination['Bucket'], Key=destination['Key'])
    print("info_succeed")
    
    for element in reviews:
        print(element)
        #kakao/419카페거리주변/20240226/1008441729_reviews.json
        copy_source = {'Bucket': 'de-4-1-bucket', 'Key': element}
        #source=/info_type=/year=/month=/day=/location=/{restaurant_id}.json
        element_list=element.split('/')
        year=element_list[2][:4]
        month=element_list[2][4:6]
        day=element_list[2][6:]
        location=element_list[1]
        c_id=element_list[3].split("_")[0]
        new_destination_key=f'spark/info_type=reviews/year={year}/month={month}/day={day}/location={location}/{c_id}.json'
        destination = {'Bucket': 'de-4-1-bucket', 'Key': new_destination_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination['Bucket'], Key=destination['Key'])
    print("info_succeed")

    for element in blogs:
        print(element)
        #kakao/419카페거리주변/20240226/1008441729_blogs.json
        copy_source = {'Bucket': 'de-4-1-bucket', 'Key': element}
        #source=/info_type=/year=/month=/day=/location=/{restaurant_id}.json
        element_list=element.split('/')
        year=element_list[2][:4]
        month=element_list[2][4:6]
        day=element_list[2][6:]
        location=element_list[1]
        c_id=element_list[3].split("_")[0]
        new_destination_key=f'spark/info_type=blogs/year={year}/month={month}/day={day}/location={location}/{c_id}.json'
        destination = {'Bucket': 'de-4-1-bucket', 'Key': new_destination_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination['Bucket'], Key=destination['Key'])
    print("info_succeed")
    return store_dict ##{keyword:[date, [store_list]]}

get_keyword_store_info(aws_access_key_id, aws_secret_access_key, region_name)
