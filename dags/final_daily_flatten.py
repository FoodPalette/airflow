import re
import json
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import logging
from airflow.operators.python_operator import PythonOperator

access_key = Variable.get("aws_access_key_id")
secret_key = Variable.get("aws_secret_access_key")
region_name = "ap-northeast-2"


def get_keyword_store_info(
    aws_access_key_id, aws_secret_access_key, region_name, folder_date
):
    bucket_name = "de-4-1-bucket"
    prefix = "kakao/"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    # S3 객체 목록 가져오기
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    folder_list = []
    # 가져온 폴더 목록 출력
    if "CommonPrefixes" in response:
        folders = [prefix.get("Prefix") for prefix in response["CommonPrefixes"]]
        for folder in folders:
            folder = folder.split("/")
            folder_list.append(folder[1])
    else:
        logging.info("No folders found.")

    # 가게별 파일 수 가져오기
    bucket_name = "de-4-1-bucket"
    info = []
    visitor = []
    reviews = []
    blogs = []
    for folder in folder_list:
        folder_path = "kakao/" + folder + "/"
        date = ""

        paginator = s3.get_paginator("list_objects_v2")
        list1 = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)

        for page in list1:
            for item in page["Contents"]:
                temp_list = item["Key"].split("/")
                date = temp_list[2]
                if date == folder_date:
                    if "info.json" in item["Key"]:
                        info.append(item["Key"])
                    elif "visitor.json" in item["Key"]:
                        visitor.append(item["Key"])
                    elif (
                        "reviews.json" in item["Key"] or "comments.json" in item["Key"]
                    ):
                        reviews.append(item["Key"])
                    elif "blogs.json" in item["Key"]:
                        blogs.append(item["Key"])
                    else:
                        logging.info(item["Key"])
    result_dict = {}
    result_dict["info"] = info
    result_dict["visitor"] = visitor
    result_dict["reviews"] = reviews
    logging.info(len(info))
    logging.info(len(visitor))
    logging.info(len(reviews))
    logging.info(len(blogs))
    return result_dict


def flatten_info(y, store_id, location):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                if a == "menus":
                    out["menus"] = ";".join(
                        [
                            f"{item['menu']} ({item['price']}, {item['description']}, {item['img']})"
                            for item in x[a]
                        ]
                    )
                elif a == "facilities":
                    out["facilities"] = ";".join(
                        [k for k, v in x[a].items() if v == "Y"]
                    )
                else:
                    flatten(x[a], name + a + "_")
        elif type(x) is list:
            if name[:-1] in ["tags", "open_hours"]:
                out[name[:-1]] = ";".join(x)
            else:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + "_")
                    i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    out["kakao_id"] = store_id
    out["keyword"] = location
    return out


def flatten_review(y, store_id, location):
    out_list = []  # 하나의 리뷰를 한 행으로 만들기 위해

    if "comment_list" in y:
        for item in y["comment_list"]:
            out = {}
            out["contents"] = item.get("contents", "")
            out["rate"] = item.get("point", "")
            out["created_at"] = item.get("date", "")
            out["my_store_pick"] = item.get("myStorePick", "")
            out["user_comment_count"] = item.get("userCommentCount", "")
            out["user_comment_average_score"] = item.get("userCommentAverageScore", "")
            out["kakaomap_user_id"] = item.get("kakaoMapUserId", "")
            out["kakao_id"] = store_id
            out["keyword"] = location
            if "strengths" in item:
                out["strengths"] = "; ".join(
                    [strength["name"] for strength in item["strengths"]]
                )
            out_list.append(out)

    return out_list


def flatten_visitor(y, store_id, location):
    out = []

    # 'days' 정보 펼치기
    for day, values in y["days"].items():
        for hour, count in enumerate(values):
            out.append(
                {
                    "kakao_id": store_id,
                    "keyword": location,
                    "type": "day",
                    "subtype": day,
                    "hour": hour,
                    "count": count,
                }
            )

    # 'gender' 정보 펼치기
    for label, count in zip(y["gender"]["labels"], y["gender"]["data"]):
        out.append(
            {
                "kakao_id": store_id,
                "keyword": location,
                "type": "gender",
                "subtype": label,
                "count": count,
            }
        )

    # 'age' 정보 펼치기
    for label, count in zip(y["age"]["labels"], y["age"]["data"]):
        out.append(
            {
                "kakao_id": store_id,
                "keyword": location,
                "type": "age",
                "subtype": label,
                "count": count,
            }
        )

    return out


def update_flatten_s3(**kwargs):
    bucket_name = "de-4-1-bucket"

    execution_date = Variable.get('prev_execution_date')
    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z")
    now_date = execution_date.strftime("%Y%m%d")
    # now_date = execution_date.strftime("%Y%m%d")
    logging.info(now_date)
    all_dict = get_keyword_store_info(access_key, secret_key, region_name, now_date)
    logging.info("SUCCESS")
    year = now_date[:4]
    month = now_date[4:6]
    day = now_date[6:]
    # kakao/419카페거리주변/20240226/1008441729_reviews.json
    s3_client = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    s3_dest = boto3.resource(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )

    for file_name in all_dict["info"]:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        key = file_name.split("/")
        store_id = key[3].split("_")[0]
        location = key[1]

        # # JSON 파일 flatten
        flattened_data = [flatten_info(item, store_id, location) for item in data]

        # flatten된 JSON 파일을 다른 S3 버킷에 저장
        flattened_key = f"flatten/info_type=info/year={year}/month={month}/day={day}/location={location}/{store_id}.json"
        s3_dest.Object("de-4-1-glue-test", flattened_key).put(
            Body=json.dumps(flattened_data)
        )
    logging.info("info_success")

    for file_name in all_dict["visitor"]:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response["Body"].read().decode("utf-8")
        if content == "" or content == [{}]:
            continue
        else:
            data = json.loads(content)
            if data != [{}]:
                key = file_name.split("/")
                store_id = key[3].split("_")[0]
                location = key[1]
                # JSON 파일 flatten
                flattened_data = [
                    flatten_visitor(item, store_id, location) for item in data
                ]
                # flatten된 JSON 파일을 다른 S3 버킷에 저장
                flattened_key = f"flatten/info_type=visitor/year={year}/month={month}/day={day}/location={location}/{store_id}.json"
                s3_dest.Object("de-4-1-glue-test", flattened_key).put(
                    Body=json.dumps(flattened_data[0])
                )
    logging.info("visitor_success")

    for file_name in all_dict["reviews"]:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response["Body"].read().decode("utf-8")
        if content == "" or content == [{}]:
            continue
        else:
            data = json.loads(content)
            if data != [{}]:
                key = file_name.split("/")
                store_id = key[3].split("_")[0]
                location = key[1]

                # # JSON 파일 flatten
                flattened_data = [
                    flatten_review(item, store_id, location) for item in data
                ]
                # flatten된 JSON 파일을 다른 S3 버킷에 저장
                flattened_key = f"flatten/info_type=reviews/year={year}/month={month}/day={day}/location={location}/{store_id}.json"
                s3_dest.Object("de-4-1-glue-test", flattened_key).put(
                    Body=json.dumps(flattened_data[0])
                )
    logging.info("reviews_success")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 19),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}

# Create a DAG instance
dag = DAG(
    "daily_flatten_dag_v3",
    default_args=default_args,
    description="ELT process",
    catchup=False,
)

# Define PythonOperators for each task
update_flatten_task = PythonOperator(
    task_id="daily_flatten_task",
    python_callable=update_flatten_s3,
    provide_context=True,
    dag=dag,
)

update_flatten_task
