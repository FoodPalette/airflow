from airflow import DAG
import difflib
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import logging
import pymysql
from sqlalchemy import create_engine, MetaData, Table, select, text
from utils.slack_Alert import SlackAlert

# slack = SlackAlert("#practice", Variable.get("slack"))

places = {
    "강남구": [
        "강남역",
        "선릉역",
        "신논현역·논현역",
        "역삼역",
        "가로수길",
        "압구정로데오거리",
        "청담동 명품거리",
        "강남 MICE 관광특구",
    ],
    "강동구": ["서울 암사동 유적", "고덕역", "천호역", "광나루한강공원"],
    "강북구": ["미아사거리역", "북한산우이역", "수유역", "4·19 카페거리", "수유리 먹자골목", "북서울꿈의숲"],
    "강서구": ["발산역", "서울식물원·마곡나루역", "김포공항", "강서한강공원"],
    "관악구": ["서울대입구역", "신림역"],
    "광진구": ["건대입구역", "군자역", "뚝섬한강공원", "아차산", "어린이대공원"],
    "구로구": ["구로디지털단지역", "구로역", "남구로역", "대림역", "신도림역", "고척돔"],
    "금천구": ["가산디지털단지역"],
    "도봉구": ["쌍문동 맛집거리", "창동 신경제 중심지"],
    "동대문구": ["장한평역", "회기역", "외대앞", "청량리 제기동 일대 전통시장", "동대문 관광특구"],
    "동작구": ["사당역", "총신대입구(이수)역", "노량진"],
    "마포구": [
        "신촌·이대역",
        "합정역",
        "홍대입구역 9번 출구",
        "연남동",
        "DMC(디지털미디어시티)",
        "난지한강공원",
        "망원한강공원",
        "월드컵공원",
        "홍대 관광특구",
    ],
    "서대문구": ["충정로역", "불광천"],
    "서초구": [
        "고속터미널역",
        "교대역",
        "양재역",
        "방배역 먹자골목",
        "반포한강공원",
        "서리풀공원·몽마르뜨공원",
        "잠원한강공원",
        "청계산",
    ],
    "성동구": ["뚝섬역", "왕십리역", "성수카페거리", "서울숲공원", "응봉산"],
    "성북구": ["성신여대입구역"],
    "송파구": ["장지역", "가락시장", "잠실종합운동장", "잠실한강공원", "잠실 관광특구"],
    "양천구": ["오목교역·목동운동장"],
    "영등포구": ["여의도", "영등포 타임스퀘어", "양화한강공원", "여의도한강공원"],
    "용산구": [
        "삼각지역",
        "서울역",
        "용산역",
        "이태원역",
        "용리단길",
        "이태원 앤틱가구거리",
        "해방촌·경리단길",
        "국립중앙박물관·용산가족공원",
        "노들섬",
        "이촌한강공원",
        "이태원 관광특구",
    ],
    "은평구": ["연신내역"],
    "종로구": [
        "경복궁",
        "광화문·덕수궁",
        "보신각",
        "창덕궁·종묘",
        "동대문역",
        "혜화역",
        "광장(전통)시장",
        "낙산공원·이화마을",
        "북촌한옥마을",
        "서촌",
        "인사동·익선동",
        "광화문광장",
        "청와대",
        "종로·청계 관광특구",
    ],
    "중구": ["덕수궁길·정동길", "DDP(동대문디자인플라자)", "남산공원", "시청광장", "명동 관광특구"],
}

exceptions = {
    "4·19 카페거리": "419카페거리주변",
    "DDP(동대문디자인플라자)": "DDP",
    "DMC(디지털미디어시티)": "DMC",
    "서울 암사동 유적": "암사동",
    "광장(전통)시장": "광장시장",
    "방배역 먹자골목": "방배역",
    "수유리 먹자골목": "수유리",
    "영등포 타임스퀘어": "영등포",
    "이태원 앤틱가구거리": "이태원",
    "창동 신경제 중심지": "창동",
    "청담동 명품거리": "청담동",
    "청량리 제기동 일대 전통시장": "청량리",
}


def get_RDS_engine():
    engine = create_engine(Variable.get("mysql"))
    return engine


def get_Redshift_engine():
    engine = create_engine(Variable.get("redshift"))
    return engine


def etl(schema, table, **kwargs):
    redshift_engine = get_Redshift_engine()
    metadata_redshift = MetaData(bind=redshift_engine)

    rds_engine = get_RDS_engine()
    metadata_rds_con = MetaData(bind=rds_engine, schema=schema)
    metadata_rds_gu = MetaData(bind=metadata_redshift, schema=schema)
    target_table = Table(
        table, metadata_rds_con, autoload_with=rds_engine, autoload=True
    )
    target_table_gu = Table(
        "population_guinfo", metadata_rds_gu, autoload_with=rds_engine, autoload=True
    )
    # Redshift로 직접 쿼리를 사용하여 데이터를 추출하는 코드
    query = """
        SELECT *
        FROM analytics.realtime_population
        WHERE conn_time IN (
            SELECT MAX(conn_time)
            FROM analytics.realtime_population
            GROUP BY area_name
        )
    """
    redshift_data = redshift_engine.execute(query).fetchall()
    logging.info(redshift_data)

    # RDS로 데이터 적재
    for row in redshift_data:
        if (
            row.area_name != "4·19 카페거리"
            and "·" in row.area_name
            and "특구" not in row.area_name
        ):
            area_names = row.area_name.split("·")
            area1, area2 = area_names[0], area_names[1]
            if "역" in area1 and "역" not in area2:
                area2 += "역"
            elif "역" in area2 and "역" not in area1:
                area1 += "역"

            areas = [area1, area2]
            for i in range(2):
                data = {
                    "conn_time": row.conn_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "location": areas[i],  # 신논현역·논현역
                    "area_code": row.area_code,
                    "area_congest": row.area_congest,
                    "area_congest_msg": row.area_congest_msg,
                    "area_population_min": row.area_population_min,
                    "area_population_max": row.area_population_max,
                    "male_rate": row.male_rate,
                    "female_rate": row.female_rate,
                    "fcst_yn": row.fcst_yn,
                }
                execution_date = kwargs.get("execution_date")
                data["request_time"] = execution_date
                data["api_location"] = row.area_name

                gu_name = None
                for key, value in places.items():
                    if data["api_location"] in value:
                        gu_name = key
                        break

                if gu_name == None:
                    logging.warning("서울대공원은 처리되지 않습니다.")
                    continue

                if gu_name is not None:
                    stmt2 = select([target_table_gu.c.id]).where(
                        target_table_gu.c.gu == gu_name
                    )
                    gu_id = rds_engine.execute(stmt2).fetchone()
                    print(gu_id)
                    if gu_id:
                        data["gu_id"] = gu_id[0]
                    else:
                        logging.warning(f"No ID found for gu_name: {gu_name}")

                stmt = select([target_table]).where(
                    target_table.c.location == data["location"]
                )
                result = rds_engine.execute(stmt)

                if result.fetchone() is not None:
                    stmt = (
                        target_table.update()
                        .where(target_table.c.location == data["location"])
                        .values(data)
                    )
                else:
                    stmt = target_table.insert().values(data)

                rds_engine.execute(stmt)

            logging.info("Load done")

        else:
            if row.area_name in exceptions:
                area_name = exceptions[row.area_name]
            else:
                area_name = row.area_name

            data = {
                "conn_time": row.conn_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  
                "location": area_name,
                "area_code": row.area_code,
                "area_congest": row.area_congest,
                "area_congest_msg": row.area_congest_msg,
                "area_population_min": row.area_population_min,
                "area_population_max": row.area_population_max,
                "male_rate": row.male_rate,
                "female_rate": row.female_rate,
                "fcst_yn": row.fcst_yn,
            }
            execution_date = kwargs.get("execution_date")
            data["request_time"] = execution_date
            data["api_location"] = row.area_name

            gu_name = None
            for key, value in places.items():
                if data["api_location"] in value:
                    gu_name = key
                    break

            if gu_name == None:
                logging.warning("서울대공원은 처리되지 않습니다.")
                continue

            if gu_name is not None:
                stmt2 = select([target_table_gu.c.id]).where(
                    target_table_gu.c.gu == gu_name
                )
                gu_id = rds_engine.execute(stmt2).fetchone()
                print(gu_id)
                if gu_id:
                    data["gu_id"] = gu_id[0]
                else:
                    logging.warning(f"No ID found for gu_name: {gu_name}")

            stmt = select([target_table]).where(
                target_table.c.location == data["location"]
            )
            result = rds_engine.execute(stmt)

            if result.fetchone() is not None:
                stmt = (
                    target_table.update()
                    .where(target_table.c.location == data["location"])
                    .values(data)
                )
            else:
                stmt = target_table.insert().values(data)

            rds_engine.execute(stmt)

        logging.info("Load done")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 3, 2),
    "schedule_interval": timedelta(minutes=10),
}

dag = DAG(
    "refresh_tourism_data",
    default_args=default_args,
    description="A DAG to full refresh congestion data in RDS",
    schedule="5,15,25,35,45,55 * * * *",
    catchup=False,
)

etl_task = PythonOperator(
    task_id="etl_task",
    python_callable=etl,
    op_kwargs={"schema": "de41mysql", "table": "population_congestioninfo"},
    provide_context=True,
    dag=dag,
)

etl_task
