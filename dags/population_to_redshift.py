from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta
import time

import requests
import logging
import json

tourism_spots = [
    '강남 MICE 관광특구', '동대문 관광특구', '명동 관광특구', '이태원 관광특구', '잠실 관광특구',
    '종로·청계 관광특구', '홍대 관광특구', '경복궁', '광화문·덕수궁', '보신각', '서울 암사동 유적',
    '창덕궁·종묘', '가산디지털단지역', '강남역', '건대입구역', '고덕역', '고속터미널역', '교대역',
    '구로디지털단지역', '구로역', '군자역', '남구로역', '대림역', '동대문역', '뚝섬역', '미아사거리역',
    '발산역', '북한산우이역', '사당역', '삼각지역', '서울대입구역', '서울식물원·마곡나루역', '서울역',
    '선릉역', '성신여대입구역', '수유역', '신논현역·논현역', '신도림역', '신림역', '신촌·이대역',
    '양재역', '역삼역', '연신내역', '오목교역·목동운동장', '왕십리역', '용산역', '이태원역', '장지역',
    '장한평역', '천호역', '총신대입구(이수)역', '충정로역', '합정역', '혜화역', '회기역', '4·19 카페거리',
    '가락시장', '가로수길', '광장(전통)시장', '김포공항', '낙산공원·이화마을', '노량진', '덕수궁길·정동길',
    '방배역 먹자골목', '북촌한옥마을', '서촌', '성수카페거리', '수유리 먹자골목', '쌍문동 맛집거리',
    '압구정로데오거리', '여의도', '연남동', '영등포 타임스퀘어', '외대앞', '용리단길', '이태원 앤틱가구거리',
    '인사동·익선동', '창동 신경제 중심지', '청담동 명품거리', '청량리 제기동 일대 전통시장',
    '해방촌·경리단길', 'DDP(동대문디자인플라자)', 'DMC(디지털미디어시티)', '강서한강공원', '고척돔',
    '광나루한강공원', '광화문광장', '국립중앙박물관·용산가족공원', '난지한강공원', '남산공원', '노들섬',
    '뚝섬한강공원', '망원한강공원', '반포한강공원', '북서울꿈의숲', '불광천', '서리풀공원·몽마르뜨공원',
    '서울대공원', '서울숲공원', '아차산', '양화한강공원', '어린이대공원', '여의도한강공원', '월드컵공원',
    '응봉산', '이촌한강공원', '잠실종합운동장', '잠실한강공원', '잠원한강공원', '청계산', '청와대'
]

def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        conn_time datetime,
                        area_name varchar(50),
                        area_code varchar(20),
                        area_congest varchar(20),
                        area_congest_msg varchar(1000),
                        area_population_min integer,
                        area_population_max integer,
                        male_rate float,
                        female_rate float,
                        fcst_yn varchar(10),
                        created_date timestamp default CURRENT_TIMESTAMP,
                        PRIMARY KEY (area_name, conn_time)
                    );""")


@task
def etl(schema, table):
    service_key = Variable.get('service_key')

    cur = get_Redshift_connection()
    for area in tourism_spots:
        url = f'http://openapi.seoul.go.kr:8088/{service_key}/json/citydata_ppltn/1/5/{area}'
        response = requests.get(url=url)
        logging.info(f"API Response:{response.text}")

        try:
            data = json.loads(response.text)
            records = data['SeoulRtd.citydata_ppltn']

            cur = get_Redshift_connection()
            _create_table(cur, schema, table, False)

            cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")

            for d in records:
                day = datetime.strptime(d["PPLTN_TIME"], '%Y-%m-%d %H:%M')
                sql = (f"""INSERT INTO t VALUES 
                    ('{day}', '{d["AREA_NM"]}', '{d["AREA_CD"]}', 
                    '{d["AREA_CONGEST_LVL"]}', '{d["AREA_CONGEST_MSG"]}', {d["AREA_PPLTN_MIN"]}, 
                    {d["AREA_PPLTN_MAX"]}, {d["MALE_PPLTN_RATE"]}, {d["FEMALE_PPLTN_RATE"]}, '{d["FCST_YN"]}');
                    """)
                cur.execute(sql)

            _create_table(cur, schema, table, True)
            cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f"Error in ETL process: {str(e)}")
            time.sleep(5)
            raise
        logging.info("load done")


with DAG(
        dag_id='Realtime_Population_to_Redshift',
        start_date=datetime(2024, 3, 5),
        schedule='*/10 * * * *',
        catchup=False,
        default_args={
            'retries': 3,
            'retry_delay': timedelta(minutes=1),
            'dagrun_timeout': timedelta(hours=1),
        }
) as dag:
    etl("analytics", "realtime_population")