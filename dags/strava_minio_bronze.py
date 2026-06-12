import time
import pendulum
import logging
import requests
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

OWNER = "n_bainin"
DAG_ID = "strava_minio_bronze"
# airflow.variables
VARIABLES_STRAVA = 'strava_cred'
# s3
MINIO_CONN_ID = "minios3_conn"
BUCKET_NAME = "dev"

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

def fetch_and_save_activities(**context):

    dag_run_conf = context['dag_run'].conf

    if dag_run_conf and 'start_date' in dag_run_conf and 'end_date' in dag_run_conf:
        start_date = pendulum.parse(dag_run_conf['start_date']).in_timezone('Europe/Moscow')
        end_date = pendulum.parse(dag_run_conf['end_date']).in_timezone('Europe/Moscow')
        logging.info(f"Manual backflip. Interval: {start_date} -> {end_date}")
    else:
        start_date = context['data_interval_start']
        end_date = context['data_interval_end']
        logging.info("Schedule")
    try:
        creds = Variable.get(VARIABLES_STRAVA, deserialize_json=True)
        access_token = creds.get('access_token')
    except Exception as e:
        logging.error(f"Failed get access_token from airflow.variables: {e}")
        raise e

    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {'Authorization': 'Bearer ' + access_token}
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    current_date = start_date
    with requests.Session() as session:
        while current_date < end_date:
            next_date = current_date.add(days=1)

            params = {
                'after': int(current_date.timestamp()),
                'before': int(next_date.timestamp()),
                'page': 1,
                'per_page': 100
            }

            try:
                response = session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=10
                )

                if response.status_code == 429:
                    now = pendulum.now('Europe/Moscow')

                    minutes_to_wait = 15 - (now.minute % 15)
                    sleep_seconds = minutes_to_wait * 60 - now.second + 15
                    logging.warning(f"[429] Strava {now.format('HH:mm:ss')} Moscow")
                    time.sleep(sleep_seconds)
                    continue

                response.raise_for_status()
                activities = response.json()

            except requests.exceptions.RequestException as e:
                raise e

            if activities:
                try:
                    s3_key = f"raw/strava_batch/{current_date.format('YYYY/MM/DD')}/data.json"

                    s3_hook.load_string(
                        string_data=json.dumps(activities),
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True
                    )

                    logging.info(f"Successfully loaded {len(activities)} activities to s3://{BUCKET_NAME}/{s3_key}")

                except Exception as e:
                    raise e

            current_date = next_date

            if dag_run_conf:
                time.sleep(1)

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule='0 0 * * *',
    description=SHORT_DESCRIPTION,
    max_active_runs=1,
    catchup=False,
    tags=['strava', 'minio', 'batch'],
    params={
            "start_date": Param(
                default="2026-01-01",
                type="string",
                format="date",
                description="Дата начала выгрузки в формате YYYY-MM-DD"
            ),
            "end_date": Param(
                default="2026-06-01",
                type="string",
                format="date",
                description="Дата окончания выгрузки в формате YYYY-MM-DD"
            ),
    }
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    fetch_and_save_activities_t = PythonOperator(
        task_id='fetch_and_save_activities',
        python_callable=fetch_and_save_activities,
    )

