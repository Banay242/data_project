import json
import pendulum
import requests
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


logger = logging.getLogger(__name__)

# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'api__earthquake__raw__daily'

# s3
BUCKET_NAME = "dev"
MINIO_CONN_ID = "minios3_conn"


LONG_DESCRIPTION = """
"""

SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
    'catchup': True,
}

def get_api(**context):

    start_date = context['data_interval_start'].format('YYYY-MM-DD')
    end_date = context['data_interval_end'].format('YYYY-MM-DD')

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date
    }
    response = requests.get(url, params, timeout=30)

    response.raise_for_status()

    data = response.json()

    s3_key = f"raw-data/earthquakes/{context['data_interval_start'].format('YYYY/MM/DD')}/data.json"

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    s3_hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False),
        bucket_name=BUCKET_NAME,
        key=s3_key,
        replace=True
    )

    logger.info(f"Successfully uploaded data {len(data['features'])} records to s3://{BUCKET_NAME}/{s3_key}")

    return s3_key


with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule='0 5 * * *',
    catchup=True,
    tags=['api', 'stg', 'earthquake'],
    description=SHORT_DESCRIPTION,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    get_api_task = PythonOperator(
        task_id='get_api',
        python_callable=get_api
    )

    get_api_task