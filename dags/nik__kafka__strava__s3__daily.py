from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import logging
import pendulum

logger = logging.getLogger(__name__)
# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'nik__kafka__strava__s3__daily'

#s3
BUCKET_NAME = 'dev'

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
    'catchup': False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    run_extractor = DockerOperator(
        task_id='run_extractor',
        image='test-kafka-to-s3:latest',
        api_version='auto',
        auto_remove=True,
        network_mode='',
        application_args=[
            "--target_date", "{{ ds }}",
            "--topic_name", "your_strava_topic",
            "--broker_name", "kafka:29093",
            "--minio_url", "{{ conn.minios3_conn.extra_dejson.get('endpoint_url', 'http://minio:9000') }}",
            "--minio_access_key", "{{ conn.minios3_conn.access_key }}",
            "--minio_secret_key", "{{ conn.minios3_conn.secret_key }}",
            "--bucket_name", f"{BUCKET_NAME}"
        ]
    )