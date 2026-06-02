import logging
import pendulum
import requests
import json

from kafka import KafkaProducer
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'strava_kafka'

#airflow
VARIABLE_STRAVA = 'strava_cred'

#kafka
TOPIC_NAME = 'strava_kafka'

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

def fetch_push(**context):
    try:
        creds = Variable.get(VARIABLE_STRAVA, deserialize_json=True)
        access_token = creds.get('access_token')
    except KeyError as e:
        raise e

    start_dt = context['data_interval_start']
    after_timestamp = int(start_dt.timestamp())

    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {'Authorization': 'Bearer ' + access_token}
    params = {
        'after': after_timestamp,
        'per_page': 100,
    }

    try:
        response = requests.get(url,
                                headers=headers,
                                params=params,
                                timeout=15)

        response.raise_for_status()
        activities = response.json()

    except requests.exceptions.RequestException as e:
        raise e

    logger.info(f"Got {len(activities)} activities")

    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
    except Exception as e:
        raise AirflowException(f"Failed to find the connect to Kafka: {e}")

    for activity in activities:
        activity_id = str(activity.get('id'))

        try:
            producer.send(
                topic=TOPIC_NAME,
                key=activity_id.encode('utf-8'),
                value=activity
            )
            logger.info(f"Pushed activity {activity_id}")
        except Exception as e:
            logger.error(f"Failed to push activity {activity_id}: {e}")

    producer.flush()
    producer.close()
    logger.info("Successfully pushed all activities")



with DAG(
    dag_id=DAG_ID,
    schedule='0 6 * * *',
    default_args=args,
    description=SHORT_DESCRIPTION,
    max_active_runs=1,
    catchup=False,
    tags=['strava', 'kafka', 'api'],
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    fetch_push_t = PythonOperator(
        task_id='fetch',
        python_callable=fetch_push,
    )

