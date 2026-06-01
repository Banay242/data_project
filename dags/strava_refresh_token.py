from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.exceptions import AirflowException
from airflow.models import Variable

import requests
import logging
import pendulum



logger = logging.getLogger(__name__)

# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'strava_refresh_token'

#airflow
VARIABLE_STRAVA = 'strava_cred'

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

def refresh_token():

    try:
        creds = Variable.get(VARIABLE_STRAVA, deserialize_json=True)
    except KeyError:
        raise AirflowException(f"Variable {VARIABLE_STRAVA} is missing")

    payload = {
        'client_id': creds.get('client_id'),
        'client_secret': creds.get('client_secret'),
        'grant_type': 'refresh_token',
        'refresh_token': creds.get('refresh_token'),
    }

    logger.info("sending request for refresh token in Strava API")

    try:
        response = requests.post(
            url='https://www.strava.com/api/v3/oauth/token',
            data=payload,
            timeout=10,
        )

        response.raise_for_status()
        token_data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Error request in Strava API: {e}")
        if response is not None:
            logger.error(f"Server response: {response.text}")
        raise e

    # обновляем access и refresh в Variable
    creds['access_token'] = token_data['access_token']
    creds['refresh_token'] = token_data['refresh_token']

    Variable.set(VARIABLE_STRAVA, creds, serialize_json=True)
    logger.info("Tokens successfully updated")


with DAG(
    dag_id=DAG_ID,
    schedule='0 */5 * * *',
    default_args=args,
    description=SHORT_DESCRIPTION,
    tags=['api', 'strava'],
    catchup=False,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    get_token = PythonOperator(
        task_id='get_token',
        python_callable=refresh_token,
    )