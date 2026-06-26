
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
import logging

logger = logging.getLogger(__name__)

DAG_ID = "s3_from_bronze_to_silver"
OWNER = "nbainin"

#conn
PG_HOOK = 'backend_db'

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

def create_log_table_if_not_exists():

    pg_hook = PostgresHook(postgres_conn_id=PG_HOOK)

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS airflow_monitoring")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS airflow_monitoring.tasks_logs (
                dag_id             TEXT,
                task_id            TEXT,
                run_id             TEXT,
                state              VARCHAR(50),
                execution_date     TIMESTAMP,
                duration_seconcds  DOUBLE PRECISION)
            """)
        conn.commit()
        logger.info("Created table")
    finally:
        conn.close()


with DAG(
    dag_id=DAG_ID,
    default_args=args,
    catchup=False,
    schedule="0 7 * * *",
    tags=['API', 'S3', 'GP', 'CH']
) as dag:

    with TaskGroup(group_id='environment') as env_group:
        create_log_table = PythonOperator(
            task_id='create_log_table',
            python_callable=create_log_table_if_not_exists
        )