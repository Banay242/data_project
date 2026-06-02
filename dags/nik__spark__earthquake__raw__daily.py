import logging
import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

from sqlalchemy.sql.functions import mode

logger = logging.getLogger(__name__)
# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'nik__spark__earthquake__raw__daily'
#spark
SPARK_CONN = "spark_default"
MINIO_CONN = "minios3_conn"

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

with DAG(
    dag_id=DAG_ID,
    schedule='0 6 * * *',
    default_args=args,
    description=SHORT_DESCRIPTION,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    target_date = "{{ data_interval_start.format('YYYY-MM-DD') }}"

    wait_for_s3_task = ExternalTaskSensor(
        task_id='wait_fot_s3',
        external_dag_id='api__earthquake__raw__daily',
        external_task_id=None,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60 * 6,
        execution_delta=pendulum.duration(hours=1),
    )

    run_spark_task = SparkSubmitOperator(
        task_id='run_spark',
        application="/opt/airflow/scripts/spark_jobs__nik/transform__nik/transform__earthquakes.py",
        conn_id=SPARK_CONN,
        application_args=[
            "--target_date", "{{ ds }}",
            "--jdbc-url", "jdbc:clickhouse://{{ conn.clickhouse_dm.host }}:{{ conn.clickhouse_dm.port }}/{{ conn.clickhouse_dm.schema }}",
            "--db-user", "{{ conn.clickhouse_dm.login }}",
            "--db-password", "{{ conn.clickhouse_dm.password }}",
            "--table-name", "earthquakes"
        ],
        conf={
            "spark.jars.packages": (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "org.postgresql:postgresql:42.5.0,"
                "com.clickhouse:clickhouse-jdbc:0.6.5,"
                "org.apache.httpcomponents.client5:httpclient5:5.2.1"
            ),
            "spark.hadoop.fs.s3a.endpoint": f"{{{{ conn.{MINIO_CONN}.extra_dejson.get('endpoint_url', 'http://minio:9000') }}}}",
            "spark.hadoop.fs.s3a.access.key": f"{{{{ conn.{MINIO_CONN}.login }}}}",
            "spark.hadoop.fs.s3a.secret.key": f"{{{{ conn.{MINIO_CONN}.password }}}}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

            "spark.driver.memory": "2g",
            #"spark.executor.instances": "10",
            # "spark.executor.memory": "10g",
            "spark.executor.cores": "5"
        },
        verbose=True
    )

    wait_for_s3_task >> run_spark_task



