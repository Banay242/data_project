from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import os

DAG_ID = "strava_minio_ch_silver"
OWNER = "nbainin"
#spark
SPARK_CONN = "spark_default"
MINIO_CONN = "minios3_conn"
CLICK_CONN = "clickhouse_conn"

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    default_args=args,
    description=SHORT_DESCRIPTION,
    tags=['minio', 'spark', 'silver'],
    max_active_runs=1,
    catchup=False
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    process_data_t = SparkSubmitOperator(
        task_id='process_data',
        conn_id=SPARK_CONN,
        application='/opt/airflow/scripts/spark_jobs__nik/transform__nik/transform__strava_activities.py',
        application_args=[
            "--ch_user", f"{{{{ conn.{CLICK_CONN}.login }}}}",
            "--ch_password", f"{{{{ conn.{CLICK_CONN}.password }}}}",
            "--ch_table", "strava_activities",
        ],
        conf = {
            "spark.jars.packages": (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "com.clickhouse:clickhouse-jdbc:0.4.6"
            ),
            "spark.hadoop.fs.s3a.endpoint": f"{{{{ conn.{MINIO_CONN}.extra_dejson.get('endpoint_url', 'http://minio:9000') }}}}",
            "spark.hadoop.fs.s3a.access.key": f"{{{{ conn.{MINIO_CONN}.login }}}}",
            "spark.hadoop.fs.s3a.secret.key": f"{{{{ conn.{MINIO_CONN}.password }}}}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.credentials.provider": "true"
        }
    )

    process_data_t

