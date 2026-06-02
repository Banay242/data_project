import logging
import pendulum
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


logger = logging.getLogger(__name__)
# конфиги дага
OWNER = 'n_bainin'
DAG_ID = 'nik__streaming__order_events__daily'
SPARK_CONN = 'spark_default'
MINIO_CONN = "minios3_conn"

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
    schedule=None,
    tags=['spark', 'streaming', 'click']
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    stream_kafka_to_s3_task = SparkSubmitOperator(
        task_id='stream_kafka_to_s3',
        application='/opt/airflow/scripts/spark_jobs__nik/load__nik/load__order_events.py',
        conn_id=SPARK_CONN,
        application_args=[
            "--target_date", "{{ ds }}",
            "--s3_path", f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/stream/order_events/',
            "--kafka_topic", 'backend.public.order_events',
            "--kafka_bootstrap", 'kafka:29093'
        ],
        conf={
            "spark.jars.packages": (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,"
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
            # "spark.executor.instances": "10",
            # "spark.executor.memory": "10g",
            "spark.executor.cores": "5"
        }
    )

    stream_kafka_to_s3_task