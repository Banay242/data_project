from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import pendulum
import logging
import tempfile
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq


OWNER = "n_bainin"
DAG_ID = "postgres_to_bronze_s3"

#airflow connections
POSTGRES_CONN = 'backend_db'
S3_CONN = 'minios3_conn'

#s3
BACKET_NAME='dev'

LONG_DESCRIPTION = """
"""
SHORT_DESCRIPTION = ""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 5, 1, tz='Europe/Moscow'),
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=1),
}

def export_table_to_s3(table_name, **context):
    logical_date = context['ds']

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    s3_hook = S3Hook(aws_conn_id=S3_CONN)

    s3_path = f"bronze/{table_name}/dt={logical_date}/{table_name}_raw.parquet"

    logging.info(f"Starting to export data of table {table_name} for {logical_date}")

    engine = pg_hook.get_sqlalchemy_engine()

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file_path = os.path.join(tmp_dir, f"{table_name}_raw.parquet")

        chunck_size = 50000

        query = f"select * from {table_name}"

        first_chunk = True
        writer = None

        for chunk in pd.read_sql(query, engine, chunksize=chunck_size):
            if 'app_id' in chunk.columns:
                chunk['app_id'] = chunk['app_id'].astype(str)
            if 'metadata' in chunk.columns:
                chunk['metadata'] = chunk['metadata'].astype(str)

            table = pa.Table.from_pandas(chunk)

            if first_chunk:
                writer = pq.ParquetWriter(tmp_file_path, table.schema, compression="snappy")
                first_chunk = False

            writer.write_table(table)
            logging.info(f"Exported data chunk of size {len(chunk)} to Parquet")

        if writer:
            writer.close()

        logging.info(f"Uploading {tmp_file_path} to S3 bucket {s3_path}")
        s3_hook.load_file(
            filename=tmp_file_path,
            key=s3_path,
            bucket_name=BACKET_NAME,
            replace=True
        )

    logging.info(f"Finished exporting data of table {table_name} for {logical_date}")


with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule="0 5 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["minio", "postgres", "bronze"],
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    extract_products_t = PythonOperator(
        task_id="extract_products",
        python_callable=export_table_to_s3,
        op_kwargs={"table_name": "ods.products"},
        provide_context=True,
    )

    extract_applications_t = PythonOperator(
        task_id="extract_applications",
        python_callable=export_table_to_s3,
        op_kwargs={"table_name": "ods.applications"},
        provide_context=True,
    )

    [extract_products_t, extract_applications_t]