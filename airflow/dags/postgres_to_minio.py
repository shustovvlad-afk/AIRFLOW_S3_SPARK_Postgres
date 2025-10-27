from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Final

import pandas as pd
import pendulum
import psycopg2
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator


LOG = logging.getLogger(__name__)

# Default connection settings for the source Postgres instance.
POSTGRES_HOST: Final[str] = os.environ.get("POSTGRES_HOST", "postgres_db")
POSTGRES_PORT: Final[int] = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER: Final[str] = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: Final[str] = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB: Final[str] = os.environ.get("POSTGRES_DB", "postgres")

# MinIO configuration.
MINIO_ENDPOINT: Final[str] = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY: Final[str] = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: Final[str] = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET: Final[str] = os.environ.get("MINIO_BUCKET", "datalake")
MINIO_OBJECT_KEY: Final[str] = os.environ.get(
    "MINIO_OBJECT_KEY", "newtable/newtable.parquet"
)

PARQUET_PATH: Final[Path] = Path("/tmp/newtable.parquet")


def _get_postgres_connection() -> psycopg2.extensions.connection:
    """Return a psycopg2 connection using environment configuration."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def create_newtable_if_missing() -> None:
    """Create the target table in Postgres if it does not already exist."""
    create_sql = "CREATE TABLE IF NOT EXISTS public.newtable (column1 VARCHAR NULL);"
    with _get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

    LOG.info("Ensured public.newtable exists in database %s", POSTGRES_DB)


def export_newtable_to_minio() -> None:
    """Pull data from Postgres, write it to Parquet, and upload to MinIO."""
    with _get_postgres_connection() as conn:
        df = pd.read_sql("SELECT * FROM public.newtable;", conn)

    if df.empty:
        LOG.warning("public.newtable is empty; uploading an empty Parquet file.")

    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    LOG.info("Wrote %d rows to %s", len(df.index), PARQUET_PATH)

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        LOG.info("Created MinIO bucket %s", MINIO_BUCKET)

    minio_client.fput_object(MINIO_BUCKET, MINIO_OBJECT_KEY, str(PARQUET_PATH))
    LOG.info(
        "Uploaded Parquet file to MinIO bucket %s with key %s",
        MINIO_BUCKET,
        MINIO_OBJECT_KEY,
    )


with DAG(
    dag_id="postgres_to_minio",
    description="Create public.newtable in Postgres and export it to MinIO as Parquet.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["postgres", "minio", "parquet", "0.3"],
) as dag:
    create_table = PythonOperator(
        task_id="create_table_if_missing", python_callable=create_newtable_if_missing
    )
    export_table = PythonOperator(
        task_id="export_table_to_minio", python_callable=export_newtable_to_minio
    )

    create_table >> export_table
