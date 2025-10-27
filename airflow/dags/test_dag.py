"""Simple test DAG to validate the Airflow installation."""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def _print_message() -> None:
    """Log a message so we can see the DAG ran."""
    print("Test DAG executed")  # noqa: T201


with DAG(
    dag_id="test_dag",
    description="Simple test DAG to verify the Airflow setup.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")
    print_message = PythonOperator(
        task_id="print_message", python_callable=_print_message
    )

    start >> print_message
