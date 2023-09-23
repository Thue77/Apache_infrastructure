from __future__ import annotations

import logging
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
log = logging.getLogger(__name__)

test = {
    "spark.jars.packages": 'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-azure:3.3.3'
}

with DAG("landing_electricity_consumption_industry",start_date=datetime(2023,5,27),
    schedule_interval="@daily", catchup=False) as dag:
    run_spark_job = SparkSubmitOperator(
        task_id = "run_spark_job",
        application = "/opt/bitnami/spark/apps/landing/electricity_consumption_industry.py",
        conn_id = "spark_conn",
        verbose = True,
        conf = test
    )