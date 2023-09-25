from __future__ import annotations

import logging
from pathlib import Path
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset
from datetime import datetime
log = logging.getLogger(__name__)

spark_conf = {
    "spark.jars.packages": 'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-azure:3.3.3'
}

landing_dataset = Dataset(Path(__file__).stem)

with DAG("landing_electricity_consumption_industry",start_date=datetime(2023,5,27),
    schedule_interval="@daily", 
    description = "Spark job to load electricity consumption by industry from Energi Data Service API(https://www.energidataservice.dk/) to landing",
    catchup=False) as dag:


    api_to_landing = SparkSubmitOperator(
        task_id = "api_to_landing",
        application = "/opt/bitnami/spark/apps/landing/electricity_consumption_industry.py",
        conn_id = "spark_conn",
        verbose = True,
        outlets = [landing_dataset],
        conf = spark_conf
    )