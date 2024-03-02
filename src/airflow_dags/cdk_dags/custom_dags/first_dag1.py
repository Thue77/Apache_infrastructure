# from datetime import datetime

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from pathlib import Path
# import sys

# scripts_path = Path('/opt/bitnami/python/scripts')

# # print(sys.path)

# sys.path.append(str(scripts_path))

# fromthue_dwh.src.common_modules.hello_world import hello_world

# # A DAG represents a workflow, a collection of tasks
# with DAG(dag_id="test-dag", start_date=datetime(2022, 1, 1), schedule="@daily", catchup=False) as dag:

#     # Run Hello World script from apps_path with python operator
#     hello = PythonOperator(task_id="hello", python_callable=hello_world.hello)

#     @task()
#     def airflow():
#         print("airflow")

#     # Set dependencies between tasks
#     hello >> airflow()