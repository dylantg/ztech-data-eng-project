from __future__ import annotations

from airflow import DAG
# from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
# import requests
import os

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"
# file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "spark-test",
    default_args=default_args,
    schedule_interval=timedelta(1),
    start_date=datetime(now.year, now.month, now.day),
    catchup=False,
)

start = EmptyOperator(
    task_id="start",
    dag=dag
)
#
# @task
# def get_data():
#     # NOTE: configure this as appropriate for your airflow environment
#     data_path = "/opt/airflow/dags/files/employees.csv"
#     os.makedirs(os.path.dirname(data_path), exist_ok=True)
#
#     url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
#
#     response = requests.request("GET", url)
#
#     with open(data_path, "w") as file:
#         file.write(response.text)
#
#     postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
#     conn = postgres_hook.get_conn()
#     cur = conn.cursor()
#     with open(data_path, "r") as file:
#         cur.copy_expert(
#             "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
#             file,
#         )
#     conn.commit()

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/hello-world.py",  # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=True,
    conf={"spark.master": spark_master},
    # application_args=[file_path],
    dag=dag
)

templated_bash_command = """
    echo 'HOSTNAME: localhost'
    cd usr/local/spark/app/
    /home/airflow/.local/bin/spark-submit hello-world.py
"""

submit_spark_task = SSHOperator(
    task_id="SSH_task",
    ssh_conn_id='ssh_default',
    command=templated_bash_command,
    dag=dag
)

print_spark_home = BashOperator(
    task_id="print_spark_home",
    bash_command="echo $SPARK_HOME",
)

print_date = BashOperator(
    task_id="print_date",
    bash_command="date",
)

end = EmptyOperator(task_id="end", dag=dag)

start >> print_spark_home >> spark_job >> end
print_spark_home >> submit_spark_task >> end
# start >> print_date >> end
