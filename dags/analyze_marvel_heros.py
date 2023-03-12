from __future__ import annotations

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from datetime import datetime, timedelta

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
ts = 1678428168
marvel_public_key = Variable.get("marvel_public_key")
marvel_private_key = Variable.get("marvel_private_key")

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
    "analyze-marvel-heroes",
    default_args=default_args,
    schedule="0 21 * * *",  # Run at 9pm UTC
    # schedule_interval=timedelta(1),
    start_date=datetime(now.year, now.month, now.day),
    catchup=False,
)

start = EmptyOperator(
    task_id="start",
    dag=dag
)

get_events = BashOperator(
    task_id='get_events',
    #TODO: Check actual location
    bash_command=f'python /usr/local/spark/app/landing/get_events.py --marvel_public_key {marvel_public_key} --marvel_private_key {marvel_private_key} --ts {ts}',
    dag=dag
)

get_characters = BashOperator(
    task_id='get_characters',
    #TODO: Check actual location
    # bash_command='python /home/airflow/airflow/spark/landing/get_characters.py',
    # bash_command='python /usr/local/spark/app/landing/get_characters.py',
    bash_command=f'''
        python /usr/local/spark/app/landing/get_characters.py \
        --marvel_public_key {marvel_public_key} --marvel_private_key {marvel_private_key} --ts {ts}
    ''',
    dag=dag
)


process_events = SparkSubmitOperator(
    task_id="process_events",
    application="/usr/local/spark/app/silver/process_events.py",
    # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_home",
    verbose=True,
    conf={"spark.master": spark_master},
    # application_args=[file_path],
    dag=dag
)

process_characters = BashOperator(
    task_id='process_characters',
    # bash_command='usr/bin/spark-submit --master spark://spark:7077 /usr/local/spark/app/silver/process_characters.py',
    bash_command="/home/airflow/.local/lib/python3.7/site-packages/pyspark/bin/spark-submit --master spark://spark:7077 /usr/local/spark/app/silver/process_characters.py",
    dag=dag
)
# process_characters = SparkSubmitOperator(
#     task_id="process_characters",
#     application="/usr/local/spark/silver/process_characters.py",
#     # Spark application path created in airflow and spark cluster
#     name=spark_app_name,
#     conn_id="spark_default",
#     verbose=True,
#     conf={"spark.master": spark_master},
#     # application_args=[file_path],
#     dag=dag
# )


# process_characters = SSHOperator(
#     task_id='process_characters',
#     ssh_conn_id='spark_home',
#     command=f'set -a; PYSPARK_PYTHON=python3; /usr/bin/spark-submit --master spark://spark:7077 /usr/local/spark/app/silver/process_characters.py',
#     # command=f'set -a; PYSPARK_PYTHON=python3; /usr/bin/spark-submit --deploy-mode cluster {spark_parameters} /usr/local/spark/silver/process_characters.py',
#     dag=dag
# )



process_gold = SparkSubmitOperator(
    task_id="process_gold",
    application="/usr/local/spark/silver/gold_analysis.py",
    # Spark application path created in airflow and spark cluster
    name=spark_app_name,
    conn_id="spark_default",
    verbose=True,
    conf={"spark.master": spark_master},
    # application_args=[file_path],
    dag=dag
)

plot_data = BashOperator(
    task_id='plot_data',
    #TODO: Check actual location
    bash_command='python /home/airflow/airflow/spark/gold/plot_data.py',
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

echo_spark_home = BashOperator(
    task_id='echo_spark_home',
    bash_command="echo 'SPARK_HOME: ' $SPARK_HOME",
    dag=dag
)
echo_java_home = BashOperator(
    task_id='echo_java_home',
    bash_command="echo 'JAVA_HOME: ' $JAVA_HOME",
    dag=dag
)
echo_path = BashOperator(
    task_id='echo_path',
    bash_command="echo 'PATH: ' $PATH",
    dag=dag
)

java_version = BashOperator(
    task_id='java_version',
    bash_command="java -version",
    dag=dag
)
start >> get_characters >> process_characters >> process_gold
start >> get_events >> process_events >> process_gold
start >> echo_spark_home >> echo_java_home >> echo_path >> java_version >> end
process_gold >> plot_data >> end
