from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

spark_master = "spark://spark-master:7077"

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('report_generator',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    report_generator = BashOperator(
        task_id='report_generator',
        bash_command="""
        spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        --executor-memory 1g \
        --driver-memory 1g \
        --name airflow-spark-submit \
        /app/spark.py
        """,
        dag=dag,
    )

    report_generator
