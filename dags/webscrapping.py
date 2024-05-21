from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from includes.main import ingestion_start

default_args = {
    'owner': 'svc',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='Redfin_Webscraping_ETL',
        default_args=default_args,
        start_date=datetime(2024, 5, 14, 1, 30),
        schedule_interval='@daily'
) as dag:
    task_1 = PythonOperator(
        task_id='Ingest_Data_from_Redfin',
        python_callable=ingestion_start
    )

    task_2 = SparkSubmitOperator(
        task_id='Process_and_Send_Data_to_DB',
        conn_id='spark_default',
        application='dags/includes/jobs/spark_consumer.py',
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                 "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                 "org.apache.kafka:kafka-clients:3.5.1,"
                 "commons-logging:commons-logging:1.2"
    )
    [task_1, task_2]
