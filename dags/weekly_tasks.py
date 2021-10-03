from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {'owner': 'airflow', 'start_date': datetime(2021, 10, 4)}

# define the dag
dag = DAG('Tasks',
          schedule_interval='0 0 * * 1',
          default_args=default_args
          )

# Check initial table has data to execution date
bq_check_task = BigQueryCheckOperator(
    task_id='bq_check_subreddits',
    use_legacy_sql=False,
    sql=f'SELECT 1 FROM `fh-bigquery.reddit.subreddits` WHERE DATE(created_utc) >= "{{ ds }}" AND DATE(created_utc) < "{{ next_ds }}" ',
    dag=dag)

# Running weekly query
bq_task_4 = BigQueryOperator(
    task_id='task_4',
    sql='queries/weekly_task_4.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_4',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

bq_task_4.set_upstream(bq_check_task)

dag_end = DummyOperator(task_id='finish_pipeline', dag=dag)

dag_end.set_upstream(bq_task_4)
