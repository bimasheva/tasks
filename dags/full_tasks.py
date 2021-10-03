from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {'owner': 'airflow', 'start_date': datetime(2021, 10, 4)}

# define the dag
dag = DAG('Full_Dataset_Tasks',
          schedule_interval=None,
          default_args=default_args
          )

# Check initial table not empty
bq_check_task = BigQueryCheckOperator(
    task_id='bq_check_subreddits',
    use_legacy_sql=False,
    sql=f'SELECT count(1) FROM `fh-bigquery.reddit.subreddits`',
    dag=dag)

# Tasks which formulate the result for whole data in table
bq_task_1 = BigQueryOperator(
    task_id='task_1',
    sql='queries/full_task_1.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_1',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

bq_task_3 = BigQueryOperator(
    task_id='task_3',
    sql='queries/full_task_3.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_3',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

bq_task_4 = BigQueryOperator(
    task_id='task_4',
    sql='queries/full_task_4.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_4',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

bq_task_5 = BigQueryOperator(
    task_id='task_5',
    sql='queries/full_task_5.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_5',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)


bq_task_1.set_upstream(bq_check_task)
bq_task_3.set_upstream(bq_check_task)
bq_task_4.set_upstream(bq_check_task)
bq_task_5.set_upstream(bq_check_task)

dag_end = DummyOperator(task_id = 'finish_pipeline', dag=dag)

dag_end.set_upstream([
    bq_task_1,
    bq_task_3,
    bq_task_4,
    bq_task_5
])
