<h3> The structure of files </h3>

- "queries" directory contains SQL queries
    - "full_...sql" - Query can be applied to whole data in dataset
    - "daily_...sql" - Adapted to daily run and considering that table partitioned by DATE(created_utc)
    - "weekly_...sql" - Adapted to weekly run and considering that table partitioned by DATE(created_utc)
- dags
    - full_tasks.py - to run queries to whole data in dataset
    - daily_tasks.py - to schedule everyday scripts
    - weekly_tasks.py - to schedule every week scripts

<h3> Clarification to task answers </h3>

Answers to the tasks were formulated to the whole dataset in the table. However, if we consider that the initial table
fh-bigquery.reddit.subreddits is huge, then this table will have a partition. In this case, it is probably a daily partition
by the field "created_utc".

- <b>Task 1:</b> By full_task_1 we can formulate the provided structure, if we need to support this table we can use
  daily_task_1.sql, considering that it is partitioned by DATE(created_utc)
    
- <b>Task 2:</b> If we need to create a table by provided schema, we can use the operator "BigQueryCreateEmptyTableOperator". 
  If we need to create the table by writing the result of SQL, we can use the operator "BigQueryOperator"
```python
# Creating Empty Table
CreateTableTask = BigQueryCreateEmptyTableOperator(
		task_id='subreddits_table_task',
		project_id='fh-bigquery',
		dataset_id='reddit',
		table_id='subreddits_task_2',
		schema_fields=[{"name": "dt", "type": "DATE", "mode": "NULLABLE"},
						{"name": "num_comments", "type": "INTEGER", "mode": "NULLABLE"},
						{"name": "posts", "type": "INTEGER", "mode": "NULLABLE"},
						{"name": "ups", "type": "INTEGER", "mode": "NULLABLE"},
						{"name": "downs", "type": "INTEGER", "mode": "NULLABLE"},
						{"name": "subreddit_metrics", "type": "RECORD", "mode": "REPEATED", 
							"fields": [{"name": "subr","type": "STRING", "mode": "NULLABLE"},
										{"name": "num_comments","type": "INTEGER", "mode": "NULLABLE"},
										{"name": "posts", "type": "INTEGER", "mode": "NULLABLE"},
										{"name": "ups", "type": "INTEGER", "mode": "NULLABLE"},
										{"name": "downs", "type": "INTEGER", "mode": "NULLABLE"}]
						}]
	)

# Creating the table by writing the result of SQL
bq_task_1 = BigQueryOperator(
    task_id='subreddits_table_task_2',
    sql='queries/full_task_1.sql',
    destination_dataset_table='fh-bigquery.reddit.subreddits_task_2',
    create_disposition='CREATE_IF_NEEDED',
    allow_large_results=True,
    use_legacy_sql=False
)
```  

- <b>Task 3:</b> By full_task_3 we can formulate the result required in the task. If we need to support this table we can use
  daily_task_3.sql, considering that it is partitioned by DATE(created_utc)

- <b>Task 4:</b> By full_task_4 we can formulate the result required in the task. If we need to support this table we can use
  weekly_task_3.sql, considering that it is partitioned by DATE(created_utc). Also, requirements ask for week-over-week changes.
  
- <b>Task 5:</b> By full_task_5 we can formulate the result required in the task. 
  However, to turn it to a daily basis or weekly we need clarification on the requirement.

