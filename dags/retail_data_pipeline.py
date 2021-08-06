from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from transformations import transform_raw_data, root_path, date

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime.now(),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    dag_id="retail_data_pipeline",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    max_active_runs=1,
    # user_defined_macros={
    #     "ds": lambda ds: dict(zip(["year", "month", "day"], ds.split("-")))
    # },
)

validate_retail_source_data = BashOperator(
    dag=dag,
    task_id="validate_retail_source_data",
    bash_command="cd /opt/airflow/; \
great_expectations --v3-api checkpoint run retail_source_checkpoint",
)

extract_load_retail_source = PostgresOperator(
    dag=dag,
    task_id="extract_load_retail_source",
    sql="./scripts/sql/extract_retail_data.sql",
    postgres_conn_id="postgres_source",
    params={"to_raw": f"/filesystem/raw/retail_profiling-{date}.csv"},
)

validate_retail_raw_data = BashOperator(
    dag=dag,
    task_id="validate_retail_raw_data",
    bash_command="cd /opt/airflow/; \
great_expectations --v3-api checkpoint run retail_load_checkpoint",
)

transform_load_retail_raw = PythonOperator(
    dag=dag,
    task_id="transform_load_retail_raw",
    python_callable=transform_raw_data,
    op_kwargs={
        "output_loc": f"{root_path}/filesystem/stage/retail_profiling-{date}.snappy.parquet"
    },
)

validate_retail_stage_data = BashOperator(
    dag=dag,
    task_id="validate_retail_stage_data",
    bash_command="cd /opt/airflow/; \
great_expectations --v3-api checkpoint run retail_transform_checkpoint",
)

end_of_data_pipeline = DummyOperator(dag=dag, task_id="end_of_data_pipeline")

(
    validate_retail_source_data
    >> extract_load_retail_source
    >> validate_retail_raw_data
    >> transform_load_retail_raw
    >> validate_retail_stage_data
    # >> transform_load_retail_stage (or just) load_retail_stage
    # >> validate_retail_warehouse_data
    # >> transform_load_retail_warehouse
    # >> validate_retail_dest_data
    >> end_of_data_pipeline
)
