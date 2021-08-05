from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

# for some reason, Airflow's macro used like this _{{ ds }}.csv doesn't working
date = datetime.today().strftime("%Y-%m-%d")
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

validate_source_retail_data = BashOperator(
    dag=dag,
    task_id="validate_source_retail_data",
    bash_command="cd /opt/airflow/; \
great_expectations --v3-api checkpoint run retail_source_checkpoint",
)

extract_retail_data = PostgresOperator(
    dag=dag,
    task_id="extract_retail_data",
    sql="./scripts/sql/extract_retail_data.sql",
    postgres_conn_id="postgres_source",
    params={"to_raw": f"/filesystem/raw/retail_profiling_{date}.csv"},
)

# load_retail_data = PythonOperator(

# )

validate_load_retail_data = BashOperator(
    dag=dag,
    task_id="validate_load_retail_data",
    bash_command="cd /opt/airflow/; \
great_expectations --v3-api checkpoint run retail_load_checkpoint",
)

end_of_data_pipeline = DummyOperator(dag=dag, task_id="end_of_data_pipeline")

(
    validate_source_retail_data
    >> extract_retail_data
    >> validate_load_retail_data
    >> end_of_data_pipeline
    # >> load_retail_data
)
