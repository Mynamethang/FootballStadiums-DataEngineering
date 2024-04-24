from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_world():
    print("Hello, world!")
# Define the default arguments for the DAG
default_args = {
    "owner": "thangnguyen",
    "start_date": datetime(2023, 11, 2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG with a daily schedule
with DAG(
    dag_id="example_python_operators",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Create a task that calls the hello_world function
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world,
    )