import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dagster_airlift import migrating_to_dagster

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.INFO)
requests_log.propagate = True


def write_to_file_in_airflow_home() -> None:
    airflow_home = os.environ["AIRFLOW_HOME"]
    with open(os.path.join(airflow_home, "airflow_home_file.txt"), "w") as f:
        f.write("Hello")


def write_to_other_file_in_airflow_home() -> None:
    airflow_home = os.environ["AIRFLOW_HOME"]
    with open(os.path.join(airflow_home, "other_airflow_home_file.txt"), "w") as f:
        f.write("Hello")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG(
    "the_dag", default_args=default_args, schedule_interval=None, is_paused_upon_creation=False
)
op_to_migrate = PythonOperator(
    task_id="some_task", python_callable=write_to_file_in_airflow_home, dag=dag
)
op_doesnt_migrate = PythonOperator(
    task_id="other_task", python_callable=write_to_other_file_in_airflow_home, dag=dag
)
# Add a dependency between the two tasks
op_doesnt_migrate.set_upstream(op_to_migrate)

# # set up the debugger
# print("Waiting for debugger to attach...")
# debugpy.listen(("localhost", 7778))
# debugpy.wait_for_client()
migrating_to_dagster(migration_status={"the_dag": {"some_task": True, "other_task": True}})
