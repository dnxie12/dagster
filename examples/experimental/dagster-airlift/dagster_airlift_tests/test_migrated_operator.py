import os
import signal
import subprocess
import time
from tempfile import TemporaryDirectory
from typing import Any, Generator

import pytest
import requests
from dagster import AssetKey, DagsterInstance, DagsterRunStatus
from dagster._core.test_utils import environ
from dagster._time import get_current_timestamp


@pytest.fixture(name="airflow_home")
def setup_airflow_home() -> Generator[str, None, None]:
    with TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(name="setup")
def setup_fixture(airflow_home: str) -> Generator[str, None, None]:
    # run chmod +x create_airflow_cfg.sh and then run create_airflow_cfg.sh tmpdir
    temp_env = {
        **os.environ.copy(),
        "AIRFLOW_HOME": airflow_home,
        "DAGSTER_URL": "http://localhost:3333",
    }
    # go up one directory from current
    path_to_script = os.path.join(os.path.dirname(__file__), "..", "airflow_setup.sh")
    path_to_dags = os.path.join(os.path.dirname(__file__), "af_migrated_operator", "dags")
    subprocess.run(["chmod", "+x", path_to_script], check=True, env=temp_env)
    subprocess.run([path_to_script, path_to_dags], check=True, env=temp_env)
    with environ({"AIRFLOW_HOME": airflow_home, "DAGSTER_URL": "http://localhost:3333"}):
        yield airflow_home


def dagster_is_ready() -> bool:
    try:
        response = requests.get("http://localhost:3333")
        return response.status_code == 200
    except:
        return False


@pytest.fixture(name="dagster_home")
def setup_dagster_home() -> Generator[str, None, None]:
    with TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(name="dagster_dev")
def setup_dagster(dagster_home: str) -> Generator[Any, None, None]:
    temp_env = {**os.environ.copy(), "DAGSTER_HOME": dagster_home}
    path_to_defs = os.path.join(
        os.path.dirname(__file__), "af_migrated_operator", "dagster_defs.py"
    )
    process = subprocess.Popen(
        ["dagster", "dev", "-f", path_to_defs, "-p", "3333"],
        env=temp_env,
        shell=False,
        preexec_fn=os.setsid,  # noqa
    )
    # Give dagster a second to stand up
    time.sleep(5)

    dagster_ready = False
    initial_time = get_current_timestamp()
    while get_current_timestamp() - initial_time < 60:
        if dagster_is_ready():
            dagster_ready = True
            break
        time.sleep(1)

    assert dagster_ready, "Dagster did not start within 30 seconds..."
    yield process
    os.killpg(process.pid, signal.SIGKILL)


def test_migrated_operator(
    airflow_instance: None, dagster_dev: None, dagster_home: str, airflow_home: str
) -> None:
    """Tests that dagster migrated operator can correctly map airflow tasks to dagster tasks, and kick off executions."""
    response = requests.post(
        "http://localhost:8080/api/v1/dags/the_dag/dagRuns", auth=("admin", "admin"), json={}
    )
    assert response.status_code == 200, response.json()
    # Wait until the run enters a terminal state
    terminal_status = None
    start_time = get_current_timestamp()
    while get_current_timestamp() - start_time < 30:
        response = requests.get(
            "http://localhost:8080/api/v1/dags/the_dag/dagRuns", auth=("admin", "admin")
        )
        assert response.status_code == 200, response.json()
        dag_runs = response.json()["dag_runs"]
        if dag_runs[0]["state"] in ["success", "failed"]:
            terminal_status = dag_runs[0]["state"]
            break
        time.sleep(1)
    assert terminal_status == "success", (
        "Never reached terminal status"
        if terminal_status is None
        else f"terminal status was {terminal_status}"
    )
    with environ({"DAGSTER_HOME": dagster_home}):
        instance = DagsterInstance.get()
        runs = instance.get_runs()
        # The graphql endpoint kicks off a run for each of the tasks in the dag
        assert len(runs) == 2
        some_task_run = [  # noqa
            run
            for run in runs
            if set(list(run.asset_selection)) == {AssetKey(["the_dag__other_task"])}  # type: ignore
        ][0]
        other_task_run = [  # noqa
            run
            for run in runs
            if set(list(run.asset_selection)) == {AssetKey(["the_dag__some_task"])}  # type: ignore
        ][0]
        assert some_task_run.status == DagsterRunStatus.SUCCESS
        assert other_task_run.status == DagsterRunStatus.SUCCESS
