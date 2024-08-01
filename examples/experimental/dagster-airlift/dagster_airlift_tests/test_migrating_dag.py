import json
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Generator

import pytest
import requests
from dagster._core.test_utils import environ


@pytest.fixture(name="setup")
def setup_fixture() -> Generator[str, None, None]:
    with TemporaryDirectory() as tmpdir:
        # run chmod +x create_airflow_cfg.sh and then run create_airflow_cfg.sh tmpdir
        temp_env = {**os.environ.copy(), "AIRFLOW_HOME": tmpdir}
        # go up one directory from current
        path_to_script = os.path.join(os.path.dirname(__file__), "..", "airflow_setup.sh")
        path_to_dags = os.path.join(os.path.dirname(__file__), "airflow_migrating_project", "dags")
        subprocess.run(["chmod", "+x", path_to_script], check=True, env=temp_env)
        subprocess.run([path_to_script, path_to_dags], check=True, env=temp_env)
        with environ({"AIRFLOW_HOME": tmpdir}):
            yield tmpdir


def test_migrating_dag(airflow_instance: None) -> None:
    """Test that airflow dags set as migrating have injected migration information."""
    response = requests.get("http://localhost:8080/api/v1/dags/print_dag", auth=("admin", "admin"))
    assert response.status_code == 200
    tags = response.json()["tags"]
    assert len(tags) == 1
    assert json.loads(tags[0]["name"]) == {
        "dagster_migration": {"print_task": True, "downstream_print_task": False}
    }
