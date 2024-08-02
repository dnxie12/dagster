import json
import os
import sys

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def mark_as_dagster_migrating(
    migration_status: dict,
) -> None:
    """Alters all airflow dags in the current context to be marked as migrating to dagster.
    Uses a migration dictionary to determine the status of the migration for each task within each dag.
    Should only ever be the last line in a dag file.
    """
    # get global context from above frame
    global_vars = sys._getframe(1).f_globals  # noqa: SLF001
    globals_to_update = {}
    for var in global_vars:
        if not isinstance(global_vars[var], DAG):
            continue
        dag: DAG = global_vars[var]
        if dag.dag_id in migration_status:
            tags = [
                *dag.tags,
                json.dumps({"dagster_migration": migration_status[dag.dag_id]}),
            ]
            new_dag = DAG(
                dag_id=dag.dag_id,
                description=dag.description,
                schedule_interval=dag.schedule_interval,
                timetable=dag.timetable,
                start_date=dag.start_date,
                end_date=dag.end_date,
                full_filepath=dag.full_filepath,
                template_searchpath=dag.template_searchpath,
                template_undefined=dag.template_undefined,
                user_defined_macros=dag.user_defined_macros,
                user_defined_filters=dag.user_defined_filters,
                default_args=dag.default_args,
                concurrency=dag.concurrency,
                max_active_tasks=dag.max_active_tasks,
                max_active_runs=dag.max_active_runs,
                dagrun_timeout=dag.dagrun_timeout,
                sla_miss_callback=dag.sla_miss_callback,
                default_view=dag.default_view,
                orientation=dag.orientation,
                catchup=dag.catchup,
                on_success_callback=dag.on_success_callback,
                on_failure_callback=dag.on_failure_callback,
                doc_md=dag.doc_md,
                params=dag.params,
                access_control=dag.access_control,
                is_paused_upon_creation=dag.is_paused_upon_creation,
                jinja_environment_kwargs=dag.jinja_environment_kwargs,
                render_template_as_native_obj=dag.render_template_as_native_obj,
                tags=tags,
                owner_links=dag.owner_links,
                auto_register=dag.auto_register,
                fail_stop=dag.fail_stop,
            )
            globals_to_update[var] = new_dag
    global_vars.update(globals_to_update)


ASSET_NODES_QUERY = """
query AssetNodeQuery {
    assetNodes {
        id
        assetKey {
            path
        }
        opName
        jobs {
            id
            name
            repository {
                id
                name
                location {
                    id
                    name
                }
            }
        }
    }
}
"""

TRIGGER_ASSETS_MUTATION = """
mutation LaunchAssetsExecution($executionParams: ExecutionParams!) {
  launchPipelineExecution(executionParams: $executionParams) {
    ... on LaunchRunSuccess {
      run {
        id
        pipelineName
        __typename
      }
      __typename
    }
    ... on PipelineNotFoundError {
      message
      __typename
    }
    ... on InvalidSubsetError {
      message
      __typename
    }
    ... on RunConfigValidationInvalid {
      errors {
        message
        __typename
      }
      __typename
    }
    ...PythonErrorFragment
    __typename
  }
}

fragment PythonErrorFragment on PythonError {
  message
  stack
  errorChain {
    ...PythonErrorChain
    __typename
  }
  __typename
}

fragment PythonErrorChain on ErrorChainLink {
  isExplicitLink
  error {
    message
    stack
    __typename
  }
  __typename
}
"""

# request format
# {
#   "executionParams": {
#     "mode": "default",
#     "executionMetadata": {
#       "tags": []
#     },
#     "runConfigData": "{}",
#     "selector": {
#       "repositoryLocationName": "toys",
#       "repositoryName": "__repository__",
#       "pipelineName": "__ASSET_JOB_0",
#       "assetSelection": [
#         {
#           "path": [
#             "bigquery",
#             "raw_customers"
#           ]
#         }
#       ],
#       "assetCheckSelection": []
#     }
#   }
# }

RUNS_QUERY = """
query RunQuery($runId: ID!) {
	runOrError(runId: $runId) {
		__typename
		...PythonErrorFragment
		...NotFoundFragment
		... on Run {
			id
			status
			__typename
		}
	}
}
fragment NotFoundFragment on RunNotFoundError {
	__typename
	message
}
fragment PythonErrorFragment on PythonError {
	__typename
	message
	stack
	causes {
		message
		stack
		__typename
	}
}
"""


def compute_fn() -> None:
    # https://github.com/apache/airflow/discussions/24463
    os.environ["NO_PROXY"] = "*"
    dag_id = os.environ["AIRFLOW_CTX_DAG_ID"]
    task_id = os.environ["AIRFLOW_CTX_TASK_ID"]
    expected_op_name = f"{dag_id}__{task_id}"
    assets_to_trigger = {}  # key is (repo_location, repo_name, job_name), value is list of asset keys
    # create graphql client
    dagster_url = os.environ["DAGSTER_URL"]
    response = requests.post(f"{dagster_url}/graphql", json={"query": ASSET_NODES_QUERY}, timeout=3)
    for asset_node in response.json()["data"]["assetNodes"]:
        if asset_node["opName"] == expected_op_name:
            repo_location = asset_node["jobs"][0]["repository"]["location"]["name"]
            repo_name = asset_node["jobs"][0]["repository"]["name"]
            job_name = asset_node["jobs"][0]["name"]
            if (repo_location, repo_name, job_name) not in assets_to_trigger:
                assets_to_trigger[(repo_location, repo_name, job_name)] = []
            assets_to_trigger[(repo_location, repo_name, job_name)].append(
                asset_node["assetKey"]["path"]
            )
    print(f"Found assets to trigger: {assets_to_trigger}")  # noqa: T201
    triggered_runs = []
    for (repo_location, repo_name, job_name), asset_keys in assets_to_trigger.items():
        execution_params = {
            "mode": "default",
            "executionMetadata": {"tags": []},
            "runConfigData": "{}",
            "selector": {
                "repositoryLocationName": repo_location,
                "repositoryName": repo_name,
                "pipelineName": job_name,
                "assetSelection": [{"path": asset_key} for asset_key in asset_keys],
                "assetCheckSelection": [],
            },
        }
        print(f"Triggering run for {repo_location}/{repo_name}/{job_name} with assets {asset_keys}")  # noqa: T201
        response = requests.post(
            f"{dagster_url}/graphql",
            json={
                "query": TRIGGER_ASSETS_MUTATION,
                "variables": {"executionParams": execution_params},
            },
            timeout=3,
        )
        run_id = response.json()["data"]["launchPipelineExecution"]["run"]["id"]
        print(f"Launched run {run_id}...")  # noqa: T201
        triggered_runs.append(run_id)
    completed_runs = {}  # key is run_id, value is status
    while len(completed_runs) < len(triggered_runs):
        for run_id in triggered_runs:
            if run_id in completed_runs:
                continue
            response = requests.post(
                f"{dagster_url}/graphql",
                json={"query": RUNS_QUERY, "variables": {"runId": run_id}},
                timeout=3,
            )
            run_status = response.json()["data"]["runOrError"]["status"]
            if run_status in ["SUCCESS", "FAILURE", "CANCELED"]:
                print(f"Run {run_id} completed with status {run_status}")  # noqa: T201
                completed_runs[run_id] = run_status
    non_successful_runs = [
        run_id for run_id, status in completed_runs.items() if status != "SUCCESS"
    ]
    if non_successful_runs:
        raise Exception(f"Runs {non_successful_runs} did not complete successfully.")
    print("All runs completed successfully.")  # noqa: T201
    return None


def build_dagster_migrated_operator(task_id: str, dag: DAG, **kwargs):
    return PythonOperator(task_id=task_id, dag=dag, python_callable=compute_fn, **kwargs)
