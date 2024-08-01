import json
import sys
from ..migration_state import AirflowMigrationState
from airflow import DAG


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
