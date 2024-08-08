import time

from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.test_utils import create_run_for_test
from dagster._core.utils import make_new_backfill_id
from dagster._time import get_current_timestamp
from dagster_graphql.implementation.fetch_runs import MegaRunCursor
from dagster_graphql.test.utils import execute_dagster_graphql

from dagster_graphql_tests.graphql.graphql_context_test_suite import (
    ExecutingGraphQLContextTestMatrix,
)

GET_MEGA_RUNS_QUERY = """
query MegaRunsQuery($cursor: String, $limit: Int!) {
    megaRunsOrError(cursor: $cursor, limit: $limit) {
      ... on MegaRuns {
        results {
          results {
            runId
            runStatus
            creationTime
            startTime
            endTime
            jobName
            assetSelection {
                path
            }
            assetCheckSelection {
                name
            }
            tags {
                key
                value
            }
            runType
          }
          cursor
          hasMore
        }
      }
      ... on PythonError {
        stack
        message
      }
    }
}
"""

# when runs are inserted into the database, sqlite uses CURRENT_TIMESTAMP to set the creation time.
# CURRENT_TIMESTAMP only has second precision for sqlite, so if we create runs and backfills without any delay
# the resulting list is a chunk of runs and then a chunk of backfills when ordered by time. Adding a small
# delay between creating a run and a backfill makes the resulting list more interwoven
CREATE_DELAY = 0.5


def _create_run(graphql_context) -> DagsterRun:
    return create_run_for_test(
        instance=graphql_context.instance,
    )


def _create_run_for_backfill(graphql_context, backfill_id: str) -> DagsterRun:
    return create_run_for_test(
        instance=graphql_context.instance,
        tags={
            **DagsterRun.tags_for_backfill_id(backfill_id),
        },
    )


def _create_backfill(graphql_context) -> str:
    backfill = PartitionBackfill(
        backfill_id=make_new_backfill_id(),
        serialized_asset_backfill_data="foo",  # the content of the backfill doesn't matter for testing fetching mega runs
        status=BulkActionStatus.COMPLETED,
        reexecution_steps=None,
        tags=None,
        backfill_timestamp=get_current_timestamp(),
        from_failure=False,
    )
    graphql_context.instance.add_backfill(backfill)
    return backfill.backfill_id


class TestMegaRuns(ExecutingGraphQLContextTestMatrix):
    def test_get_mega_runs(self, graphql_context):
        for _ in range(10):
            _create_run(graphql_context)
            time.sleep(CREATE_DELAY)
            _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        old_cursor = result.data["megaRunsOrError"]["results"]["cursor"]
        assert old_cursor is not None

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": old_cursor,
            },
        )

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]

    def test_get_mega_runs_ignores_backfill_runs(self, graphql_context):
        for _ in range(10):
            _create_run_for_backfill(graphql_context, backfill_id="foo")
            time.sleep(CREATE_DELAY)
            _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]
            assert res["runType"] == "BACKFILL"

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]

    def test_get_mega_runs_inexact_limit(self, graphql_context):
        for _ in range(10):
            _create_run(graphql_context)
            time.sleep(CREATE_DELAY)
            _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 15,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 15
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": result.data["megaRunsOrError"]["results"]["cursor"],
            },
        )

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 5
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]

    def test_get_mega_runs_no_runs_or_backfills_exist(self, graphql_context):
        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 0
        assert not result.data["megaRunsOrError"]["results"]["hasMore"]

    def test_get_mega_runs_cursor_respected(self, graphql_context):
        for _ in range(10):
            _create_run(graphql_context)
            time.sleep(CREATE_DELAY)
            _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None

        old_cursor = MegaRunCursor.from_string(result.data["megaRunsOrError"]["results"]["cursor"])
        run_cursor_run = graphql_context.instance.get_run_record_by_id(old_cursor.run_cursor)
        backfill_cursor_backfill = graphql_context.instance.get_backfill(old_cursor.backfill_cursor)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": old_cursor.to_string(),
            },
        )

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

            assert res["runId"] != old_cursor.run_cursor
            assert res["runId"] != old_cursor.backfill_cursor

            assert res["creationTime"] <= run_cursor_run.create_timestamp.timestamp()
            assert res["creationTime"] <= backfill_cursor_backfill.backfill_timestamp

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]

    def test_get_mega_runs_one_backfill_long_ago(self, graphql_context):
        backfill_id = _create_backfill(graphql_context)
        time.sleep(1)  # to ensure that all runs are more recent than the backfill
        for _ in range(15):
            _create_run(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

            # first 10 results should all be runs
            assert res["runType"] == "RUN"

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        # no backfills have been returned yet, so backfill cursor should be None
        assert (
            MegaRunCursor.from_string(
                result.data["megaRunsOrError"]["results"]["cursor"]
            ).backfill_cursor
            is None
        )

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": result.data["megaRunsOrError"]["results"]["cursor"],
            },
        )

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 6
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        assert (
            MegaRunCursor.from_string(
                result.data["megaRunsOrError"]["results"]["cursor"]
            ).backfill_cursor
            == backfill_id
        )

    def test_get_mega_runs_one_new_backfill(self, graphql_context):
        for _ in range(15):
            _create_run(graphql_context)

        time.sleep(1)  # to ensure that all runs are older than the backfill
        backfill_id = _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 10
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        assert (
            MegaRunCursor.from_string(
                result.data["megaRunsOrError"]["results"]["cursor"]
            ).backfill_cursor
            == backfill_id
        )

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": result.data["megaRunsOrError"]["results"]["cursor"],
            },
        )

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 6
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

            # all remaining results should be runs
            assert res["runType"] == "RUN"

        assert not result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        # even though no backfill was returned, the cursor should point to the backfill that was returned by the previous call
        assert (
            MegaRunCursor.from_string(
                result.data["megaRunsOrError"]["results"]["cursor"]
            ).backfill_cursor
            == backfill_id
        )

    def test_get_mega_runs_backfill_created_between_calls(self, graphql_context):
        # TestMegaRuns::test_get_mega_runs_backfill_created_between_calls[sqlite_with_default_run_launcher_managed_grpc_env]
        for _ in range(10):
            _create_run(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 5,
                "cursor": None,
            },
        )

        assert not result.errors
        assert result.data

        assert len(result.data["megaRunsOrError"]["results"]["results"]) == 5
        prev_run_time = None
        for res in result.data["megaRunsOrError"]["results"]["results"]:
            if prev_run_time:
                assert res["creationTime"] <= prev_run_time
            prev_run_time = res["creationTime"]

        assert result.data["megaRunsOrError"]["results"]["hasMore"]
        assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        assert (
            MegaRunCursor.from_string(
                result.data["megaRunsOrError"]["results"]["cursor"]
            ).backfill_cursor
            is None
        )

        # create a backfill before the next call
        _create_backfill(graphql_context)

        result = execute_dagster_graphql(
            graphql_context,
            GET_MEGA_RUNS_QUERY,
            variables={
                "limit": 10,
                "cursor": result.data["megaRunsOrError"]["results"]["cursor"],
            },
        )

        # TODO - should this next query include the new backfill? Which maybe
        # means that the cursor needs to hold a timestamp as well that we compare results to

        # assert len(result.data["megaRunsOrError"]["results"]["results"]) == 5
        # for res in result.data["megaRunsOrError"]["results"]["results"]:
        #     if prev_run_time:
        #         assert res["creationTime"] <= prev_run_time
        #     prev_run_time = res["creationTime"]

        #     # the newly created backfill should not be returned?
        #     assert res["runType"] == "RUN"

        # assert not result.data["megaRunsOrError"]["results"]["hasMore"]
        # assert result.data["megaRunsOrError"]["results"]["cursor"] is not None
        # assert MegaRunCursor.from_string(result.data["megaRunsOrError"]["results"]["cursor"]).backfill_cursor is None
