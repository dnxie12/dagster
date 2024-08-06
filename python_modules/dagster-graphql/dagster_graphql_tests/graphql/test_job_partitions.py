from dagster import Definitions, StaticPartitionsDefinition, job, op
from dagster._core.definitions.repository_definition import SINGLETON_REPOSITORY_NAME
from dagster._core.test_utils import ensure_dagster_tests_import, instance_for_test
from dagster_graphql.test.utils import define_out_of_process_context, execute_dagster_graphql

ensure_dagster_tests_import()


GET_PARTITIONS_QUERY = """
  query SingleJobQuery($selector: PipelineSelector!) {
    pipelineOrError(params: $selector) {
      ... on Pipeline {
        id
        name
        partitionKeysOrError {
            partitionKeys
        }
      }
    }
  }

"""


def get_repo_with_partitioned_op_job():
    @op
    def op1(): ...

    @job(partitions_def=StaticPartitionsDefinition(["1", "2"]))
    def job1():
        op1()

    return Definitions(jobs=[job1]).get_repository_def()


def test_get_partition_names():
    repo = get_repo_with_partitioned_op_job()
    with instance_for_test() as instance:
        with define_out_of_process_context(
            __file__, "get_repo_with_partitioned_op_job", instance
        ) as context:
            result = execute_dagster_graphql(
                context,
                GET_PARTITIONS_QUERY,
                variables={
                    "selector": {
                        "repositoryLocationName": context.code_location_names[0],
                        "repositoryName": SINGLETON_REPOSITORY_NAME,
                        "pipelineName": "job1",
                    }
                },
            )
            assert not result

    selector = infer_repository_selector(graphql_context)
    result = execute_dagster_graphql(
        graphql_context,
        GET_PARTITION_SET_QUERY,
        variables={
            "partitionSetName": "integers_partition_set",
            "repositorySelector": selector,
        },
    )

    assert result.data
    snapshot.assert_match(result.data)

    invalid_partition_set_result = execute_dagster_graphql(
        graphql_context,
        GET_PARTITION_SET_QUERY,
        variables={"partitionSetName": "invalid_partition", "repositorySelector": selector},
    )

    assert (
        invalid_partition_set_result.data["partitionSetOrError"]["__typename"]
        == "PartitionSetNotFoundError"
    )
    assert invalid_partition_set_result.data

    snapshot.assert_match(invalid_partition_set_result.data)

    result = execute_dagster_graphql(
        graphql_context,
        GET_PARTITION_SET_QUERY,
        variables={
            "partitionSetName": "dynamic_partitioned_assets_job_partition_set",
            "repositorySelector": selector,
        },
    )

    assert result.data
    snapshot.assert_match(result.data)


def test_get_partition_names_asset_selection():
    pass


def test_get_partition_tags():
    selector = infer_repository_selector(graphql_context)
    result = execute_dagster_graphql(
        graphql_context,
        GET_PARTITION_SET_TAGS_QUERY,
        variables={
            "partitionSetName": "integers_partition_set",
            "repositorySelector": selector,
        },
    )

    assert not result.errors
    assert result.data
    partitions = result.data["partitionSetOrError"]["partitionsOrError"]["results"]
    assert len(partitions) == 1
    sorted_items = sorted(partitions[0]["tagsOrError"]["results"], key=lambda item: item["key"])
    tags = OrderedDict({item["key"]: item["value"] for item in sorted_items})
    assert tags == {
        "foo": "0",
        "dagster/partition": "0",
        "dagster/partition_set": "integers_partition_set",
    }


def test_get_partition_config():
    pass
