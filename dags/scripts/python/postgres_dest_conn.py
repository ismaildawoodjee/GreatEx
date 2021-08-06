# For GreatEx to connect to tables in the destination PostgresDB

from ruamel import yaml
from pprint import pprint

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

CONNECTION_STRING = "postgresql+psycopg2://destdb1:destdb1@localhost:5434/destdb"  # Use 5432 if running on Airflow, 5434 if testing locally

context = ge.get_context()

datasource_config = {
    "name": "retail_dest",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": CONNECTION_STRING,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": True,  # to specify schema name when calling BatchRequest
        },
    },
}

checkpoint_yaml = """
name: retail_dest_checkpoint
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request:
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
evaluation_parameters: {}
runtime_configuration: {}
validations:
  - batch_request:
      datasource_name: retail_dest
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: public.retail_profiling
      data_connector_query:
        index: -1
    expectation_suite_name: retail_dest_suite
profilers: []
ge_cloud_id:
"""

# pprint(context.get_available_data_asset_names())
# pprint(context.list_expectation_suite_names())

context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="retail_dest",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="public.retail_profiling",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from public.retail_profiling LIMIT 1000"},
    batch_identifiers={
        "default_identifier_name": "First 1000 rows for profiling retail destination data"
    },
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_dest_suite"
)

# print(validator.head())

context.add_checkpoint(**yaml.load(checkpoint_yaml))
context.run_checkpoint(checkpoint_name="retail_dest_checkpoint")
