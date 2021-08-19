from utils import *

# Fill in information for the fields specified in all-caps
DATASOURCE_NAME = "retail_transform"
EXPECTATION_SUITE_NAME = f"{DATASOURCE_NAME}_suite"
CHECKPOINT_NAME = f"{DATASOURCE_NAME}_checkpoint"

# Data Asset fields
BASE_DIRECTORY = "../filesystem/stage"  # again, relative to GE YAML file
GROUP_NAMES = [
    "data_asset_name",
    "year",
    "month",
    "day",
    "parquet_compression",
]
REGEX_PATTERN = "(.*)(-\d{4})-(\d{2})-(\d{2})\.(.*)\.parquet"
DATA_ASSET_NAME = "retail_profiling"

datasource_config = {
    "name": DATASOURCE_NAME,
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": BASE_DIRECTORY,
            "default_regex": {
                "group_names": GROUP_NAMES,
                "pattern": REGEX_PATTERN,
            },
        },
    },
}

batch_request = BatchRequest(
    datasource_name=DATASOURCE_NAME,
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=DATA_ASSET_NAME,
)

checkpoint_config = f"""\
name: {CHECKPOINT_NAME}
config_version: 1
class_name: Checkpoint
validations:
- batch_request:
    datasource_name: {DATASOURCE_NAME}
    data_connector_name: {data_connector_name}
    data_asset_name: {DATA_ASSET_NAME}
    data_connector_query:
        index: -1
  expectation_suite_name: {EXPECTATION_SUITE_NAME}
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
{VALIDATION_ACTION}
"""  # the validation action defined in utils.py


def connect_to_datasource():
    """Connects to stage data in the filesystem/stage directory, and adds
    configuration to YAML file if successful.
    """

    try:
        context.test_yaml_config(yaml.dump(datasource_config))

        # empty Expectation Test Suite, its only purpose is to validate the Datasource connection
        context.create_expectation_suite(
            expectation_suite_name="test_suite", overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )
        print(validator.head())

    except IndexError as ex:  # very common error I got
        logging.exception(
            f"""Unmatched data references are not available for connection.\
Ensure that your base directory: "{BASE_DIRECTORY}", group names "{GROUP_NAMES}",\
and regex pattern "{REGEX_PATTERN}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{BASE_DIRECTORY}"'
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added stage data file config to `great_expectations.yml` file")


def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )
    # Add Expectations here
    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            "invoice_number",
            "stock_code",
            "detail",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country",
        ]
    )
    validator.expect_column_min_to_be_between(
        column="quantity", min_value=0, max_value=0
    )
    validator.expect_column_values_to_not_be_null(column="quantity")
    # Save Expectations to JSON
    validator.save_expectation_suite(discard_failed_expectations=False)


def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    # result = context.run_checkpoint(checkpoint_name)
    # print(f'Successful checkpoint validation: {result["success"]}\n')


connect_to_datasource()
create_expectation_suite(expectation_suite_name=EXPECTATION_SUITE_NAME)
create_checkpoint(checkpoint_name=CHECKPOINT_NAME)
