from utils import *

# Fill in information for the fields specified in all-caps
DATASOURCE_NAME = "retail_load"
EXPECTATION_SUITE_NAME = f"{DATASOURCE_NAME}_suite"
CHECKPOINT_NAME = f"{DATASOURCE_NAME}_checkpoint"

# Data Asset fields
BASE_DIRECTORY = "../filesystem/raw"  # relative to GE YAML file
GROUP_NAMES = ["data_asset_name", "year", "month", "day"]
REGEX_PATTERN = "(.*)(-\d{4})-(\d{2})-(\d{2})\.csv"
DATA_ASSET_NAME = "retail_profiling"
READER_METHOD = "read_csv"

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
    batch_spec_passthrough={
        "reader_method": READER_METHOD,
        "reader_options": {"nrows": 1000},
    },
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
    """Connects to raw data in the filesystem/raw directory, and adds
    configuration to YAML file if successful.
    """

    try:
        # connect to raw data and sample out 1000 rows from the CSV file
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
        logging.info("Added raw data file config to `great_expectations.yml` file")


def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )
    # Add Expectations here:
    # Table-Level Expectations
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
    validator.expect_table_row_count_to_be_between(max_value=300000, min_value=250000)
    # Column-level Expectations
    # invoice_number
    validator.expect_column_values_to_not_be_null(column="invoice_number")
    validator.expect_column_values_to_be_in_type_list(
        column="invoice_number", type_list=["str"]
    )
    # stock_code
    validator.expect_column_values_to_not_be_null(column="stock_code")
    validator.expect_column_values_to_be_in_type_list(
        column="stock_code", type_list=["str"]
    )
    # detail
    validator.expect_column_values_to_not_be_null(column="detail", mostly=0.99)
    validator.expect_column_values_to_be_in_type_list(
        column="detail", type_list=["str"]
    )
    # quantity
    validator.expect_column_min_to_be_between(
        column="quantity", min_value=0, max_value=0
    )
    validator.expect_column_max_to_be_between(
        column="quantity", max_value=100000, min_value=50000
    )
    validator.expect_column_values_to_not_be_null(column="quantity")
    validator.expect_column_values_to_be_in_type_list(
        column="quantity", type_list=["int64"]
    )
    # invoice_date
    validator.expect_column_values_to_not_be_null(column="invoice_date")
    validator.expect_column_values_to_match_strftime_format(
        column="invoice_date", strftime_format="%Y-%m-%d %H:%M:%S"
    )
    validator.expect_column_values_to_be_in_type_list(
        column="invoice_date", type_list=["str"]
    )
    # unit_price
    validator.expect_column_min_to_be_between(
        column="unit_price", max_value=0.0, min_value=0.0
    )
    validator.expect_column_max_to_be_between(
        column="unit_price", max_value=45000.00, min_value=35000.00
    )
    validator.expect_column_values_to_not_be_null(column="unit_price")
    validator.expect_column_values_to_be_in_type_list(
        column="unit_price", type_list=["float64"]
    )
    # customer_id
    validator.expect_column_values_to_not_be_null(column="customer_id", mostly=0.7)
    validator.expect_column_values_to_be_in_type_list(
        column="customer_id", type_list=["float64"]
    )
    # country
    validator.expect_column_values_to_be_in_set(
        column="country",
        value_set=[
            "Australia",
            "Austria",
            "Bahrain",
            "Belgium",
            "Brazil",
            "Canada",
            "Channel Islands",
            "Cyprus",
            "Czech Republic",
            "Denmark",
            "EIRE",
            "European Community",
            "Finland",
            "France",
            "Germany",
            "Greece",
            "Hong Kong",
            "Iceland",
            "Israel",
            "Italy",
            "Japan",
            "Lebanon",
            "Lithuania",
            "Malta",
            "Netherlands",
            "Norway",
            "Poland",
            "Portugal",
            "Saudi Arabia",
            "Singapore",
            "Spain",
            "Sweden",
            "Switzerland",
            "USA",
            "United Arab Emirates",
            "United Kingdom",
            "Unspecified",
        ],
    )
    validator.expect_column_values_to_not_be_null(column="country")
    validator.expect_column_values_to_be_in_type_list(
        column="country", type_list=["str"]
    )
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
