from utils import *

# Fill in information for the fields specified in all-caps
DATASOURCE_NAME = "retail_dest"
EXPECTATION_SUITE_NAME = f"{DATASOURCE_NAME}_suite"
CHECKPOINT_NAME = f"{DATASOURCE_NAME}_checkpoint"

# Data Asset fields
DATABASE_CONN = os.environ.get("DESTDB_CONN")
INCLUDE_SCHEMA_NAME = True
SCHEMA_NAME = "public"
TABLE_NAME = "retail_profiling"
DATA_ASSET_NAME = f"{SCHEMA_NAME}.{TABLE_NAME}" if INCLUDE_SCHEMA_NAME else TABLE_NAME

datasource_config = {
    "name": DATASOURCE_NAME,
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": DATABASE_CONN,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": INCLUDE_SCHEMA_NAME,
        },
    },
}

batch_request = RuntimeBatchRequest(
    datasource_name=DATASOURCE_NAME,
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=DATA_ASSET_NAME,
    runtime_parameters={"query": f"SELECT * from {DATA_ASSET_NAME} LIMIT 1000"},
    batch_identifiers={
        "default_identifier_name": "First 1000 rows for profiling final destination data"
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
    """Connects to Postgres destination database, where schema is `public`,
    and adds to YAML file if successful.
    """

    try:
        # connect to the datasource and sample out about 1000 rows just to confirm
        context.test_yaml_config(yaml.dump(datasource_config))

        # empty Expectation Test Suite, its only purpose is to validate the Datasource connection
        context.create_expectation_suite(
            expectation_suite_name="test_suite", overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )
        print(validator.head())

    except Exception as ex:
        # the raised error may be completely off the mark, saying that password authentication
        # failed even though password is correct, but port number, host name or DB name could be wrong.
        logging.exception(
            f"Cannot connect to database with connection string {DATABASE_CONN}"
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added database config to `great_expectations.yml`")


def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )
    # Add Expectations here:
    # Table-Level Expectations
    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            "customer_id",
            "stock_code",
            "invoice_date",
            "quantity",
            "unit_price",
            "country",
        ]
    )
    validator.expect_table_row_count_to_be_between(max_value=250000, min_value=150000)
    # Column-level Expectations
    # customer_id
    validator.expect_column_values_to_not_be_null(column="customer_id")
    validator.expect_column_values_to_be_in_type_list(
        column="customer_id", type_list=["INTEGER"]
    )
    # stock_code
    validator.expect_column_values_to_not_be_null(column="stock_code")
    validator.expect_column_values_to_be_in_type_list(
        column="stock_code", type_list=["VARCHAR"]
    )
    # invoice_date
    validator.expect_column_values_to_not_be_null(column="invoice_date")
    validator.expect_column_values_to_be_in_type_list(
        column="invoice_date", type_list=["TIMESTAMP"]
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
        column="quantity", type_list=["INTEGER"]
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
        column="unit_price", type_list=["NUMERIC"]
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
    validator.expect_column_values_to_not_be_null(column="country", mostly=0.99)
    validator.expect_column_values_to_be_in_type_list(
        column="country", type_list=["VARCHAR"]
    )
    # Save Expectations to JSON
    validator.save_expectation_suite(discard_failed_expectations=False)


def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))

    # WARNING: Running the checkpoint locally will mess up the Run Time in the Data Docs.

    # result = context.run_checkpoint(checkpoint_name)
    # print(f'Successful checkpoint validation: {result["success"]}\n')


connect_to_datasource()
create_expectation_suite(expectation_suite_name=EXPECTATION_SUITE_NAME)
create_checkpoint(checkpoint_name=CHECKPOINT_NAME)
