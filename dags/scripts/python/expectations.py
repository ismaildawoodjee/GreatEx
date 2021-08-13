"""
Python script to generate Expectation Suites by sampling a batch of data
from the `postgres-source` and `postgres-dest` databases. The configuration is
quite similar to connecting to Datasources (in the datasources.py), so
that Expectation Suites could actually be created right after connecting to them.

Some of the code is repeated from the `datasources.py` script, except for writing
the expectations themselves. I should probably try to refactor the scripts
to be "Datasource-oriented", where each Datasource and its configuration are placed
in a single script, instead of writing a massive script for each Great Expectations
configuration step.
"""

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

context = ge.get_context()


def create_sourcedb_expectation_suite():

    EXPECTATION_SUITE_NAME = "retail_source_suite"
    DATASOURCE_NAME = "retail_source"
    INCLUDE_SCHEMA_NAME = True
    SCHEMA_NAME = "ecommerce"
    TABLE_NAME = "retail_profiling"
    DATA_ASSET_NAME = (
        f"{SCHEMA_NAME}.{TABLE_NAME}" if INCLUDE_SCHEMA_NAME else TABLE_NAME
    )

    context.create_expectation_suite(EXPECTATION_SUITE_NAME, overwrite_existing=True)

    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"query": f"SELECT * from {DATA_ASSET_NAME} LIMIT 1000;"},
        batch_identifiers={
            "default_identifier_name": "First 1000 rows for profiling retail source data"
        },
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=EXPECTATION_SUITE_NAME
    )

    # Add expectations here
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
    # validator.expect_column_values_to_match_strftime_format(
    #     column="invoice_date", strftime_format="%Y-%m-%d %H:%M:%S"
    # )  # unfortunately, there is no strf_format expectation for SQL datasources

    # Save expectations to JSON
    validator.save_expectation_suite(discard_failed_expectations=False)


def create_destdb_expectation_suite():

    EXPECTATION_SUITE_NAME = "retail_dest_suite"
    DATASOURCE_NAME = "retail_dest"
    INCLUDE_SCHEMA_NAME = True
    SCHEMA_NAME = "public"
    TABLE_NAME = "retail_profiling"
    DATA_ASSET_NAME = (
        f"{SCHEMA_NAME}.{TABLE_NAME}" if INCLUDE_SCHEMA_NAME else TABLE_NAME
    )

    context.create_expectation_suite(EXPECTATION_SUITE_NAME, overwrite_existing=True)

    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"query": f"SELECT * from {DATA_ASSET_NAME} LIMIT 1000"},
        batch_identifiers={
            "default_identifier_name": "First 1000 rows for profiling retail source data"
        },
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=EXPECTATION_SUITE_NAME
    )

    # Add expectations here
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
    validator.expect_column_min_to_be_between(
        column="unit_price", min_value=0, max_value=0
    )
    validator.expect_column_values_to_not_be_null(column="quantity")
    # validator.expect_column_values_to_match_strftime_format(
    #     column="invoice_date", strftime_format="%Y-%m-%d %H:%M:%S"
    # )  # unfortunately, there is no strf_format expectation for SQL datasources

    # Save expectations to JSON
    validator.save_expectation_suite(discard_failed_expectations=False)


# create_sourcedb_expectation_suite()
# create_destdb_expectation_suite()
