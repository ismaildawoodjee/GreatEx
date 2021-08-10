"""
Python script to configure Datasources and populate the Great Expectations YAML file
Need to install sqlalchemy and psycopg2 when connecting to a Postgres database
"""

import logging
from logging.config import fileConfig

from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

context = ge.get_context()  # can only be called after initializing GE context


def sourcedb_conn():
    """Connects to Postgres source database, adding to YAML file if successful."""

    SOURCEDB_NAME = "retail_source"  # name of Datasource (can be anything)
    SOURCEDB_CONN = "postgresql+psycopg2://sourcedb1:sourcedb1@localhost:5433/sourcedb"
    INCLUDE_SCHEMA_NAME = True  # to specify schema name when calling BatchRequest
    DATA_ASSET_NAME = "ecommerce.retail_profiling"

    sourcedb_config = {
        "name": SOURCEDB_NAME,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": SOURCEDB_CONN,
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

    try:
        # connect to the datasource and sample out about 1000 rows just to confirm
        context.test_yaml_config(yaml.dump(sourcedb_config))
        batch_request = RuntimeBatchRequest(
            datasource_name=SOURCEDB_NAME,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=DATA_ASSET_NAME,
            runtime_parameters={"query": f"SELECT * from {DATA_ASSET_NAME} LIMIT 1000"},
            batch_identifiers={
                "default_identifier_name": "First 1000 rows for profiling retail source data"
            },
        )
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
            f"Cannot connect to database with connection string {SOURCEDB_CONN}"
        )

    else:
        # context.add_datasource(**sourcedb_config)
        logging.info("Added database config to `great_expectations.yml`")


def raw_data_conn():

    RAW_DATASOURCE_NAME = "retail_load"
    RAW_DATA_DIRECTORY = "../filesystem/raw"  # relative to GE YAML file
    RAW_DATA_GROUP_NAMES = ["data_asset_name", "year", "month", "day"]
    RAW_DATA_REGEX_PATTERN = "(.*)(-\d{4})-(\d{2})-(\d{2})\.csv"
    DATA_ASSET_NAME = "retail_profiling"
    READER_METHOD = "read_csv"

    raw_data_config = {
        "name": RAW_DATASOURCE_NAME,
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
                "base_directory": RAW_DATA_DIRECTORY,
                "default_regex": {
                    "group_names": RAW_DATA_GROUP_NAMES,
                    "pattern": RAW_DATA_REGEX_PATTERN,
                },
            },
        },
    }

    try:
        # connect to raw data and sample out 1000 rows from the CSV file
        context.test_yaml_config(yaml.dump(raw_data_config))
        batch_request = BatchRequest(
            datasource_name=RAW_DATASOURCE_NAME,
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name=DATA_ASSET_NAME,
            batch_spec_passthrough={
                "reader_method": READER_METHOD,
                "reader_options": {"nrows": 1000},
            },
        )
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
Ensure that your base directory: "{RAW_DATA_DIRECTORY}", group names "{RAW_DATA_GROUP_NAMES}",\
and regex pattern "{RAW_DATA_REGEX_PATTERN}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{RAW_DATA_DIRECTORY}"'
        )

    else:
        # context.add_datasource(**raw_data_config)
        logging.info("Added raw data file config to `great_expectations.yml` file")


def stage_data_conn():

    STAGE_DATASOURCE_NAME = "retail_transform"
    STAGE_DATA_DIRECTORY = "../filesystem/stage"  # again, relative to GE YAML file
    STAGE_DATA_GROUP_NAMES = [
        "data_asset_name",
        "year",
        "month",
        "day",
        "parquet_compression",
    ]
    STAGE_DATA_REGEX_PATTERN = "(.*)(-\d{4})-(\d{2})-(\d{2})\.(.*)\.parquet"
    DATA_ASSET_NAME = "retail_profiling"

    stage_data_config = {
        "name": STAGE_DATASOURCE_NAME,
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
                "base_directory": STAGE_DATA_DIRECTORY,
                "default_regex": {
                    "group_names": STAGE_DATA_GROUP_NAMES,
                    "pattern": STAGE_DATA_REGEX_PATTERN,
                },
            },
        },
    }

    try:
        context.test_yaml_config(yaml.dump(stage_data_config))
        batch_request = BatchRequest(
            datasource_name=STAGE_DATASOURCE_NAME,
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name=DATA_ASSET_NAME,
        )
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
Ensure that your base directory: "{STAGE_DATA_DIRECTORY}", group names "{STAGE_DATA_GROUP_NAMES}",\
and regex pattern "{STAGE_DATA_REGEX_PATTERN}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{STAGE_DATA_DIRECTORY}"'
        )

    else:
        # context.add_datasource(**stage_data_config)
        logging.info("Added stage data file config to `great_expectations.yml` file")


# sourcedb_conn()
# raw_data_conn()
stage_data_conn()
