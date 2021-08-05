from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

context = ge.get_context()

datasource_config = {
    "name": "retail_transform",
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
            "base_directory": "../filesystem/stage",
            "default_regex": {
                "group_names": [
                    "data_asset_name",
                    "year",
                    "month",
                    "day",
                    "parquet_compression",
                ],
                "pattern": "(.*)(-\d{4})-(\d{2})-(\d{2})\.(.*)\.parquet",
            },
        },
    },
}

context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)

batch_request = BatchRequest(
    datasource_name="retail_load",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="retail_profiling",
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_source_suite"
)

print(validator.head())
