import logging
from logging.config import fileConfig
from great_expectations.marshmallow__shade.fields import Bool, Boolean

from ruamel import yaml

import great_expectations as ge

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

context = ge.get_context()

PLACES = ["source", "load", "transform", "warehouse", "dest"]
SCHEMAS = ["ecommerce.", "", "", "stage.", "public."]

DATASOURCE_NAMES = [("retail_" + place) for place in PLACES]
CHECKPOINT_NAMES = [("retail_" + place + "_checkpoint") for place in PLACES]
DATA_ASSET_NAMES = [(schema + "retail_profiling") for schema in SCHEMAS]
EXPECTATION_SUITE_NAME = "test_suite"


class SimpleCheckpoint:

    _context = ge.get_context()

    def __init__(
        self,
        checkpoint_name,
        datasource_name,
        data_asset_name,
        expectation_suite_name,
        data_connector_name="default_inferred_data_connector_name",
    ):

        self.checkpoint_name = checkpoint_name
        self.datasource_name = datasource_name
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        self.data_connector_name = data_connector_name

    def configure_checkpoint(self):
        yaml_config = f"""
        name: {self.checkpoint_name}
        config_version: 1
        class_name: SimpleCheckpoint
        validations:
        - batch_request:
            datasource_name: {self.datasource_name}
            data_connector_name: {self.data_connector_name}
            data_asset_name: {self.data_asset_name}
            data_connector_query:
                index: -1
          expectation_suite_name: {self.expectation_suite_name}
        """
        return yaml_config

    def create_checkpoint(self, yaml_config: str):
        return SimpleCheckpoint._context.test_yaml_config(yaml_config=yaml_config)

    def get_checkpoint(self, checkpoint):
        return checkpoint.get_substituted_config().to_yaml_str()

    def add_checkpoint(self, yaml_config: str):
        SimpleCheckpoint._context.add_checkpoint(**yaml.load(yaml_config))

    def run_checkpoint(self, checkpoint_name):
        result = SimpleCheckpoint._context.run_checkpoint(checkpoint_name)
        print(f'Successful? {result["Success"]}')


def add_example_checkpoints():
    """Example for creating checkpoints with an organized naming convention"""

    for ds_name, cp_name, da_name in zip(DATASOURCE_NAMES, CHECKPOINT_NAMES, DATA_ASSET_NAMES):
        sc = SimpleCheckpoint(
            checkpoint_name=cp_name,
            datasource_name=ds_name,
            data_asset_name=DATA_ASSET_NAMES,
            expectation_suite_name=EXPECTATION_SUITE_NAME,
        )

        # configure the YAML string first, then create a Checkpoint instance using the config
        config = sc.configure_checkpoint()
        checkpoint = sc.create_checkpoint(config)

        # prints out the Checkpoint YAML configuration
        print(sc.get_checkpoint(checkpoint))

        # adds Checkpoint to a YAML file in `great_expectations` folder
        sc.add_checkpoint(config)

        sc.run_checkpoint(cp_name)

# add_example_checkpoints()

# if __name__ == "__main__":
#     add_example_checkpoints()

    # sc = SimpleCheckpoint(checkpoint_name="d")
    # print(datasources)
    # print(checkpoints)

# print(context.run_checkpoint("retail_source_checkpoint")["success"])