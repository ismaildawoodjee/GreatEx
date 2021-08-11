import logging
from logging.config import fileConfig

from ruamel import yaml
import great_expectations as ge

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

PLACES = ["source", "load", "transform", "warehouse", "dest"]
SCHEMAS = ["ecommerce.", "", "", "stage.", "public."]
DATASOURCE_NAMES = [("retail_" + place) for place in PLACES]
CHECKPOINT_NAMES = [("retail_" + place + "_checkpoint") for place in PLACES]
DATA_ASSET_NAMES = [(schema + "retail_profiling") for schema in SCHEMAS]
EXPECTATION_SUITE_NAME = "test_suite"


class SimpleCheckpoint:
    def __init__(self):
        self._context = ge.get_context()

    def configure_checkpoint(
        self,
        checkpoint_name,
        datasource_name,
        data_asset_name,
        expectation_suite_name,
        data_connector_name="default_inferred_data_connector_name",
    ) -> str:
        yaml_config = f"""
        name: {checkpoint_name}
        config_version: 1
        class_name: SimpleCheckpoint
        validations:
        - batch_request:
            datasource_name: {datasource_name}
            data_connector_name: {data_connector_name}
            data_asset_name: {data_asset_name}
            data_connector_query:
                index: -1
          expectation_suite_name: {expectation_suite_name}
        """
        return yaml_config

    def create_checkpoint(self, yaml_config: str):
        return self._context.test_yaml_config(
            yaml_config=yaml_config, pretty_print=True
        )

    def get_checkpoint(self, checkpoint) -> str:
        return checkpoint.get_substituted_config().to_yaml_str()

    def add_checkpoint(self, yaml_config: str):
        self._context.add_checkpoint(**yaml.load(yaml_config))

    def run_checkpoint(self, checkpoint_name) -> dict:
        return self._context.run_checkpoint(checkpoint_name)


def add_example_checkpoints():
    """Example for creating checkpoints with an organized naming convention"""

    for ds_name, cp_name, da_name in zip(
        DATASOURCE_NAMES, CHECKPOINT_NAMES, DATA_ASSET_NAMES
    ):
        sc = SimpleCheckpoint()

        # configure the YAML string first, then create a Checkpoint instance using the config
        config = sc.configure_checkpoint(
            checkpoint_name=cp_name,
            datasource_name=ds_name,
            data_asset_name=da_name,
            expectation_suite_name=EXPECTATION_SUITE_NAME,
        )
        checkpoint = sc.create_checkpoint(config)

        # prints out the Checkpoint YAML configuration
        print(sc.get_checkpoint(checkpoint))

        # adds Checkpoint to a YAML file in `great_expectations` folder
        sc.add_checkpoint(config)

        result = sc.run_checkpoint(cp_name)
        print(f'Successful checkpoint validation: {result["success"]}\n')


if __name__ == "__main__":
    add_example_checkpoints()
