import logging
from logging.config import fileConfig

from ruamel import yaml
import great_expectations as ge

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

# context = ge.get_context()  # can only be called after initializing GE context


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

    def test_checkpoint(self, yaml_config: str):
        return SimpleCheckpoint._context.test_yaml_config(yaml_config=yaml_config)

    def review_checkpoint(self, checkpoint):
        return checkpoint.get_substituted_config().to_yaml_str()

    def add_checkpoint(self, yaml_config: str):
        SimpleCheckpoint._context.add_checkpoint(**yaml.load(yaml_config))

    def run_checkpoint(self):
        SimpleCheckpoint._context.run_checkpoint(checkpoint_name=self.checkpoint_name)
