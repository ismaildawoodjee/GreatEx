import logging
from logging.config import fileConfig

from ruamel import yaml
import great_expectations as ge

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

# context = ge.get_context()  # can only be called after initializing GE context


class SimpleCheckpoint:
    def __init__(
        self,
        checkpoint_name,
        datasource_name,
        data_connector_name,
        data_asset_name,
        expectation_suite_name,
    ):
        self.context = ge.get_context()

        self.checkpoint_name = checkpoint_name
        self.datasource_name = datasource_name
        self.data_connector_name = data_connector_name
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name

    def create_checkpoint(self):

        config = f"""
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




def raw_data_checkpoint():
    pass


def stage_data_checkpoint():
    pass


def warehousedb_checkpoint():
    pass


def destdb_checkpoint():
    pass
