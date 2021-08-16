"""These imports and variables are common across all Datasources"""
import os
import logging
from logging.config import fileConfig
from dotenv import load_dotenv, find_dotenv

from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

# Keep sensitive information in the .env file and in the config_variables.yml file
load_dotenv(find_dotenv())

# to send log information to both a log file and STDOUT
fileConfig("./debugging/logging.ini")
logger = logging.getLogger("dev")

context = ge.get_context()  # can only be called after initializing GE context
data_connector_name = "default_inferred_data_connector_name"  # default variable


"""These Validation fields are common across all Checkpoint configurations. 
If specific settings are required, put the variables into the appropriate 
Datasource Python scripts.
"""
VALIDATION_ACTION_NAME = "email_on_validation_failure"
NOTIFY_ON = "failure"  # possible values: "all", "failure", "success"
SMTP_ADDRESS = os.environ.get("SMTP_ADDRESS")
SMTP_PORT = os.environ.get("SMTP_PORT")
SENDER_LOGIN = os.environ.get("SENDER_LOGIN")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECEIVER_EMAILS = os.environ.get("RECEIVER_EMAILS")
