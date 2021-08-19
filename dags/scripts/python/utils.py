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
USE_TLS = True
USE_SSL = False
SMTP_ADDRESS = os.environ.get("SMTP_ADDRESS")
SMTP_PORT = os.environ.get("SMTP_PORT")  # use port 587 for TLS
SENDER_LOGIN = os.environ.get("SENDER_LOGIN")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECEIVER_EMAILS = os.environ.get("RECEIVER_EMAILS")

VALIDATION_ACTION = f"""\
  - name: {VALIDATION_ACTION_NAME}
    action:
      class_name: EmailAction
      notify_on: {NOTIFY_ON} 
      notify_with:
      use_tls: {USE_TLS}
      use_ssl: {USE_SSL}
      renderer:
        module_name: great_expectations.render.renderer.email_renderer
        class_name: EmailRenderer
      smtp_address: {SMTP_ADDRESS}
      smtp_port: {SMTP_PORT}
      sender_login: {SENDER_LOGIN}
      sender_password: {SENDER_PASSWORD}
      receiver_emails: {RECEIVER_EMAILS}
"""
