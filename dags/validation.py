import logging
import subprocess
from airflow.exceptions import AirflowException


def validate_checkpoint(checkpoint_name):
    """Python function to validate checkpoint. Writes output to Airflow logs and
    also generates a list of Data Docs when the validation fails.

    Args:
        checkpoint_name (str): name of checkpoint to be validated against

    Raises:
        AirflowException: when validation fails. Other ways of failing the validation
        either make the Airflow Task incorrectly succeed, or completely fail
        without being up for retry.
    """
    LOCAL_DIRECTORY = r"C:\Users\DELL\Desktop\Darmadia\GreatEx"
    DATA_DOCS_LOCATION = (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )

    validation_process = subprocess.Popen(
        ["great_expectations", "--v3-api", "checkpoint", "run", checkpoint_name],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    validation_stdout = validation_process.communicate()[0]
    return_code = validation_process.returncode

    if return_code:
        logging.error(validation_stdout)

        # TODO: could build Data Docs to S3 or somewhere, not just within the container

        docs_process = subprocess.Popen(
            ["great_expectations", "--v3-api", "docs", "list"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        docs_stdout = docs_process.communicate()[0]
        logging.info(docs_stdout)
        logging.info(f"{LOCAL_DIRECTORY}/{DATA_DOCS_LOCATION}")

        raise AirflowException(
            "Checkpoint validation failed. Inspect the Data Docs for more information."
        )

    else:
        logging.info(validation_stdout)


# validate_checkpoint(checkpoint_name="retail_source_checkpoint")
