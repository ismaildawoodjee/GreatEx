import os
import sys
import logging
import subprocess
from airflow.exceptions import AirflowException


def validate_checkpoint_alt(checkpoint_name):
    """Alternate method of validating checkpoints using `subprocess` and exceptions

    Args:
        checkpoint_name (str): name of checkpoint to be validated against

    Raises:
        AirflowException: when validation fails
    """
    try:
        os.system("cd /opt/airflow")
        result = subprocess.Popen(
            ["great_expectations", "--v3-api", "checkpoint", "run", checkpoint_name]
        )
        text = result.communicate()[0]  # so that Bash exit code can be obtained
        return_code = result.returncode
        if return_code:
            raise AirflowException(
                "Checkpoint validation failed. The Bash command returned a non-zero exit code. "
                "Inspect the Data Docs for more information.\n"
            )
    except AirflowException as error:
        # use logging instead? with severity error. or try to exit with sys.exit
        print(error)
        subprocess.Popen(["great_expectations", "--v3-api", "docs", "list"])


def validate_checkpoint(checkpoint_name):
    import great_expectations as ge

    context = ge.get_context()

    result = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request=None,
        run_name=None,
    )

    if not result["success"]:
        logging.error("Validation failed! Inspect the Data Docs for more info")
        os.system("great_expectations --v3-api docs list")
        sys.exit(1)

    logging.info("Validation succeeded!")
    sys.exit(0)


validate_checkpoint(checkpoint_name="retail_source_checkpoint")
# validate_checkpoint_alt(checkpoint_name="retail_source_checkpoint")
