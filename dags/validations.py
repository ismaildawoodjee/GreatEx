import os
import subprocess
from airflow.exceptions import AirflowException


def validate_checkpoint(checkpoint_name):

    try:
        os.system("cd /opt/airflow")
        result = subprocess.Popen(
            ["great_expectations", "--v3-api", "checkpoint", "run", checkpoint_name]
        )
        text = result.communicate()[0]
        return_code = result.returncode
        if return_code:
            raise AirflowException(
                "Checkpoint validation failed. The Bash command returned a non-zero exit code. "
                "Inspect the Data Docs for more information.\n"
            )
    except AirflowException as error:
        print(error)
        subprocess.Popen(["great_expectations", "--v3-api", "docs", "list"])


# validate_checkpoint(checkpoint_name="retail_source_checkpoint")
