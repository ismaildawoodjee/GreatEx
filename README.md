# Airflow Data Pipeline Validation with Great Expectations

- [Airflow Data Pipeline Validation with Great Expectations](#airflow-data-pipeline-validation-with-great-expectations)
  - [Introduction](#introduction)
  - [Setup](#setup)
  - [About the `docker-compose` File](#about-the-docker-compose-file)
  - [Retail Pipeline DAG](#retail-pipeline-dag)
  - [Configuring Great Expectations](#configuring-great-expectations)
  - [Running the DAG](#running-the-dag)
    - [Airflow Log Output](#airflow-log-output)
    - [Email on Validation Failure](#email-on-validation-failure)

## Introduction

This project looks at how Great Expectations can be used to ensure data quality within an Airflow
pipeline. All the databases and Airflow components are designed to run within Docker containers, so that
it's easy to set it up with a `docker-compose` file. The setup for this architecture contains instructions
for running on both Windows and Linux OS. The flowchart below shows the high-level architecture of this data pipeline:

![Data pipeline](assets/images/pipeline.png)

I will be using Great Expectations `v0.13.25` and the Version 3 API, Docker `v20.10.7` (and Docker Desktop in Windows),
`docker-compose` `v1.29.2`, Python `v3.8.10` for the containers (`v3.9.5` on local machine), and Windows OS (Windows 10).
The setup was also tested on Ubuntu `v20.04` and can be run by following additional instructions in the [Setup](#setup) section.

[**Google Sheet to fill out Great Expectations configuration**](https://docs.google.com/spreadsheets/d/1yO5cSmHFrE78R7kcUfP7fNzZcRpgG5eieApFwZ0f8oY/edit?usp=sharing)

[**Official Documentation**](https://docs.greatexpectations.io/en/latest/) - the new official documentation

[**Legacy Documentation**](https://legacy.docs.greatexpectations.io/en/latest/) - old legacy docs

**Repo directory structure:**

    .
    ├── dags
    │   ├── scripts
    │   │   ├── python
    │   │   │   ├── retail_dest.py
    │   │   │   ├── retail_load.py
    │   │   │   ├── retail_source.py
    │   │   │   ├── retail_transform.py
    │   │   │   ├── retail_warehouse.py
    │   │   │   └── utils.py   
    │   │   └── sql
    │   │       ├── extract_load_retail_source.sql
    │   │       ├── load_retail_stage.sql
    │   │       ├── transform_load_retail_warehouse.sql
    │   │       └── validations_store.sql
    │   ├── retail_data_pipeline.py
    │   ├── transformations.py
    │   └── validations.py
    ├── database-setup
    │   ├── destinationdb.sql
    │   └── sourcedb.sql
    ├── debugging
    │   └── logging.ini
    ├── dest-data
    │   └── dummy.txt
    ├── filesystem
    │   ├── raw
    │   │   └── dummy.txt
    │   └── stage
    │       ├── temp
    │       │   └── dummy.txt
    │       └── dummy.txt
    ├── great_expectations
    │   ├── checkpoints
    │   │   ├── retail_dest_checkpoint.yml
    │   │   ├── retail_load_checkpoint.yml
    │   │   ├── retail_source_checkpoint.yml
    │   │   ├── retail_transform_checkpoint.yml
    │   │   └── retail_warehouse_checkpoint.yml
    │   ├── expectations
    │   │   ├── .ge_store_backend_id
    │   │   ├── retail_dest_suite.json
    │   │   ├── retail_load_suite.json
    │   │   ├── retail_source_suite.json
    │   │   ├── retail_transform_suite.json
    │   │   ├── retail_warehouse_suite.json
    │   │   └── test_suite.json
    │   ├── uncommitted
    │   │   └── config_variables.yml
    │   ├── .gitignore
    │   └── great_expectations.yml
    ├── source-data
    │   ├── retail_profiling.csv
    │   └── retail_validating.csv
    ├── .dockerignore
    ├── .env-example
    ├── .gitattributes
    ├── .gitignore
    ├── airflow_conn.ps1
    ├── airflow_conn.sh
    ├── docker-compose.yml
    ├── Dockerfile
    ├── README.md
    └── requirements.txt

## Setup

To clone the source data from this repository, `git-lfs` or Git Large File Storage must be installed first.
On Windows OS, follow the instructions from this [website](https://git-lfs.github.com/). On Linux, run the following
command to install `git-lfs` on your system:

    sudo apt install git-lfs

Once that is done, you can proceed with cloning this repo:

    git clone https://github.com/ismaildawoodjee/GreatEx; cd GreatEx

Ensure that the CSV files and their contents are present in the `source-data` folder, either using
`Get-Content .\source-data\retail_profiling.csv | select -First 10` with Powershell
or `head -n 10 source-data/retail_profiling.csv` with Bash, or just opening it as a file:

![CSV file contents](assets/images/source_data.png)

Prepare a Python virtual environment (assuming Windows OS),

    python -m venv .venv; .venv\Scripts\activate

If on a Linux OS, run the following:

    python3 -m venv .venv; source .venv/bin/activate

Install Great Expectations and required dependencies with

    pip install -r requirements.txt

Currently, the `great_expectations` folder in this repo is partially initialized.
Reinitialize it by running:

    great_expectations --v3-api init

A warning will pop up that Great Expectations cannot find the credentials, but that
can be ignored unless you want to test by running the pipeline locally. Since we're going
to be running the pipeline on Airflow containers, the warning can be ignored.

Before initializing the Docker containers, make sure that there is an `.env` file
in the root directory of `GreatEx`. This is the `.env-example` file in the repo, which should
be **renamed** to `.env`.

The database connections inside it should remain unchanged, but the final three fields
highlighted below should be changed to the appropriate emails and passwords. If you're
using Gmail, [less secure apps](https://support.google.com/accounts/answer/6010255?hl=en)
of the Sender email should be turned on.

![Environment variables configuration](assets/images/configuring_env_variables.png)

Next, ensure that Docker is running before initializing the Airflow containers.
The Dockerfile extends the Airflow image to install Python libraries within the
containers as well.

On Windows OS and using Powershell, run the first command to add the path to local directory into the `.env` file
and run the second command to initialize Airflow containers:

    Add-Content -Path .\.env -Value "`nLOCAL_DIRECTORY=$pwd"
    docker-compose up --build airflow-init

On Linux OS, additional commands need to be run to provide permissions for read/write
(prepending `sudo` and providing permissions where needed):

    echo -e "\nAIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\nLOCAL_DIRECTORY=$(pwd)" >> .env
    mkdir ./logs ./plugins
    sudo chmod 777 dags/* logs/ filesystem/* database-setup/* source-data/* dest-data/* great_expectations/*
    sudo docker-compose up --build airflow-init

If the Airflow initialization was successful, there will be a return code of 0 shown below:

![Return Code 0](assets/images/airflow_init_success_code.png)

Then, start the containers in the background (prepending `sudo` if needed):

    docker-compose up --build -d

After this, we want to add our Postgres database connections to Airflow, for which I have provided
a convenience script with `airflow_conn.ps1`. Run this Powershell script with:

    .\airflow_conn.ps1

Or if using a Linux machine, run the Bash script below (this will ask you for your `sudo` password):

    ./airflow_conn.sh

If you are using VS Code, you can check the health of the containers with the Docker extension:

![Container health from Docker extension](assets/images/container_health.png)

Otherwise, type `docker ps -a` into the terminal to check their health status.
Once the Airflow Worker and Webserver are in a healthy state, you can go to `localhost:8080` in
the webbrowser to log into the Airflow UI (using default username: "airflow" and password: "airflow")
and run the data pipeline.

The `greatex_airflow-init_1` container is expected to exit after initializing the Airflow containers,
but the rest of the containers should be healthy and functioning after several minutes. If they become unhealthy
or keep exiting, more memory needs to be allocated to the Docker engine, which can be done via the [.wslconfig](https://docs.microsoft.com/en-us/windows/wsl/wsl-config#configure-global-options-with-wslconfig)
file in Windows. If you are using the Docker Desktop application in Linux or MacOS, you can got to Settings -> Resources
and increase the memory there.

After you are finished with exploring the pipeline, tear down the infrastructure
(stopping containers, removing images and volumes):

    docker-compose down --rmi --volumes all

## About the `docker-compose` File

The original `docker-compose` file for setting up Airflow containers was obtained from
[Apache Airflow instructions](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
for running Airflow in Docker. I modified it in several different ways:

- Set `AIRFLOW__CORE__LOAD_EXAMPLES` to `false` to declutter the Airflow UI.

- An additional environment variable `AIRFLOW_CONN_POSTGRES_DEFAULT` for the Airflow meta database, which I renamed to `postgres-airflow`.

- Specified an environment file `.env` for the `env_file` key.

- I wrote a Dockerfile to extend the image (according to [the official docs](https://airflow.apache.org/docs/docker-stack/build.html#extending-the-image))
  so that the Great Expectations library and other packages will be installed inside the containers. This means I use the `build`
  key, with `build: .` to build on the extended image specified in the Dockerfile instead of using the `image` key.

- Added three additional PostgresDB containers `postgres-source`, `postgres-dest` and `postgres-store` to represent the
  retail data source, the destination data warehouse, and the database where the data validation results are stored.

- For Airflow to properly orchestrate these containers, their volumes and port mappings were also specified, e.g. `5433:5432` for the source database so that it
  won't conflict with Airflow's meta database mapping `5432:5432`. The hostnames and ports are meant for Airflow containers,
  but if you want to test the connections locally, use `localhost:543x` with the appropriate ports instead.

  A common (but terrible) error message that pops up when connections are not configured correctly looks like the following, e.g.
  where it just outputs the Datasource name instead of providing an actual error message:

  ![Error when connecting to or validating retail_source Datasource](assets/images/misconfiguring_hostname_portnumber.png)

- The `great_expectations` folder, `filesystem` folder, `database-setup`, `source-data` and `dest-data` folders also need to be
  mounted as volumes to the appropriate containers so that Airflow can access them.

- The service names for the containers are also their hostnames, so we should keep that in mind
  (else Docker will keep searching for the wrong services and won't find them). Below, I changed the
  service name from the default `postgres` to `postgres-airflow`,
  so the hostname in the connection strings must also be changed:

    ![service_name == hostname](assets/images/service_name_hostname.png)

  This also applies to username, password and database names.

- The Docker images for Airflow and Postgres can be any supported version, but I specified them to use the `latest` tag, and used Python version 3.8.

## Retail Pipeline DAG

Currently, the Airflow DAG looks like the following:

![Retail pipeline DAG](assets/images/current_dag.png)

- `validate_retail_source_data`: This task uses the PythonOperator to run a GreatEx Checkpoint for validating data in the
  `postgres-source` database. A Checkpoint is a pair between a Datasource and an Expectation Suite, so to create a Checkpoint,
  both the Datasource and the Expectation Suite must be present (see the section on [Configuring Great Expectations](#configuring-great-expectations)).

- `extract_load_retail_source`: This task extracts data from the `postgres-source` database and loads it into the `raw`
  directory of the `filesystem`. The `filesystem` could represent a cloud data lake, or an on-premise file storage system.
  The SQL script that is run in this task can be a customized query to pull any kind of data from the source.

- `validate_retail_raw_data`: This task runs another GreatEx Checkpoint to validate data that has just landed in the `raw` folder.
  It uses the same Expectation Suite, but uses the data in the `filesystem/raw` directory instead of the `postgres-source` database.

- `transform_load_retail_raw`: After the source data has landed into the `raw` folder, transformations can now take place. Here,
  I just specified a simple transformation where if a country name is `"Unspecified"`, I change it to `None`. Again, any kind of
  customized transformation can be specified. It could be a Python script running on local host, a serverless Lambda function, or
  a Spark cluster. This transformed data is then loaded into the `filesystem/stage` directory in the form of a Parquet file.

- `validate_retail_stage_data`: After the Parquet file has landed into the `stage` folder, another Checkpoint validation takes place.
  A different Expectation Suite can be created and paired up with the staged Datasource
  (I used the same Expectation Suite since there weren't any significant transformations).

- `transform_retail_stage`: This task converts the Parquet file format into CSV so that Postgres can copy the data. Postgres, unfortunately,
  does not have the functionality to copy Parquet files, so this task converts Parquet to CSV and puts the resulting CSV file
  into a temporary folder `temp` in the `filesystem/stage` directory. I could also implement a function to delete the temporary file after
  copying, but it will add an extra step to the pipeline.

- `load_retail_stage`: Once the Parquet file has been converted to CSV and put into the `temp` folder, Postgres can copy that file
  and load it into the database, using the schema name `stage` for the staging component of the destination data warehouse.

- `validate_retail_warehouse_data`: After the CSV file is copied into a PostgresDB table, we can validate that data to ensure that all
  expectations are met. Again, the same Expectation Suite was used, but depending on the SQL script for the copy operation, a different
  Suite can be created to match the data in the Postgres database.

- `transform_load_retail_warehouse`: This task transforms the data table in the `stage` schema and loads it into the `public` schema.
  Here, instead of loading all 8 columns, I chose to load 6 columns to represent a simple transformation. This transformation
  can be more complex (perhaps consisting of joins, aggregates or modelling) or can consist of more data transformation steps before
  the final loading step.

- `validate_retail_dest_data`: The data arrives at its final destination in the `public` schema of the data warehouse. In this final task,
  we ensure that the public-facing data meets the expectations and the Checkpoint validation passes before ending the pipeline.

- `end_of_data_pipeline`: A DummyOperator to mark the end of the DAG.

## Configuring Great Expectations

**Note:** The configuration in this section can be skipped since I've already done almost all of the steps
(except the email credentials in the [Setup](#setup) section).

Except for the very first step of creating a Data Context (the Great Expectations folder and associated files), all of the
configuration for connecting to Datasources, creating Expectation Suites, creating Checkpoints and validating them, will be
done using Python scripts, without any Jupyter Notebooks. I'll be using Great Expectations Version 3 API.

1. We've already reinitialized the Data Context in the Setup section. But if you wish to start from scratch,
   keep a copy of the `config_variables.yml` and `great_expectations.yml` files and delete the `great_expectations` folder.
   Then, initialize the Data Context by running:

        great_expectations --v3-api init

2. The next step is to connect to Datasources. Here, we have 5 Datasources, three of them are tables in relational databases,
   one is a CSV file in the raw directory, and another is a Parquet file in the stage directory.

      - Postgres source database `sourcedb` with schema `ecommerce`, where the source data is located (`postgres-source` container)
      - Raw directory in the `filesystem` (`filesystem/raw`)
      - Stage directory in the `filesystem` (`filesystem/stage`)
      - Postgres destination database `destdb` with schema `stage` (`postgres-dest` container), the penultimate location where transformed data is
        staged before moving it to the public schema.
      - Same destination database but with schema `public`, where the final transformed data is located

    **Note:** Before running any of the Python scripts, run the Airflow DAG at least once, so that all the data files and tables are moved
    to their respective locations. Only then will you be able to run the scripts and test the Datasource connections locally.

    Each Datasource has its own Python script in the `dags/scripts/python/` folder where batches of data can be sampled,
    Expectation Suites created, and Checkpoints can be configured. The `utils.py` file contains the imports and variables
    that are common across all Datasources. The following fields show an example configuration in the `utils.py` file, where
    they should be stored as environment variables (and stored in the `config_variables.yml` file):

        VALIDATION_ACTION_NAME = "email_on_validation_failure"
        NOTIFY_ON = "failure"
        USE_TLS = True
        USE_SSL = False
        SMTP_ADDRESS = smtp.gmail.com
        SMTP_PORT = 587
        SENDER_LOGIN = sender-gmail@gmail.com
        SENDER_PASSWORD = sender-gmail-password
        RECEIVER_EMAILS = receiver-email1@gmail.com,receiver-email2@mail.com

    The fields for connecting to Datasources are already in the Python scripts,
    and they can be renamed/modified if needed. Running the `connect_to_datasource()` function should automatically
    populate the `great_expectations.yml` file and add a new Datasource. An example configuration for the destination
    database fields are shown below:

        DATASOURCE_NAME = "retail_dest"
        EXPECTATION_SUITE_NAME = "retail_dest_suite"
        CHECKPOINT_NAME = "retail_dest_checkpoint"
        DATABASE_CONN = "postgresql+psycopg2://destdb1:destdb1@postgres-dest:5432/destdb"
        INCLUDE_SCHEMA_NAME = True
        SCHEMA_NAME = "public"
        TABLE_NAME = "retail_profiling"
        DATA_ASSET_NAME = "public.retail_profiling"

    These fields are different depending on the Datasource that we are connecting to (CSV, database, etc.).
    Again, sensitive credentials such as the database connection string should be stored as environment variables
    or in the `config_variables.yml` file, which should not tracked by Git. Note that the database
    connection strings should be changed to `localhost:543x` if you want to test connections locally.

3. After connecting to a Datasource and sampling a batch of data, we can create an Expectation Suite out of it.
   I defined three example Expectations for each Datasource, but more can be added depending on the rigor of testing
   needed for the data. Running the `create_expectation_suite` function with an Expectation Suite name creates a JSON
   file with the same name in the `great_expectations/expectations` folder, where all the Expectations specified within
   the function are stored.

4. Once we have both a Datasource and an Expectation Suite, we can pair them together in files that are called Checkpoints.
   The `checkpoint_config` string in the Python scripts does exactly this. Running the `create_checkpoint` function takes
   this string and populates a YAML file with the specified Checkpoint name, which is located in the `great_expectations/checkpoints`
   folder. I've also added an extra `action` to the `action_list` for each Checkpoint file to send an email on validation failure.

   ![For sending email on validation failure](assets/images/email_on_validation_failure.png)

   **Note:** Validating (running) the Checkpoint locally will keep a record at the local time whereas validating by
   running the Airflow DAG will keep records that use the UTC timestamp. Hence, to prevent inconsistency between the
   timestamps on the validation records shown on the Data Docs (which are also stored in the `postgres-store` database),
   it is recommended **not** to run Checkpoints locally.

5. The function for validating a Checkpoint is in the `dags/scripts/validation.py` file, where Bash commands are used
   to run a couple of Great Expectations CLI commands. Instead of using a BashOperator, I used a PythonOperator so that
   a custom error message can be displayed on the Airflow logs.

   The `validate_checkpoint` function attempts to run a Checkpoint first. If it fails, it lists out the Data Docs and
   sends the output to the Airflow log. An email is also sent to the `RECEIVER_EMAILS` specified in the Checkpoint YAML files.

   In addition, the record of each validation is stored in the `postgres-store` database, which was configured by adding
   the following lines into the `great_expectations.yml` file:

   ![Configuring validations storage in Postgres database](assets/images/validations_store.png)

## Running the DAG

The default settings and configuration I have provided in this repo should allow the DAG to run successfully without
failing. To test what happens when a step fails, modify some of the Expectations in the `great_expectations/expectations`
folder so that the data doesn't meet all of those Expectations.

### Airflow Log Output

Upon unsuccessful validation when running the DAG, the Airflow error log will contain a link to the Data Docs, where we can
see what went wrong with the validation, and why some of the Expectations failed.

![Link to Data Docs in Airflow logs](assets/images/airflow_error_log.png)

Since the pipeline is running on a container, the link is prefixed with `/opt/airflow`, so I added an additional log
message to include the link in the local machine as well. The `great_expectations` folder is mounted onto the container,
so all the files that are accessible on localhost are also available to the container, and all files created by operations
running on the container are also available on localhost.

### Email on Validation Failure

Users can be notified by email when the DAG fails to run due to data validation error. The credentials are configured in the
`config_variables.yml` file and the `.env` file, and also specified as an `action` in each of the Checkpoint YAML files.

![A typical failure email](assets/images/email.png)
