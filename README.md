# Airflow Data Pipeline Validation with Great Expectations

- [Airflow Data Pipeline Validation with Great Expectations](#airflow-data-pipeline-validation-with-great-expectations)
  - [Introduction](#introduction)
  - [Setup](#setup)
  - [About the `docker-compose` File](#about-the-docker-compose-file)
  - [Retail Pipeline DAG](#retail-pipeline-dag)
  - [Configuring Great Expectations](#configuring-great-expectations)

## Introduction

This project looks at how Great Expectations can be used to ensure data quality within an Airflow
pipeline. All the databases and Airflow components are designed to run within Docker containers, so that
it's easy to set it up with a `docker-compose` file. The setup for this architecture is supposed to run on
Windows OS, but a very similar setup can be done to run on Linux as well.

I will be using Great Expectations `v0.13.25` and the Version 3 API, Docker `v20.10.7` (and Docker Desktop in Windows),
`docker-compose` `v1.29.2`, Python `v3.8.10` for the containers (`v3.9.5` on local machine), and Windows OS (Windows 10).

[**Google Sheet to fill out Great Expectations configuration**](https://docs.google.com/spreadsheets/d/1yO5cSmHFrE78R7kcUfP7fNzZcRpgG5eieApFwZ0f8oY/edit?usp=sharing)

[**Official Documentation**](https://docs.greatexpectations.io/en/latest/) - the new official documentation

[**Legacy Documentation**](https://legacy.docs.greatexpectations.io/en/latest/) - old legacy docs

**Repo directory structure:**

    .
    ├── dags
    │   ├── scripts
    │   │   ├── python
    │   │   │   ├── checkpoints.py
    │   │   │   ├── datasources.py
    │   │   │   └── expectations.py   
    │   │   └── sql
    │   │       ├── extract_load_retail_source.sql
    │   │       ├── load_retail_stage.sql
    │   │       └── transform_load_retail_warehouse.sql
    │   ├── retail_data_pipeline.py
    │   ├── transformations.py
    │   └── validations.py
    ├── database-setup
    │   ├── destinationdb.sql
    │   └── sourcedb.sql
    ├── debugging
    │   ├── debug.log
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
    │   │   ├── example_suite.json
    │   │   ├── retail_dest_suite.json
    │   │   ├── retail_source_suite.json
    │   │   └── test_suite.json
    │   ├── uncommitted
    │   │   └── config_variables.yml
    │   ├── .gitignore
    │   └── great_expectations.yml
    ├── source-data
    │   ├── retail_profiling.csv
    │   └── retail_validating.csv
    ├── .dockerignore
    ├── .gitattributes
    ├── .gitignore
    ├── Dockerfile
    ├── README.md
    ├── airflow_conn.ps1
    ├── docker-compose.yml
    └── requirements.txt

## Setup

Clone this repository,  

    git clone https://github.com/ismaildawoodjee/GreatEx; cd GreatEx

Prepare a Python virtual environment,

    python -m venv .venv; .venv\Scripts\Activate.ps1

Install Great Expectations and required dependencies with

    pip install -r requirements.txt

And initialize the Airflow containers. The Dockerfile extends the Airflow image to
install Python libraries within the containers as well.

    docker-compose up airflow-init

Start the containers in the background with:

    docker-compose up --build -d

After this, we want to add our Postgres database connections to Airflow, for which I have provided
a convenience script with `airflow_conn.ps1`. Run this Powershell script with:

    .\airflow_conn.ps1

If you are using VS Code, you can check the health of the containers with the Docker extension:

![Container health from Docker extension](assets/images/container_health.png)

Otherwise, type `docker ps -a` into the terminal to check their health status.
Once the Airflow Worker and Webserver are in a healthy state, you can go to `localhost:8080` in
the webbrowser to log into the Airflow UI and observe the data pipeline.

## About the `docker-compose` File

The original `docker-compose` file for setting up Airflow containers was obtained from
[Apache Airflow instructions](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
for running Airflow in Docker. I modified it in several different ways:

- Set `AIRFLOW__CORE__LOAD_EXAMPLES` to `false` to declutter the Airflow UI.

- An additional environment variable `AIRFLOW_CONN_POSTGRES_DEFAULT` for the Airflow meta database, which I renamed to `postgres-airflow`.

- I wrote a Dockerfile to extend the image (according to [the official docs](https://airflow.apache.org/docs/docker-stack/build.html#extending-the-image))
  so that the Great Expectations library and other packages will be installed inside the containers.

- Added two additional PostgresDB containers `postgres-source` and `postgres-dest` to represent the retail data source and the destination data warehouse.

- For Airflow to properly orchestrate these containers, their volumes and port mappings were also specified, e.g. "5433:5432" for the source database so that it
  won't conflict with Airflow's meta database mapping "5432:5432".

- The `great_expectations` folder, `filesystem` folder, `database-setup`, `source-data` and `dest-data` folders also need to be
  mounted as volumes to the appropriate containers so that Airflow can access them.

- The service names for the containers are also their hostnames, so we should keep that in mind
  (else Docker will keep searching for the wrong services and won't find them). Below, I changed the service name from the default `postgres` to `postgres-airflow`,
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

Except for the very first step of creating a Data Context (the Great Expectations folder and associated files), all of the
configuration for connecting to Datasources, creating Expectation Suites, creating Checkpoints and validating them, will be
done using Python scripts, without any Jupyter Notebooks. I'll be using Great Expectations Version 3 API.

1. The first step is to create a Data Context from the command line:

        great_expectations --v3-api init

    This will create a `great_expectations` folder.

2. The next step is to connect to Datasources. Here, we have 5 Datasources, three of them are tables in relational databases,
   one is a CSV file in the raw directory, and another is a Parquet file in the stage directory.

      - Postgres source database `sourcedb` with schema `ecommerce`, where the source data is located (`postgres-source` container)
      - Raw directory in the `filesystem` (`filesystem/raw`)
      - Stage directory in the `filesystem` (`filesystem/stage`)
      - Postgres destination database `destdb` with schema `stage` (`postgres-dest` container), the penultimate location where transformed data is
        staged before moving it to the public schema.
      - Same destination database but with schema `public`, where the final transformed data is located
