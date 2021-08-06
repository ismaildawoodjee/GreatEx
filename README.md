# Airflow Data Pipeline Validation with Great Expectations

## Introduction

## Setup

Clone this repository, prepare a Python virtual environment and initialize the Airflow containers.

    git clone https://github.com/ismaildawoodjee/GreatEx

    cd GreatEx

    python -m venv .venv; .venv\Scripts\Activate.ps1

    docker-compose up airflow-init

Start the containers in the background with:

    docker-compose up -d

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
- The service names for the containers are also their hostnames, so we should keep that in mind
  (else Docker will keep searching for the wrong services and won't find them). Below, I changed the service name from the default `postgres` to `postgres-airflow`,
  so the hostname in the connection strings must also be changed:

    ![service_name == hostname](assets/images/service_name_hostname.png)

  This also applies to username, password and database names.
- The Docker images for Airflow and Postgres can be any supported version, but I specified them to use the `latest` tag, and used Python version 3.8.
