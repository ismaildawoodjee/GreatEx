#!/bin/bash
# Author: Ismail Dawoodjee
# Date: 24-Aug-2021
# Purpose: To set up containers to run the Great Expectations
# data pipeline on Airflow. Run this script using `source setup.sh`. 

# Sends outputs and errors into both STDOUT and log file
LOG_LOCATION="./debugging"
exec > >(tee -a $LOG_LOCATION/setup.log)
exec 2>&1

START_TIME=$(date +"%s")
DATE_TIME=$(date +"%x %r %Z")
echo -e "$(tput setaf 2)INFO: Started setup at $DATE_TIME\n$(tput sgr 0)"

# For Ubuntu Linux - Check that Docker is running
if [[ uname == 'Ubuntu'* ]]; then
    if [ "$(systemctl is-active docker)" != "active" ]; then
        echo "$(tput setaf 1)ERROR: Docker is not running." \
             "Start Docker with 'sudo service docker start'.$(tput sgr 0)"
        exit 1
    fi
fi

# For Mac OS - Check that Docker is running
if [[ $OSTYPE == 'darwin'* ]]; then
    if (! docker stats --no-stream ); then
      # On Mac OS this would be the terminal command to launch Docker
      open /Applications/Docker.app
    # Wait until Docker daemon is running and has completed initialisation
    while (! docker stats --no-stream ); do
      # Docker takes a few seconds to initialize
      echo "Waiting for Docker to launch..."
      sleep 1
    done
    fi
fi

PROJECT_NAME="greatex"

function prepare_python_environment () {
    echo -e "$(tput setaf 3)INFO: Creating virtual environment and installing dependencies...\n$(tput sgr 0)"
    
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U wheel setuptools
    pip install -r requirements.txt
}

function reinitialize_great_expectations () {
    echo -e "$(tput setaf 3)INFO: Reinitializing Great Expectations. Press 'Y' when prompted.\n$(tput sgr 0)"

    great_expectations --v3-api init
    echo -e "*\nnotebooks\nplugins\n!uncommitted/config_variables.yml" \
        >> "./great_expectations/.gitignore"
}

function setup_environment_variables () {
    echo -e "$(tput setaf 3)INFO: Setting up environment variables. Enter" \
            "Receiver email, and Sender Gmail and password.$(tput sgr 0)"
    echo -e "$(tput setaf 3)Ensure that 'Less Secure Apps' is ON if sending alerts via Gmail.\n$(tput sgr 0)"

    echo -e "SOURCEDB_CONN = postgresql+psycopg2://sourcedb1:sourcedb1@postgres-source:5432/sourcedb
DESTDB_CONN = postgresql+psycopg2://destdb1:destdb1@postgres-dest:5432/destdb
STOREDB_CONN = postgresql+psycopg2://storedb1:storedb1@postgres-store:5432/storedb
SMTP_ADDRESS = smtp.gmail.com
SMTP_PORT = 587" >> .env

    printf "Enter Receiver Email(s),Sender Email, and Sender Password as a single string seperated by a space: "
    IFS=' '
    read RECEIVER_EMAILS SENDER_LOGIN SENDER_PASSWORD
    
    echo -e "SENDER_LOGIN = $SENDER_LOGIN
SENDER_PASSWORD = $SENDER_PASSWORD
RECEIVER_EMAILS = $RECEIVER_EMAILS" >> .env
}

function setup_airflow_containers () {
    echo -e "$(tput setaf 3)\nINFO: Setting up local Airflow infrastructure.\n$(tput sgr 0)"

    echo -e "\nAIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0\nLOCAL_DIRECTORY=$(pwd)" >> .env
    mkdir ./logs ./plugins
    printf "Enter SUDO password to proceed.\n"
    sudo chmod 777 dags/* logs/ plugins/ filesystem/* database-setup/* \
        source-data/* dest-data/ great_expectations/*

    echo -e "$(tput setaf 3)INFO: Initializing Airflow containers...\n$(tput sgr 0)"
    until sudo docker-compose up --build airflow-init; do
        sleep 30  # retry if containers are still in an unhealthy state
    done

    echo -e "$(tput setaf 3)INFO: Starting containers in detached mode...\n$(tput sgr 0)"
    sudo docker-compose up --build -d

    echo -e "$(tput setaf 3)\nINFO: Waiting 2 minutes to let Airflow containers reach a healthy state.\n$(tput sgr 0)"
    sleep 60
    echo -e "$(tput setaf 3)INFO: One minute left...\n"
    sleep 60
}

function add_database_connections () {
    echo -e "$(tput setaf 3)INFO: Adding Postgres database connections to Airflow...\n$(tput sgr 0)"
    sudo docker exec -d "$PROJECT_NAME"_airflow-webserver_1 \
        airflow connections add "postgres_source" \
        --conn-type "postgres" \
        --conn-login "sourcedb1" \
        --conn-password "sourcedb1" \
        --conn-host "postgres-source" \
        --conn-port 5432 \
        --conn-schema "sourcedb"
        
    sudo docker exec -d "$PROJECT_NAME"_airflow-webserver_1 \
        airflow connections add "postgres_dest" \
        --conn-type "postgres" \
        --conn-login "destdb1" \
        --conn-password "destdb1" \
        --conn-host "postgres-dest" \
        --conn-port 5432 \
        --conn-schema "destdb"

    echo -e "$(tput setaf 3)INFO: Opening Airflow UI in web browser...\n$(tput sgr 0)"
    python3 -m webbrowser 'http://localhost:8080'
}

prepare_python_environment
reinitialize_great_expectations
setup_environment_variables
setup_airflow_containers
add_database_connections

END_TIME=$(date +"%s")
DURATION=$((END_TIME - START_TIME))
echo "$(tput setaf 2)INFO: Completed setup within $DURATION seconds.$(tput sgr 0)"