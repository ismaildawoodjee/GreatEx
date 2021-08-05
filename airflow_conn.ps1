# Run this script after initializing Airflow containers

$PROJECT_NAME = 'greatex'

docker exec -d $PROJECT_NAME'_airflow-webserver_1' `
    airflow connections add 'postgres_source' `
    --conn-type 'postgres' `
    --conn-login 'sourcedb1' `
    --conn-password 'sourcedb1' `
    --conn-host 'postgres-source' `
    --conn-port 5432 `
    --conn-schema 'sourcedb'
    
docker exec -d $PROJECT_NAME'_airflow-webserver_1' `
    airflow connections add 'postgres_dest' `
    --conn-type 'postgres' `
    --conn-login 'destdb1' `
    --conn-password 'destdb1' `
    --conn-host 'postgres-dest' `
    --conn-port 5432 `
    --conn-schema 'destdb'