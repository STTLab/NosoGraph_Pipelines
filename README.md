This repository is a part of NosoGraph Pipeline, it handles pipeline user interface for configuring assembly jobs.
We use Apache Airflow to manage the workflow, predesigned pipeline are given in `dags` folder, alternatively, you can clone this repository and run with Docker
using `docker-compose up` command, make sure you have administrative privileges to run Docker.

Once the service is running, you can access the web interface with your browser with address (localhost:8080)[localhost:8080]
The default password for accessing the Airflow interface is
username: `airflow`
password: `airflow`
