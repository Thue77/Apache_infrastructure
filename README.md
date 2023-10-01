# Open Data Lake Infrastructure
Project to define an open source development environment for Data Lake projects. The project is only intened to be used for experimenting with Data Lake infrastructures based on Apache technologies. No security is implemented.

The tech stack looks as follows:
-----
![Image](./res/architecture.drawio.png)


## Getting started
- The project may be started by first running `build.ps1` to build all custom images. 
- Git repo should be initialized in [/src/nifi/](/src/nifi/) in order to use the Nifi registry. 
- 
- Add necessary secrets to environment variables. The below steps indicate one way to do so
  - The [.env.temp](.env.temp) shows which secrets are necessary and a new filew `.env` should be created with the values populated
  - Run [dot_env_to_env_vars.ps1](./dot_env_to_env_vars.ps1) to add values from the newly created `.env` file to the environment variables
- Run [env_vars_in_files.ps1](./env_vars_in_files.ps1) to populate files with secretes from the environmnet variables. All occurences like `${azure_client_oauth2_id}` are replaced with the value of an environment variable with the same name 
- Spin up all services with Docker-Compose.
- To configure Apache Superset the first time execute [superset_init.sh](./superset_init.sh) from within the container

### Nifi Settings
To connect Nifi to the registry and use the code for [Nifi](/src/nifi/) add the registry with the uri `http://myregistry:18080`. Note that [/src/nifi/](/src/nifi/) must be a git repo to use the Nifi registry.
Nifi writes to the folder `/user/nifi` in HDFS.

To access Azurite, the uri `http://azurite:10000/devstoreaccount1` should be used for the controller `AzureStorageEmulatorCredentialsControllerService`

### Apache Hudi
Hudi is not added beforehand, but is downloaded as part of the initiation of the sparkSession in [hudi_test](/src/jupyter/hudi_test.ipynb)

### Apache Superset
When connecting Trino to Superset use the following url for local docker instances trino://trino@host.docker.internal:8080/<name of catalog>

### Apache Airflow
When the container is created a connection called "spark_conn" should be created and it should contain information on the jars to be installed. Below is an example:

![spark_conn](./res/spark_conn.png)

### Handle dependencies
In this setup, spark is run in a standalone cluster, which means all spark-submit jobs are executed in client mode. Therefore the driver is executed from the client, which is airflow-worker, and thus airflow-worker needs to have access to all necessary dependencies. This includes local modules and packages installed from PyPi. Volumes have been used to ensure simultanous existence on local modules in airflow-worker and ad-hoc. Furthermore, relevant environment variables are added to both containers in the Docker-compose file

