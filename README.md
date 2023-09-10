# Apache_infrastructure
Project to define an open source development environment for Data Lake projects. The project is only intened to be used for experimenting with Data Lake infrastructures based on Apache technologies. No security is implemented.

## Getting started
The project may be started by first running `build.ps1` to build all custom images. Then a git repo shoul be initialized in [/src/nifi/](/src/nifi/) in order to use the Nifi registry. Last step is to spin up all services with Docker-Compose.

### Nifi Settings
To connect Nifi to the registry and use the code for [Nifi](/src/nifi/) add the registry with the uri `http://myregistry:18080`. Note that [/src/nifi/](/src/nifi/) must be a git repo to use the Nifi registry.
Nifi writes to the folder `/user/nifi` in HDFS.

To access Azurite, the uri `http://azurite:10000/devstoreaccount1` should be used for the controller `AzureStorageEmulatorCredentialsControllerService`

### Apache Hudi
Hudi is not added beforehand, but is downloaded as part of the initiation of the sparkSession in [hudi_test](/src/jupyter/hudi_test.ipynb)

