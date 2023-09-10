# Apache_infrastructure
Project to define an open source development environment for Data Lake projects. The project is only intened to be used for experimenting with Data Lake infrastructures based on Apache technologies. No security is implemented.

## Getting started
The project may be started by first running `build.ps1` and then spinning up all services with Docker-Compose

### Nifi Settings
To connect Nifi to the registry and use the code for [Nifi](/src/nifi/) add the registry with the uri `http://myregistry:18080`.
Nifi writes to the folder `/user/nifi` in HDFS.

To access Azurite, the uri `http://azurite:10000/devstoreaccount1` should be used for the controller `AzureStorageEmulatorCredentialsControllerService`

### Apache Hudi
Hudi is not added beforehand, but is downloaded as part of the initiation of the sparkSession in [hudi_test](/src/jupyter/hudi_test.ipynb)

