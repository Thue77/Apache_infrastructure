# bin/bash

cd docker

bash build.sh

cd ..

bash install.sh

pwsh env_vars_in_files.ps1

docker-compose up -d

docker exec -it superset bash superset_init.sh