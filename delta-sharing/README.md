# Temporary instructions for delta sharing

- Pull docker image from DockerHub `docker run -p 8383:8080 --mount type=bind,source=delta-sharing-server.yaml,target=/config/delta-sharing-server-config.yaml deltaio/delta-sharing-server:0.2.0 -- --config /config/delta-sharing-server-config.yaml`
- Run docker container `docker run -p 8383:8080 --mount type=bind,source=C:\Users\thom1\source\repo\open-data-lakehouse\delta-sharing\delta-sharing-server.yaml,target=/config/delta-sharing-server-config.yaml deltaio/delta-sharing-server:0.6.7 -- --config /config/delta-sharing-server-config.yaml`
- Copy core-site.xml to docker container. From delta-sharing folder:  `docker cp .\core-site.xml sad_gates:/opt/docker/conf/core-site.xml`
- Access API at http://localhost:8383/delta-sharing - See all shares here: http://localhost:8383/delta-sharing/shares

__NOTE__: No security is implemented so the bearer token could be anything 