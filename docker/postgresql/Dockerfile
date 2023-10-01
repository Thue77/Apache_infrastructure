FROM docker.io/bitnami/postgresql:15

USER root

RUN mkdir -p /bitnami/postgresql/data

RUN chown -R 1001:root /bitnami/postgresql && \
    chmod 777 /bitnami/postgresql && \
    chmod 777 /bitnami/postgresql/data

USER root