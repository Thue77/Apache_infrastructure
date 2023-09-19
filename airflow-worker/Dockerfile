FROM docker.io/bitnami/airflow-worker:2

ARG spark_version=3.3.3
ARG hadoop_version=3

# Switch to root user to install packages
USER root

# Install java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Install apache spark
RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz --no-same-owner && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}

# Add python requirements file
COPY ./dependencies/requirements.txt ./bitnami/python/requirements.txt

# Install python requirements
RUN source ./opt/bitnami/airflow/venv/bin/activate \
    && python -m pip install -r /bitnami/python/requirements.txt \
    && deactivate

# Remove requirements.txt
RUN rm ./bitnami/python/requirements.txt

# Switch back to airflow user
USER 1001