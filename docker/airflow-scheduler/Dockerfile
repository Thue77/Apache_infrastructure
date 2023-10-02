FROM docker.io/bitnami/airflow-scheduler:2

# Switch to root user to install packages
USER root

# Install java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

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