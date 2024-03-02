# ARG debian_buster_image_tag=8-jre
FROM bitnami/python:3.9-debian-11
LABEL mantainer="Thomas Sørensen <thomas@thuesoerensen.dk>"

# Switch to root user to install packages
# USER root

# Install java
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME



ARG shared_workspace=/opt/workspace

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]