FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.3
ARG jupyterlab_version=4.0.5
ARG python_version=python3.9

RUN apt-get update -y && \
    # source ./opt/bitnami/python/bin/activate && \
    # apt-get install -y ${python_version}-pip && \
    python -m pip install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# Install libraries
COPY requirements.txt ./project_dependencies/
RUN python -m pip install -r ./project_dependencies/requirements.txt

# -- Runtime

USER root

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=