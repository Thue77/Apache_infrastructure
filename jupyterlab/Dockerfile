FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.3
ARG jupyterlab_version=4.0.5

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# Install libraries
COPY requirements.txt ./project_dependencies/
RUN python3 -m pip install -r ./project_dependencies/requirements.txt

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=