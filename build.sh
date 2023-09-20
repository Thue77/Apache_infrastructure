# Set input variables
SPARK_VERSION="3.3.3"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="4.0.5"


# Build images

docker build -f base/Dockerfile -t cluster-base:latest .

docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base/Dockerfile -t spark-base:latest .

# docker build -f spark-master/Dockerfile -t spark-master . 

# docker build -f spark-worker/Dockerfile -t spark-worker .

# docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab/Dockerfile -t jupyterlab .

# docker build -f airflow/Dockerfile -t airflow .