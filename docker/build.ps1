# -- Parameter for caching
param(
    [switch]$no_cache = $false
)

# TODO: Set path to file path to avoid problems when running script from elsewhere

# -- Software Stack Version

$SPARK_VERSION="3.3.3"
$HADOOP_VERSION="3"
$JUPYTERLAB_VERSION="4.0.5"
$PYTHON_VERSION="python3.9" # Since spark is standalone, we need to use the same python version as the one used by airflow. This is because spark-submit can only have deploy mode "client".

# -- Building the Images

$no_cache_arg = ""
Write-Host "Building the images with no-cache: $no_cache"

if ($no_cache) {
    $no_cache_arg = "--no-cache"

    docker build $no_cache_arg --build-arg python_version="${PYTHON_VERSION}" -f base/Dockerfile -t cluster-base .

    docker build $no_cache_arg --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base/Dockerfile -t spark-base .

    # docker build $no_cache_arg -f spark-master/Dockerfile -t spark-master . 

    # docker build $no_cache_arg -f spark-worker/Dockerfile -t spark-worker .

    # docker build $no_cache_arg --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab/Dockerfile -t ad-hoc .

    # docker build $no_cache_arg -f airflow/Dockerfile -t airflow .

    # docker build $no_cache_arg -f airflow-worker/Dockerfile -t airflow-worker .

    # docker build $no_cache_arg -f airflow-scheduler/Dockerfile -t airflow-scheduler .
}
else {
    docker build -f base/Dockerfile -t cluster-base .

    docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base/Dockerfile -t spark-base .

    # docker build -f spark-master/Dockerfile -t spark-master . 

    # docker build -f spark-worker/Dockerfile -t spark-worker .

    # docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab/Dockerfile -t ad-hoc .

    # docker build -f airflow/Dockerfile -t airflow .

    # docker build -f airflow-worker/Dockerfile -t airflow-worker .

    # docker build -f airflow-scheduler/Dockerfile -t airflow-scheduler .
}
