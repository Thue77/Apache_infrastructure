# -- Parameter for caching
param(
    [switch]$no_cache = $false
)

# -- Software Stack Version

$SPARK_VERSION="3.3.3"
$HADOOP_VERSION="3"
$JUPYTERLAB_VERSION="4.0.5"

# -- Building the Images

$no_cache_arg = ""
Write-Host "Building the images with no-cache: $no_cache"

if ($no_cache) {
    $no_cache_arg = "--no-cache"

    docker build $no_cache_arg -f base/Dockerfile -t cluster-base .

    docker build $no_cache_arg --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base/Dockerfile -t spark-base .

    docker build $no_cache_arg --no-cache -f spark-master/Dockerfile -t spark-master . 

    docker build $no_cache_arg --no-cache -f spark-worker/Dockerfile -t spark-worker .

    docker build $no_cache_arg --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab/Dockerfile -t jupyterlab .
}
else {
    docker build -f base/Dockerfile -t cluster-base .

    docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base/Dockerfile -t spark-base .

    docker build -f spark-master/Dockerfile -t spark-master . 

    docker build -f spark-worker/Dockerfile -t spark-worker .

    docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab/Dockerfile -t jupyterlab .
}
