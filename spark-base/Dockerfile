FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.3.3
ARG hudi_version=0.13.1
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3



# RUN curl https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.13.1/hudi-spark3.3-bundle_2.12-0.13.1.jar -o 

# RUN curl https://archive.apache.org/dist/hudi/${hudi_version}/hudi-${hudi_version}.src.tgz -o hudi.tgz && \
#     tar -xf hudi.tgz && \
#     mv hudi-${hudi_version} $SPARK_HOME/jars && \
#     rm hudi.tgz

# -- Runtime

WORKDIR ${SPARK_HOME}