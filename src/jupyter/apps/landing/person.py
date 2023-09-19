from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = (
    SparkSession.builder.appName("landing_person")
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)


tableName = "person.json"
basePath = f"hdfs://namenode:9000/user/nifi/my_folder/{tableName}"


df = spark.read.json(basePath)


outPath = f"hdfs://namenode:9000/datalake/landing/manual/{tableName}"
df.repartition(1).write.mode("overwrite").parquet(outPath)