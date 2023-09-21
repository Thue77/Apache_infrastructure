from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import json
from cdk.services.api.energi_data_service import EnergiDataService

# Set SAS token
sas_token = "<SAS TOKEN>"
access_key = "<ACCESS KEY>"

storage_account_name = "adlsthuehomelakehousedev"

# Create a SparkSession
spark = (
    SparkSession.builder.appName("electricity_consumption_industry")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config(
        "spark.jars.packages",
        "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,org.apache.hadoop:hadoop-azure:3.3.3"
    ) # It is very important that the version of hadoop-azure matches the version of hadoop in the cluster. It seems that it works to use spark 3.3.3 and hadoop-azure 3.3.3
    .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",access_key)
    # .config(f"spark.hadoop.fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
    # .config(f"spark.hadoop.fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    # .config(f"spark.hadoop.fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
    # .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1')
    .getOrCreate()
)


# Set dataset name and landing path
dataset_name = "ConsumptionDK3619codehour"
dataset_path = "energi_data_service"
container_name = "landing"

# landing_path = f"hdfs://namenode:9000/data/landing/energi_data_service/{dataset_name}"
landing_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{dataset_path}"


# Set connection to ADLS Gen2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    access_key)


# Set Hudi options
hudi_options = {
    'hoodie.table.name': dataset_name,
    # 'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
    'hoodie.datasource.write.recordkey.field': 'HourUTC,DK19Code,DK36Code',
    'hoodie.datasource.write.partitionpath.field': '',
    'hoodie.datasource.write.table.name': dataset_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'HourUTC',
    'hoodie.upsert.shuffle.parallelism': 1,
    'hoodie.insert.shuffle.parallelism': 1
}



data = EnergiDataService(dataset_name).get_data("2023-09-02T00:00", "2023-09-03T00:00")

# Load the data into a DataFrame
df = spark.createDataFrame(data)

# df.show(10,truncate=False)

# df.repartition(1).write.mode("overwrite").parquet(landing_path)

df.write.format("org.apache.hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(landing_path)
# print(data)

# with open('mydata.json', "w") as f:
#     f.write(str(data))