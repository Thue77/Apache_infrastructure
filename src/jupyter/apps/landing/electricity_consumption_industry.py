from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import json
from cdk.services.api.energi_data_service import EnergiDataService
from cdk.common_modules.access.secrets import Secrets
from cdk.common_modules.utility.spark_config import SparkConfig
from cdk.common_modules.utility.spark_session_builder import SparkSessionBuilder

storage_account_name = "adlsthuehomelakehousedev"

# Set Spark configurations
spark_config = SparkConfig(Secrets())

# Add jars to install
spark_config.add_jars_to_install(['hudi', 'azure_storage'])

# Add storage account access
spark_config.add_storage_account_access(storage_account_name, method='access_key')

# Build SparkSession
spark = SparkSessionBuilder("ElectricityConsumptionIndustry", "spark://spark-master:7077", spark_config).build()

# Set dataset name and landing path
dataset_name = "ConsumptionDK3619codehour"
dataset_path = "energi_data_service"
container_name = "landing"

# landing_path = f"hdfs://namenode:9000/data/landing/energi_data_service/{dataset_name}"
landing_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{dataset_path}"


# Set Hudi options
hudi_options = {
    'hoodie.table.name': dataset_name,
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
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


df.write.format("org.apache.hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(landing_path)
