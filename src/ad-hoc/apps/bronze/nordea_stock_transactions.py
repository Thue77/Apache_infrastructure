import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
from pathlib import Path

from cdk.services.api.energi_data_service import EnergiDataService
from cdk.common_modules.access.secrets import Secrets
from cdk.common_modules.spark.spark_config import SparkConfig
from cdk.common_modules.spark.spark_session_builder import SparkSessionBuilder
from cdk.common_modules.models.connectors.hudi import HudiConnector
from cdk.common_modules.delta_stores.delta_state import DeltaState
from cdk.common_modules.utility.logging import Logger

# Set logging
Logger = Logger(Path(__file__).stem)

Logger.addLoggingLevel('APPINFO', logging.INFO - 5)

logger = Logger.get_logger()

storage_account_name = "adlsthuehomelakehousedev"

# Set Spark configurations
logger.appinfo("Setting Spark configurations")
spark_config = SparkConfig(Secrets())

# Add jars to install
logger.appinfo("Adding jars to install")
spark_config.add_jars_to_install(['hudi', 'azure_storage','delta'])

# Add storage account access
logger.appinfo("Adding storage account access")
spark_config.add_storage_account_access(storage_account_name, method='access_key')

# Build SparkSession
logger.appinfo("Building SparkSession")
spark = SparkSessionBuilder("Nordea Transactions", spark_config).build()

# Set dataset name and landing path
source_dataset_name = "stock_transactions"
source_dataset_path = "nordea"
source_container_name = "landing"
destination_dataset_name = "stock_transactions"
destination_dataset_path = "nordea"
destination_container_name = "bronze"

FULL_LOAD = False


delta_path = f"abfss://utility@{storage_account_name}.dfs.core.windows.net/delta"

landing_path = f"abfss://{source_container_name}@{storage_account_name}.dfs.core.windows.net/{source_dataset_path}/{source_dataset_name}"
bronze_path = f"abfss://{destination_container_name}@{storage_account_name}.dfs.core.windows.net/{destination_dataset_path}/{destination_dataset_name}"



# Set Hudi options
hudi_options = {
    'hoodie.table.name': destination_dataset_name,
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
    'hoodie.datasource.write.recordkey.field': 'HourUTC,DK19Code,DK36Code',
    'hoodie.datasource.write.partitionpath.field': '',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.table.name': destination_dataset_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'HourUTC',
    'hoodie.upsert.shuffle.parallelism': 1,
    'hoodie.insert.shuffle.parallelism': 1
}


# Create connector for delta state
logger.appinfo("Creating connector for delta state")
state_connector = HudiConnector(from_dataset_name=source_dataset_name,
                                to_dataset_name=destination_dataset_name,
                                group_name=source_dataset_path,
                                to_layer=destination_container_name,
                                spark=spark,
                                delta_entity_name='delta_state',
                                delta_path=delta_path
                                )

# Create DeltaState object
logger.appinfo("Creating DeltaState object")
delta_state = DeltaState(state_connector).get_delta_store()

if not FULL_LOAD:
    # Get the delta state
    delta_state_value = delta_state.get_delta_state(default_value='2023-09-01T00:00')
    logger.appinfo(f"Delta state: {delta_state_value}")

    # Increment the delta state by one day
    start_date = datetime.datetime.strptime(delta_state_value, "%Y-%m-%dT%H:%M") + datetime.timedelta(days=1)
else:
    start_date = datetime.datetime(1970,1,1,0,0,0)

# Get the data
logger.appinfo(f"Getting data from {start_date.strftime('%Y-%m-%dT%H:%M')}")
try:
    # TODO: Read with schema
    # Define schema
    schema = T.StructType([
        T.StructField("year", T.StringType(), True),
        T.StructField("month", T.StringType(), True),
        T.StructField("day", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
        T.StructField("Instrument navn", T.StringType(), True),
        T.StructField("Opdateret den", T.StringType(), True),
        T.StructField("Transaktionstype", T.StringType(), True),
        T.StructField("Antal", T.StringType(), True),
        T.StructField("Total", T.StringType(), True)
    ])
    df = (
            spark.read.csv(landing_path, header=True, sep=";", schema=schema)
            .withColumnRenamed("Opdateret den", "Opdateret_den")
            .withColumnRenamed("Instrument navn", "Instrument_navn")
            .withColumn("date", F.to_date(F.concat_ws('-','year','month','day'), "yyyy-MM-dd"))
            .filter((F.col("date")>start_date))
            .withColumn("dwh_updatedAt", F.current_timestamp())
        )
    rows = df.count()
except Exception as e:
    if "empty dataset" in str(e):
        logger.appinfo(f"No data found. Should be checked manually that there is no data from {delta_state_value} to {end_date.strftime('%Y-%m-%dT%H:%M')}")
        rows = 0
    else:
        raise e

# Log the number of rows written
logger.appinfo(f"Number of rows written: {rows}")

if rows > 0:
    # Write the DataFrame to Delta
    # TODO: partition by year
    df.write.format("delta"). \
        mode("append"). \
        save(bronze_path)

    # Select the latest date in the data
    max_date = df.select(F.max("date")).collect()[0][0]

    # Update the delta state
    delta_state.set_delta_state(max_date.strftime("%Y-%m-%dT%H:%M"))
