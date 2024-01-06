import datetime
import os
from pyclbr import Class
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging
from pathlib import Path

from cdk.common_modules.access.secrets import Secrets
from cdk.common_modules.spark.spark_config import SparkConfig
from cdk.common_modules.spark.spark_session_builder import SparkSessionBuilder
from cdk.common_modules.delta_stores.delta_state import DeltaState
from cdk.common_modules.pipelines.bronze_date_worker import BronzeIncrementalDateWorker
from cdk.common_modules.models.connectors.hudi import HudiConnector
from cdk.common_modules.models.file import File
from cdk.common_modules.utility.logging import Logger

from cdk.services.sources.csv_reader import CsvReader
from cdk.services.sinks.delta_writer import DeltaWriter

from transformers.bronze.nordea_stock_transactions import Transformer

# Set logging
Logger = Logger(Path(__file__).stem)

Logger.addLoggingLevel('APPINFO', logging.INFO - 5)

logger = Logger.get_logger()

# Set variables
logger.appinfo("Setting variables")
source_dataset_name = "stock_transactions"
source_dataset_path = "nordea"
source_container_name = "landing"
destination_dataset_name = "stock_transactions"
destination_dataset_path = "nordea"
destination_container_name = "bronze"
output_format = "delta"
FULL_LOAD = False
storage_account_name = "adlsthuehomelakehousedev"
delta_path = f"abfss://utility@{storage_account_name}.dfs.core.windows.net/delta"
landing_path = f"abfss://{source_container_name}@{storage_account_name}.dfs.core.windows.net/{source_dataset_path}/{source_dataset_name}"
bronze_path = f"abfss://{destination_container_name}@{storage_account_name}.dfs.core.windows.net/{output_format}/{destination_dataset_path}/{destination_dataset_name}"


# Create file object
logger.appinfo("Creating file object")
source_file = File(name='Transaktioner',path=landing_path,type='csv',partitions=['year','month','day'])
destination_file = File('transactions',path=bronze_path,type='delta')

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



# Create connector for delta state
logger.appinfo("Creating connector for delta state")
state_connector = HudiConnector(from_dataset_name=os.path.join(source_dataset_path,source_dataset_name),
                                to_dataset_name=destination_dataset_name,
                                to_layer=destination_container_name,
                                spark=spark,
                                delta_entity_name='delta_state',
                                delta_path=delta_path,
                                default_value=datetime.datetime(2023,12,1,0,0,0,0,datetime.timezone.utc)
                                )

# Create DeltaState object
logger.appinfo("Creating DeltaState object")
delta_state = DeltaState(state_connector).get_delta_store()

# Define schema
schema = T.StructType([
    T.StructField("Status", T.StringType(), True),
    T.StructField("Instrument navn", T.StringType(), True),
    T.StructField("Opdateret den", T.StringType(), True),
    T.StructField("Transaktionstype", T.StringType(), True),
    T.StructField("Antal", T.StringType(), True),
    T.StructField("Total", T.StringType(), True)
])

# Define generated columns
add_columns = [
    ('year', 'INT', 'year(to_date(Opdateret_den,"dd-MM-yyyy"))')
]

# Create Reader, Writer and Transformer objects
logger.appinfo("Creating Reader and Writer objects")
data_reader=CsvReader(spark=spark, file=source_file, schema=schema, header=True, sep=";")
data_writer=DeltaWriter(spark=spark, mode='append', partition_columns=['year'], add_columns=add_columns) if not FULL_LOAD else DeltaWriter(spark=spark, mode='overwrite', partition_columns=['year'], add_columns=add_columns)

# Create Transformer object
logger.appinfo("Creating Transformer object")
transformer=Transformer()

# Create BronzeIncrementalDateWorker object
bronze_incremental_date_worker = BronzeIncrementalDateWorker(data_reader=data_reader, data_writer=data_writer, transformer=transformer, state_store=delta_state, file=destination_file, full_load=FULL_LOAD)
bronze_incremental_date_worker.execute()