from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
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


logger.appinfo('Setting variables')
storage_account_name = "adlsthuehomelakehousedev"
# Set dataset name and landing path
dataset_name = "ConsumptionDK3619codehour"
data_source = "energi_data_service"
dataset_path = data_source + '/' + dataset_name
container_name = "stage"

# landing_path = f"hdfs://namenode:9000/data/landing/energi_data_service/{dataset_name}"
destination_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/delta/{dataset_path}"

delta_path = f"abfss://utility@{storage_account_name}.dfs.core.windows.net/delta"

# Instantiate blob service client from connection string
logger.appinfo("Instantiating blob service client")
blob_service_client = BlobServiceClient.from_connection_string(Secrets().get_secret(f'ADLS_{storage_account_name}_connection_string'))

# The api will have a lag of 9 days
max_date = datetime.today() - timedelta(days=13) 

# # Set Spark configurations
# logger.appinfo("Setting Spark configurations")
# spark_config = SparkConfig(Secrets())

# # Add jars to install
# logger.appinfo("Adding jars to install")
# spark_config.add_jars_to_install(['delta', 'hudi', 'azure_storage'])

# # Add storage account access
# logger.appinfo("Adding storage account access")
# spark_config.add_storage_account_access(storage_account_name, method='access_key')

# # Build SparkSession
# logger.appinfo("Building SparkSession")
# spark = SparkSessionBuilder("Delta ElectricityConsumptionIndustry", spark_config).build()


# Create connector for delta state
# logger.appinfo("Creating connector for delta state")
# state_connector = HudiConnector(from_dataset_name=dataset_name,
#                                 to_dataset_name=dataset_name,
#                                 group_name=data_source,
#                                 to_layer=container_name,
#                                 spark=spark,
#                                 delta_entity_name='delta_state',
#                                 delta_path=delta_path
#                                 )

# Create DeltaState object
logger.appinfo("Creating DeltaState object")
# delta_state = DeltaState(state_connector).get_delta_store()

while True:
    # Get the delta state
    delta_state_value = '2023-09-15T00:00' #delta_state.get_delta_state(default_value='2023-09-15T00:00')
    logger.appinfo(f"Delta state: {delta_state_value}")

    # Increment the delta state by one day
    end_date = datetime.strptime(delta_state_value, "%Y-%m-%dT%H:%M") + timedelta(days=1)

    # Get the data
    logger.appinfo(f"Getting data from {delta_state_value} to {end_date.strftime('%Y-%m-%dT%H:%M')}")
    data = EnergiDataService(dataset_name).get_data(delta_state_value, end_date.strftime("%Y-%m-%dT%H:%M"), raw=True)

    # Write Json data to blob
    logger.appinfo(f"Writing data to blob")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{dataset_path}_{delta_state_value}_{end_date}.json")
    blob_client.upload_blob(str(data), overwrite=True)

    break
