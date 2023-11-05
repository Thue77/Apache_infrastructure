import pathlib
import datetime

from cdk.services.api.el_overblik import ElOverblik
from cdk.common_modules.access.secrets import Secrets
from cdk.common_modules.spark.spark_config import SparkConfig
from cdk.common_modules.spark.spark_session_builder import SparkSessionBuilder
from cdk.common_modules.models.connectors.hudi import HudiConnector
from cdk.common_modules.delta_stores.delta_state import DeltaState
from cdk.common_modules.models.file import File
from cdk.services.sinks.azure_blob_writer import AzureBlobWriter
from cdk.common_modules.models.class_time_interval_method import ClassTimeIntervalMethod
from cdk.common_modules.pipelines.incremental_date_worker import LandingIncrementalDateWorker
import cdk.common_modules.utility.logging as my_logging
import logging

# Set logging
logger = logging.getLogger(pathlib.Path(__file__).stem)


logger.info('Setting variables')
storage_account_name = "adlsthuehomelakehousedev"
# Set dataset name and landing path
dataset_name = "electricity_consumption_timeseries"
data_source = "el_overblik"
file = File(name=dataset_name, path=data_source + '/', type='json')
container_name = "landing"
delta_path = f"abfss://utility@{storage_account_name}.dfs.core.windows.net/delta"
secret_store = Secrets()

# Create spark session
logger.info("Creating spark session")
spark_config = SparkConfig(secret_store)

# Add jars to install
logger.info("Adding jars to install")
spark_config.add_jars_to_install(['hudi', 'azure_storage'])

# Add storage account access
logger.info("Adding storage account access")
spark_config.add_storage_account_access(storage_account_name, method='access_key')

# Build SparkSession
logger.info("Building SparkSession")
spark = SparkSessionBuilder(pathlib.Path(__file__).stem, spark_config).build()


# Create connector for delta state
logger.info("Creating connector for delta state")
delta_state_connector = HudiConnector(
    from_dataset_name=data_source,
    to_dataset_name=dataset_name,
    to_layer=container_name,
    spark=spark,
    delta_entity_name='delta_state',
    delta_path=delta_path,
    default_value=datetime.datetime(2023, 10, 20)
)

# Create DeltaState object
logger.info("Creating DeltaState object")
delta_state = DeltaState(delta_state_connector).get_delta_store()

# Create class time interval method
logger.info("Creating class time interval method")
class_time_interval_method = ClassTimeIntervalMethod(
    class_instance=ElOverblik(secret_store),
    method_name='get_timeseries_data',
    method_kwargs={'aggregate': 'Actual'}
)

# Create data writer
logger.info("Creating data writer")
data_writer = AzureBlobWriter(storage_account_name=storage_account_name, container_name=container_name, secret_store=secret_store)

# Create worker
logger.info("Creating worker")
worker = LandingIncrementalDateWorker(
    data_method=class_time_interval_method,
    data_writer=data_writer,
    increment=datetime.timedelta(days=1),
    max_state=datetime.datetime.today() - datetime.timedelta(days=1),
    state_store=delta_state,
    file=file
)

# Execute worker
logger.info("Executing worker")
worker.execute()


