from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from typing import Protocol,List

class StateConnector(Protocol):
    '''
    Interface for connector object that holds the information needed by Hudistore

    Args:
        from_dataset_name (str): Name of the source dataset
        to_dataset_name (str): Name of the dataset related to the delta state is located
        to_layer (str): Name of the layer in which the dataset related to the delta state is located
        spark (SparkSession): SparkSession - Must be configured with the correct storage account access
        delta_entity_name (str): Name of the entity
        delta_path (str): Path to the delta table. Varies depending on selection of storage solution
    '''
    from_dataset_name: str
    to_dataset_name: str
    to_layer: str
    spark: SparkSession
    delta_entity_name: str
    delta_path: str


class HudiDateStore:
    '''
    Class to keep track of the delta states of the system. The state is a datetime object. The state is stored in a hudi table.

    Args:
        spark (SparkSession): SparkSession - Must be configured with the correct storage account access
        dataset_name (str): Name of the dataset
        storage_account_name (str): Name of the storage account
        layer (str): Name of the layer. Example: landing
    '''
    def __init__(self, state_connector: StateConnector) -> None:
        self.spark = state_connector.spark
        self.from_dataset_name = state_connector.from_dataset_name
        self.to_dataset_name = state_connector.to_dataset_name
        self.to_layer = state_connector.to_layer
        self.delta_table_name = state_connector.delta_entity_name
        self.delta_path = state_connector.delta_path + '/' + self.delta_table_name 
        self.hudi_options = {
                            'hoodie.table.name': self.delta_table_name,
                            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                            'hoodie.datasource.write.recordkey.field': 'FromDatasetName,ToDatasetName,ToLayer',
                            'hoodie.datasource.write.partitionpath.field': '',
                            'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
                            'hoodie.datasource.write.table.name': self.delta_table_name,
                            'hoodie.datasource.write.operation': 'upsert',
                            'hoodie.datasource.write.precombine.field': 'DeltaState',
                            'hoodie.upsert.shuffle.parallelism': 1,
                            'hoodie.insert.shuffle.parallelism': 1
                        }

    def get_delta_state(self, default_value: str) -> str:
        '''
        Get the delta state of the dataset. If a default value is provided, then the default value is returned if the row does not exist.
        '''
        try:
            delta_state = (
                self.spark.read.format('hudi').load(self.delta_path)
                    .filter((F.col('ToDatasetName')==self.to_dataset_name) & (F.col('FromDatasetName')==self.from_dataset_name))
                    .select('DeltaState').collect()[0][0]
            )
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                '''If the row does not exist, then the row is created with the default value and the default value is returned'''
                delta_state = default_value
                self.set_delta_state(delta_state)
            else:
                raise e
        return delta_state
    
    def set_delta_state(self, delta_state: str) -> None:
        '''
        Set the delta state of the dataset
        '''
        df = self.spark.createDataFrame([(self.from_dataset_name, self.to_dataset_name, self.to_layer, delta_state)], ['FromDatasetName', 'ToDatasetName', 'ToLayer', 'DeltaState'])
        df.write.format('hudi').options(**self.hudi_options).mode('append').save(self.delta_path)