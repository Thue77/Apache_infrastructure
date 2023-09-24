from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from typing import Protocol,List

class DeltaStore(Protocol):
    '''Interface for delta store
    '''
    dataset_name: str

    def get_delta_state(self) -> str:
        '''Get the delta state of the dataset
        '''
        ...
    
    def set_delta_state(self) -> str:
        '''Set the delta state of the dataset
        '''
        ...

class DeltaState:
    '''
    Class to keep track of the delta states of the system. Mainly used to keep track of
    what data has been ingested into the lakehouse.

    Args:
        spark (SparkSession): SparkSession - Must be configured with the correct storage account access
        dataset_name (str): Name of the dataset
        storage_account_name (str): Name of the storage account
        layer (str): Name of the layer. Example: landing
    '''
    def __init__(self, spark: SparkSession, storage_account_name: str, layer: str, dataset_name: str) -> None:
        self.spark = spark
        self.dataset_name = dataset_name
        self.layer = layer
        self.delta_table_name = 'delta_table'
        self.delta_container_name = 'utility'
        self.delta_path = f"abfss://{self.delta_container_name}@{storage_account_name}.dfs.core.windows.net/delta/{self.delta_table_name}"
        self.hudi_options = {
                            'hoodie.table.name': self.delta_table_name,
                            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                            'hoodie.datasource.write.recordkey.field': 'DatasetName,Layer',
                            'hoodie.datasource.write.partitionpath.field': '',
                            'hoodie.datasource.write.table.name': self.delta_table_name,
                            'hoodie.datasource.write.operation': 'upsert',
                            'hoodie.datasource.write.precombine.field': 'DeltaState',
                            'hoodie.upsert.shuffle.parallelism': 1,
                            'hoodie.insert.shuffle.parallelism': 1
                        }

    def get_delta_state(self, default_value: str) -> str:
        '''
        Get the delta state of the dataset
        '''
        try:
            delta_state = (
                self.spark.read.format('hudi').load(self.delta_path)
                    .filter((F.col('DatasetName')==self.dataset_name) & (F.col('Layer')==self.layer))
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
        df = self.spark.createDataFrame([(self.dataset_name, self.layer, delta_state)], ['DatasetName', 'Layer', 'DeltaState'])
        df.write.format('hudi').options(**self.hudi_options).mode('append').save(self.delta_path)