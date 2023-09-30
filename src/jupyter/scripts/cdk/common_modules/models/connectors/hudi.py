from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from typing import Protocol,List
from dataclasses import dataclass

@dataclass
class HudiConnector():
    '''
    Interface for connector object that holds the information needed by Hudistore

    Args:
        from_dataset_name (str): Name of the source dataset
        to_dataset_name (str): Name of the dataset related to the delta state is located
        group_name (str): Name of the folder in which the dataset related to the delta state is located
        to_layer (str): Name of the layer in which the dataset related to the delta state is located
        spark (SparkSession): SparkSession - Must be configured with the correct storage account access
        delta_entity_name (str): Name of the entity
        delta_path (str): Path to the delta table. Varies depending on selection of storage solution
    '''
    from_dataset_name: str
    to_dataset_name: str
    group_name: str
    to_layer: str
    spark: SparkSession
    delta_entity_name: str
    delta_path: str
    connector_type = 'hudi'