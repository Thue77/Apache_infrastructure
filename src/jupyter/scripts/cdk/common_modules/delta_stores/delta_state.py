from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from typing import Protocol,List

from cdk.common_modules.delta_stores.hudi_store import HudiStore

class StateConnector(Protocol):
    '''
    Interface for connector object that holds the information needed by Hudistore

    Args:
        connector_type (str): Name of the connector type
    '''
    connector_type: str

class DeltaStore(Protocol):
    '''Interface for delta store
    '''
    dataset_name: str

    def get_delta_state(self, default: str = None) -> str:
        '''Get the delta state of the dataset
        '''
        ...
    
    def set_delta_state(self, delta_state: str) -> str:
        '''Set the delta state of the dataset
        '''
        ...

class DeltaState:
    '''
    Class to instantiate a delta state object. 
    This object is used to keep track of the delta states of the system.
    '''
    def __init__(self, state_connector: StateConnector) -> None:
        self.state_connector = state_connector
    
    def get_delta_store(self) -> DeltaStore:
        '''
        Get the delta store object
        '''
        if self.state_connector.connector_type == 'hudi':
            return self.__get_hudi_store()

    def __get_hudi_store(self) -> HudiStore:
        '''
        Get the hudi store object
        '''
        return HudiStore(self.state_connector)