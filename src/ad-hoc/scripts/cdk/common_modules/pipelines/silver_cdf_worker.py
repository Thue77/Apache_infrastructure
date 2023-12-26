from math import log
from typing import Protocol, Any, List, Tuple
from pyspark.sql import DataFrame
from retrying import retry
import datetime
import pathlib

from cdk.common_modules.models.file import File
from cdk.common_modules.models.generic_transformer import GenericTransformer
import cdk.common_modules.utility.logging as my_logging
import logging

# Set logging
logger = logging.getLogger(pathlib.Path(__file__).stem)


class StateStore(Protocol):
    dataset_name: str
    layer_name: str
    
    def get_delta_state(self) -> Any:
        pass

    def set_delta_state(self, delta_state: datetime.datetime) -> None:
        pass

    def increment_delta_state(self, increment: datetime.timedelta) -> None:
        pass


class DataWriter(Protocol):
    def write_data(self, df: DataFrame, file: File) -> None:
        pass

class DataReader(Protocol):
    def read_data(self, interval: Tuple[datetime.datetime,datetime.datetime]) -> DataFrame:
        pass

@my_logging.module_logger
class SilverIncrementalCdfWorker:
    '''
    Class to perform incremental load to silver zone from bronze where data is stored s.t. the DataReader can use a start timestamp
    to query the latest data.

    The class uses a reader, to read data from the bronze zone, and a writer to write the data to the silver zone. 
    
    Args:
        data_reader (DataReader): DataReader object
        data_writer (DataWriter): DataWriter object
        transformers (List[GenericTransformer]): List of GenericTransformer objects
        state_store (StateStore): StateStore object
        full_load (bool): Whether to perform a full load or not
    '''
    
    def __init__(self, data_reader: DataReader, data_writer: DataWriter, transformers: List[GenericTransformer], state_store: StateStore, full_load = False) -> None:
        self.state_store = state_store  
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.transformers = transformers
        self.full_load = full_load

    @my_logging.module_logger
    def execute(self):
        if self.full_load:
            delta_state_value = datetime.datetime(2023, 12, 1, tzinfo=datetime.timezone.utc)
        else:
            # Get the delta state
            delta_state_value = self.state_store.get_delta_state()
            delta_state_value = delta_state_value.replace(tzinfo=datetime.timezone.utc)

        logger.info(f"Delta state: {delta_state_value}")

        # Get current utc time
        logger.info(f"Getting current utc time")
        current_utc_time = datetime.datetime.now(datetime.timezone.utc)

        df = self.data_reader.read_data((delta_state_value, current_utc_time))
        count = df.count()

        if count > 0:
            # Transform the data
            logger.info(f"Transforming data")
            for transformer in self.transformers:
                transform_data = getattr(transformer.class_instance, transformer.method_name)
                df = transform_data(df, **transformer.method_kwargs)

            # Write data to sink
            logger.info(f"Writing data to sink")
            self.data_writer.write_data(df, self.file)

            # Update the delta state
            self.state_store.set_delta_state(current_utc_time)


    