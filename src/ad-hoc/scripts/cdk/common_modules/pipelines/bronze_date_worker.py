from math import log
from typing import Protocol, Any, List, Tuple
from pyspark.sql import DataFrame
from retrying import retry
import datetime
from dateutil import relativedelta
import pathlib

from cdk.common_modules.models.class_time_interval_method import ClassTimeIntervalMethod
from cdk.common_modules.models.file import File
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
    def read_data(self, partition_columns: List[Tuple[str,Any]]) -> DataFrame:
        pass

class Transformer(Protocol):
    def transform_data(self, df: DataFrame) -> DataFrame:
        pass

@my_logging.module_logger
class BronzeIncrementalDateWorker:
    '''
    Class to perform incremental load to bronze zone from landing where hive partitioning is used.
    For now the Hive Partitioning is assumed to be date based, i.e. year, month, date.

    The class uses a reader, to read data from the landing zone, and a writer to write the data to the bronze zone. 
    
    Args:
        data_reader (DataReader): DataReader object
        data_writer (DataWriter): DataWriter object
        transformer (Transformer): Transformer object
        increment (datetime.timedelta): Increment to increment the delta state by
        max_state (datetime.datetime): Max state to increment the delta state to
        state_store (StateStore): StateStore object
        file (File): File object
        full_load (bool): If True, the full load is performed, else the incremental load is performed
    '''
    
    def __init__(self, data_reader: DataReader, data_writer: DataWriter, transformer: Transformer, state_store: StateStore, file: File, increment = 'day', max_state = datetime.datetime.now(datetime.timezone.utc), full_load = False) -> None:
        self.increment = increment
        self.max_state = max_state
        self.state_store = state_store  
        self.data_reader = data_reader
        self.data_writer = data_writer
        self.transformer = transformer
        self.file = file
        self.full_load = full_load

    def increment_time_object(self):
        '''
        Returns the increment as a datetime object

        Returns:
            datetime.timedelta: Increment as a datetime object

        Raises:
            ValueError: If the increment is not supported
        '''
        if self.increment == 'day':
            return datetime.timedelta(days=1)
        elif self.increment == 'month':
            return relativedelta.relativedelta(months=+1)
        elif self.increment == 'year':
            return relativedelta.relativedelta(years=+1)
        else:
            raise ValueError(f'Increment {self.increment} is not supported')

    @my_logging.module_logger
    def execute(self):

        increment = self.increment_time_object()

        if self.full_load:
            delta_state_value = datetime.datetime(2023, 12, 1, tzinfo=datetime.timezone.utc)
        else:
            # Get the delta state
            delta_state_value = self.state_store.get_delta_state()
            delta_state_value = delta_state_value.replace(tzinfo=datetime.timezone.utc)

        logger.info(f"Delta state: {delta_state_value}")
        
        # Increment the delta state by one day
        end_datetime: datetime.datetime = delta_state_value + increment

        while True:

            # Creating file for current delta state
            logger.info(f"Creating file for current delta state")
            original_filename = self.file.name
            self.file.name += f"_{delta_state_value}_{end_datetime}"

            # Set the partition columns
            logger.info(f"Setting partition columns")
            partition_columns = [('year', end_datetime.strftime('%Y')), ('month', end_datetime.strftime('%m')), ('day', end_datetime.strftime('%d'))]

            # Get the data
            logger.info(f"Getting data from {end_datetime}")
            try:
                df = self.data_reader.read_data(partition_columns)
                count = df.count()
            except Exception as e:
                logger.warning(f"Error getting data from {end_datetime}: {e}")
                logger.info(f"Setting count to 0")
                count = 0

            if count > 0:
                # Transform the data
                logger.info(f"Transforming data")
                df = self.transformer.transform_data(df)

                # Write data to sink
                logger.info(f"Writing data to sink")
                self.data_writer.write_data(df, self.file)

                # Update the delta state
                self.state_store.set_delta_state(end_datetime)
            
            # Reset the file name
            self.file.name = original_filename

            # Increment the delta state by one day
            end_datetime = end_datetime + increment

            if end_datetime > self.max_state:
                logger.info(f"Reached max state {self.max_state}")
                break


    