from typing import Protocol, Any, List
from retrying import retry
import datetime
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
    def write_data(self, data: Any, name: str) -> None:
        pass

@my_logging.module_logger
class LandingIncrementalDateWorker:
    '''
    Class to perform incremental load to landing zone. 
    
    Args:
        data_method (ClassTimeIntervalMethod): ClassTimeIntervalMethod object
        data_writer (DataWriter): DataWriter object
        increment (datetime.timedelta): Increment to increment the delta state by
        max_state (datetime.datetime): Max state to increment the delta state to
        state_store (StateStore): StateStore object
        file (File): File object
    '''
    
    def __init__(self, data_method: ClassTimeIntervalMethod, data_writer: DataWriter, increment: datetime.timedelta, max_state: datetime.datetime, state_store: StateStore, file: File) -> None:
        self.increment = increment
        self.max_state = max_state
        self.state_store = state_store  
        self.data_method = data_method
        self.data_writer = data_writer
        self.file = file

    @my_logging.module_logger
    def execute(self):

        while True:
            # Get the delta state
            delta_state_value = self.state_store.get_delta_state()
            logger.info(f"Delta state: {delta_state_value}")

            # Increment the delta state by one day
            end_datetime = delta_state_value + self.increment

            # Creating file for current delta state
            logger.info(f"Creating file for current delta state")
            original_filename = self.file.name
            self.file.name += f"_{delta_state_value}_{end_datetime}"

            # Get the data
            logger.info(f"Getting data from {delta_state_value} to {end_datetime}")
            get_data = getattr(self.data_method.class_instance, self.data_method.method_name)

            data = get_data(delta_state_value, end_datetime, **self.data_method.method_kwargs)

            # Write data to sink
            logger.info(f"Writing data to sink")
            self.data_writer.write_data(data, self.file)

            # Update the delta state
            self.state_store.set_delta_state(end_datetime)

            self.file.name = original_filename

            # Break if the end date is greater than or equal to max date
            if end_datetime >= self.max_state:
                logger.info("End date is greater than or equal to max date")
                break


    