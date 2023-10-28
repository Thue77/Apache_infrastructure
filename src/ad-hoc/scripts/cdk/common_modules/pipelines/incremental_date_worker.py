from typing import Protocol, Any
from retrying import retry
import datetime
import pathlib
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


class Service(Protocol):
    def get_data(self, start: datetime.datetime, end: datetime.datetime, **kwargs) -> Any:
        pass

@my_logging.module_logger
class IncrementalDateWorker:
    
    def __init__(self, service: Service, service_method: str, service_kwargs: dict, increment: datetime.timedelta, max_state: datetime.datetime, state_store: StateStore) -> None:
        self.increment = increment
        self.max_state = max_state
        self.state_store = state_store  
        self.service = service
        self.service_method = service_method
        self.service_kwargs = service_kwargs

    @my_logging.module_logger
    def execute(self):

        while True:
            # Get the delta state
            delta_state_value = self.state_store.get_delta_state()
            logger.appinfo(f"Delta state: {delta_state_value}")

            # Increment the delta state by one day
            end_date = datetime.strptime(delta_state_value, "%Y-%m-%dT%H:%M") + datetime.timedelta(days=1)

            # Get the data
            logger.info(f"Getting data from {delta_state_value} to {end_date.strftime('%Y-%m-%dT%H:%M')}")
            get_data = getattr(self.service, self.service_method)

            data = get_data(delta_state_value, end_date, **self.service_kwargs)

            try:
                # Load the data into a DataFrame
                df = spark.createDataFrame(data)
            except Exception as e:
                if "empty dataset" in str(e):
                    logger.appinfo(f"No data found. Should be checked manually that there is no data from {delta_state_value} to {end_date.strftime('%Y-%m-%dT%H:%M')}")
                    break
                else:
                    raise e

            # Log the number of rows written
            logger.appinfo(f"Number of rows written: {df.count()}")

            # Write the DataFrame to Hudi
            df.write.format("delta"). \
                mode("append"). \
                save(destination_path)

            # Update the delta state
            delta_state.set_delta_state(end_date.strftime("%Y-%m-%dT%H:%M"))
            # break

            # Break if the end date is greater than or equal to max date
            if end_date >= max_date:
                logger.appinfo("End date is greater than or equal to max date")
                break


    