from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
import datetime
from typing import Protocol,List, Tuple, Any
from cdk.common_modules.models.table import Table


class DeltaReader:
    '''
    Class to read data from delta table using a start and end timestamp with the readChangeFeed option.

    Args:  
        spark (SparkSession): SparkSession object
        table (Table): Table object
    '''
    def __init__(self, spark: SparkSession, table: Table):
        self.spark = spark
        self.table = table


    def read_data(self, interval: Tuple[datetime.datetime,datetime.datetime]) -> DataFrame:
        '''
        Read data from delta table using a start and end timestamp with the readChangeFeed option.
        
        Args:
            interval (Tuple[datetime.datetime,datetime.datetime]): Tuple of start and end datetime objects
        '''
        try:
            if self.table.schema is not None:
                return (
                    self.spark
                        .read
                        .format("delta") 
                        .option("readChangeFeed", "true") 
                        .option("startingTimestamp", interval[0].strftime('%Y-%m-%d %H:%M:%S'))
                        .option("endingTimestamp", interval[1].strftime('%Y-%m-%d %H:%M:%S'))
                        .schema(self.table.schema)
                        .load(self.table.path)
                )
            else:
                return (
                    self.spark
                        .read
                        .format("delta") 
                        .option("readChangeFeed", "true") 
                        .option("startingTimestamp", interval[0].strftime('%Y-%m-%d %H:%M:%S'))
                        .option("endingTimestamp", interval[1].strftime('%Y-%m-%d %H:%M:%S'))
                        .load(self.table.path)
                )
        except AnalysisException as e:
            if "Please use a timestamp before or at" in str(e):
                print("No new data")
                # Create empte dataframe
                return self.spark.createDataFrame([], schema = self.table.schema)
            else:
                raise e