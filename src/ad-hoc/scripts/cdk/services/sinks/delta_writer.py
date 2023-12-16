from pyspark.sql import SparkSession, DataFrame
from typing import Protocol,List
from cdk.common_modules.models.file import File
import os

class DeltaWriter:
    def __init__(self, partition_columns: List[str] = None, mode: str = 'overwrite'):
        self.partition_columns = partition_columns
        self.mode = mode

    def write_data(self, df: DataFrame, file: File) -> None:
        if self.partition_columns is None:
            (
                df
                    .write
                    .format(file.type)
                    .mode(self.mode)
                    .save(os.path.join(file.path, file.name))
                )
        else:
            (
                df
                    .write
                    .format(file.type)
                    .mode(self.mode)
                    .partitionBy(*self.partition_columns) # See https://docs.delta.io/latest/delta-batch.html#partition-data&language-python
                    .save(os.path.join(file.path, file.name))
                )