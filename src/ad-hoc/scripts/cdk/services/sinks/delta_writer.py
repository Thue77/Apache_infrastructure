from pyspark.sql import SparkSession, DataFrame
from typing import Protocol,List
from cdk.common_modules.models.file import File

class DeltaWriter:
    def __init__(self, spark: SparkSession, partition_columns: List[str], mode: str = 'overwrite'):
        self.spark = spark
        self.partition_columns = partition_columns
        self.mode = mode

    def write_data(self, df: DataFrame, file: File) -> None:
        (
            spark
                .write
                .format(File.type)
                .mode(self.mode)
                .partitionBy(self.partition_columns) # See https://docs.delta.io/latest/delta-batch.html#partition-data&language-python
                .save(os.path.join(file.path, file.name))
            )