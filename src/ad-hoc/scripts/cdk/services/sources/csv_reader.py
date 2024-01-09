from pyspark.sql import SparkSession, DataFrame
from typing import Protocol,List, Tuple, Any
from cdk.common_modules.models.file import File
import os

class CsvReader:
    def __init__(self, spark: SparkSession, file: File, schema: DataFrame.schema = None, header: bool = True, sep: str = ','):
        self.spark = spark
        self.file = file
        self.schema = schema
        self.header = header
        self.sep = sep

    def read_data(self, partition_columns: List[Tuple[str,Any]]) -> DataFrame:
        hive_partitions = [f'{p}={v}' for p,v in partition_columns]
        if self.schema is not None:
            return (
            self.spark
                .read
                .option('basePath', self.file.path)
                .csv(os.path.join(self.file.path, *hive_partitions), header=self.header, sep=self.sep, schema=self.schema)
            )
        else:
            return (
                self.spark
                    .read
                    .option('basePath', self.file.path)
                    .csv(os.path.join(self.file.path, *hive_partitions), header=self.header, sep=self.sep)
                )