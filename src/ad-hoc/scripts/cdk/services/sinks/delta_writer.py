from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from typing import Protocol,List, Tuple
from cdk.common_modules.models.file import File
import os
from delta.tables import DeltaTable

class DeltaWriter:
    '''
    Class to write data to a delta table.

    Args:
        spark (SparkSession): SparkSession object
        partition_columns (List[str], optional): List of partition columns. Defaults to None.
        mode (str, optional): Mode to write data. Defaults to 'overwrite'.
        add_columns (List[Tuple[str,str,str]], optional): List of columns to add to the delta table. Defaults to None. The first value in the tuple is the column name, the second is the data type and the third is the SQL statement,
        which must be valid according to https://docs.delta.io/latest/delta-batch.html#-partition-data&language-python.
    '''
    def __init__(self, spark: SparkSession, partition_columns: List[str] = None, mode: str = 'overwrite', add_columns: List[Tuple[str,str,str]] = None):
        self.spark = spark
        self.partition_columns = partition_columns
        self.mode = mode
        self.add_columns = add_columns

    def write_data(self, df: DataFrame, file: File) -> None:
        delta_table = (
            DeltaTable
                .createIfNotExists(self.spark)
                .addColumns(df.schema)
                .location(file.path)
                .property("delta.enableChangeDataFeed", "true")
                
        )
        if self.add_columns is not None:
            for column in self.add_columns:
                delta_table = delta_table.addColumn(column[0],column[1],generatedAlwaysAs=column[2])
        print(delta_table)
        if self.partition_columns is not None:
            delta_table = delta_table.partitionedBy(*self.partition_columns)
        delta_table.execute()
        # if self.partition_columns is None:
        (
            df
                .write
                .format(file.type)
                .mode(self.mode)
                .save(os.path.join(file.path))
            )
        # else:
        #     (
        #         df
        #             .write
        #             .format(file.type)
        #             .mode(self.mode)
        #             .partitionBy(*self.partition_columns) # See https://docs.delta.io/latest/delta-batch.html#partition-data&language-python
        #             .save(os.path.join(file.path))
        #         )