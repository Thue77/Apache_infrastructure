from typing import Protocol,List
from pyspark.sql import SparkSession

class SparkConfig(Protocol):
    '''Interface for spark config
    '''
    configurations: dict

class SparkSessionBuilder:
    '''Class to build SparkSession
    '''

    def __init__(self, app_name: str, spark_config: SparkConfig):
        self.app_name = app_name
        self.master = "spark://spark-master:7077"
        self.spark_config = spark_config
    
    def build(self):
        spark_session = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master)
            .config("spark.executor.memory", "512m")
        )
        for key, value in self.spark_config.configurations.items():
            spark_session = spark_session.config(key, value)
        return spark_session.getOrCreate()