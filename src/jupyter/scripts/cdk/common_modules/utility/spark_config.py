from typing import Protocol,List
from pyspark.sql import SparkSession

class Secret(Protocol):
    '''Interface for secrets
    '''
    def get_secret(self, secret_name: str) -> str:
        ...

class SparkConfig:
    '''Class to define spark configurations as dictionary for different standard scenarios'''
    def __init__(self, secrets: Secret) -> None:
        self.configurations = {}
        self.secrets = secrets

    def add_hudi_config(self):
        hudi_config = {
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            ,"spark.sql.catalog.spark_catalog":"org.apache.spark.sql.hudi.catalog.HoodieCatalog"
            ,"spark.sql.extensions":"org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
        }
        # Use this to add hudi config to spark config
        self.configurations.update(hudi_config)
    
    def add_jars_to_install(self, jars: List):
        '''method to add jars to install to spark config.
        Only jars defined in the jar dictionary are available for installation.

        Args:
            jars (List): List of jars to install. Must be defined in jar dictionary.
        '''
        jar = {
            'hudi': 'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1'
            ,'azure_storage': 'org.apache.hadoop:hadoop-azure:3.3.3'
        }
        try:
            jar_config = {
                "spark.jars.packages": ','.join([jar[j] for j in jars])
            }
        except KeyError as e:
            print(f"Jar from list 'jars' not found in jar dictionary")
            raise e
        
        if 'hudi' in jars:
            '''Add hudi config if hudi jar is installed'''
            self.add_hudi_config()

        # Use this to add jars to install to spark config
        self.configurations.update(jar_config)
    
    def add_storage_account_access(self, storage_account_name: str, method = 'access_key'):
        storage_account_access_config = {
            'access_key': self.__azure_access_key,
            'sas_token': self.__azure_sas_token
        }
        # Use this to add storage account access to spark config
        self.configurations.update(storage_account_access_config[method](storage_account_name))

    def __azure_access_key(self, storage_account_name: str) -> dict:
        return {
                f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": self.secrets.get_secret(f"ADLS_{storage_account_name}_access_key")
            }
    def __azure_sas_token(self, storage_account_name: str) -> dict:
        return {
                f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "SAS"
                ,f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
                ,f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net": self.secrets.get_secret(f"ADLS_{storage_account_name}_sas_token")
            }