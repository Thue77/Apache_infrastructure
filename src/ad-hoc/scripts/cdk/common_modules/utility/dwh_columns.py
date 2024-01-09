from pyspark.sql import DataFrame
import pyspark.sql.functions as F

import cdk.common_modules.utility.logging as my_logging

class DwhColumns:
    '''
    Class to add DWH columns to dataframes.
    '''

    @my_logging.module_logger
    def add_filename(self, df: DataFrame) -> DataFrame:
        '''
        Add the filename to the dataframe.
        The filename is adapted to be relative to the different zones, "landing", "bronze", "silver" and "gold".

        Args:
            df (DataFrame): The dataframe to add the filename to
            zone (str): The zone the dataframe is in
        '''

        df = self._add_filename_azure_datalake(df)

        return df

    def _add_filename_azure_datalake(self, df: DataFrame) -> DataFrame:
        '''
        Add the filename to the dataframe.
        The filename is adapted to be relative to the different zones, "landing", "bronze", "silver" and "gold".

        Args:
            df (DataFrame): The dataframe to add the filename to
            zone (str): The zone the dataframe is in
        '''

        df = df.withColumn('dwh_filename', F.input_file_name())

        # Adapt filename to be relative to the different zones, "landing", "bronze", "silver" and "gold"
        df = df.withColumn('dwh_filename', F.regexp_replace('dwh_filename', '.*dfs.core.windows.net/', ''))

        return df

    @my_logging.module_logger
    def add_updated_timestamp(self, df: DataFrame) -> DataFrame:
        '''
        Add the updated utc timestamp to the dataframe.
        '''
        
        df = df.withColumn('dwh_updated_timestamp', F.current_timestamp())

        return df




    