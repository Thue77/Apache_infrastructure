from pyspark.sql import DataFrame

class PysparkMetaData:
    '''
    Class for adding meta data to a pyspark dataframe.
    The class handles both the addition of meta data to a dataframe and the reading of meta data from a dataframe.
    
    Args:
        df (DataFrame): The dataframe to add meta data to
    '''

    def __init__(self, df: DataFrame) -> None:
        self.df = df

    def get_column_meta_data(self, column_name: str) -> dict:
        '''
        Get meta data from a column in the dataframe.
        
        Args:
            column_name (str): The name of the column to get meta data from
        
        Returns:
            dict: The meta data of the column
        '''

        return self.df.schema[column_name].metadata

    def add_column_meta_data(self, column_name: str, meta_data: dict) -> None:
        '''
        Add meta data to a column in the dataframe.
        
        Args:
            column_name (str): The name of the column to add meta data to
            meta_data (dict): The meta data to add to the column
        '''

        # Add existing meta data to the new meta data, but placed under the key 'source_meta_data'
        meta_data['source_meta_data'] = self.get_column_meta_data(column_name)

        self.df = self.df.withMetadata(columnName=column_name, metadata=meta_data)

    def add_columns_meta_data(self, columns_meta_data: dict) -> None:
        '''
        Add meta data to multiple columns in the dataframe.
        
        Args:
            columns_meta_data (dict): The meta data to add to the columns
        '''

        for column_name, meta_data in columns_meta_data.items():
            self.add_column_meta_data(column_name, meta_data)

    
    def get_table_meta_data(self, format='json') -> dict:
        '''
        Get meta data from the table in the dataframe.
        
        Args:
            format (str): The format of the meta data
        
        Returns:
            dict: The meta data of the table
        '''

        if format == 'json':
            return self.df.schema.json()
        else:
            raise ValueError(f'Format {format} is not supported')