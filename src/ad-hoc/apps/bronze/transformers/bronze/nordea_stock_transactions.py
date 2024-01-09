from h11 import Data
from pyspark.sql import DataFrame
from cdk.common_modules.utility.dwh_columns import DwhColumns
from cdk.common_modules.utility.meta_data import PysparkMetaData

class Transformer:
    def transform_data(self, df: DataFrame) -> DataFrame:
        df = DwhColumns().add_filename(df)
        df =  (
            df
                .withColumnRenamed("Opdateret den", "Opdateret_den")
                .withColumnRenamed("Instrument navn", "Instrument_navn")
                .drop("year","month","day")
        )
        df = DwhColumns().add_updated_timestamp(df)
        return df