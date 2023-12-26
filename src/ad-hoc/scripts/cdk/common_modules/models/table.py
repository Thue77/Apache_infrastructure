import dataclasses
from typing import List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

@dataclasses.dataclass
class Table:
    df: DataFrame
    name: str
    path: str
    schema: StructType = None