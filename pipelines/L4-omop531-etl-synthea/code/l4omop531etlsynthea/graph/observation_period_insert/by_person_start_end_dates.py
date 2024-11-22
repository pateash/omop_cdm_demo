from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def by_person_start_end_dates(spark: SparkSession, by_person_source_value: DataFrame) -> DataFrame:
    df1 = by_person_source_value.groupBy(col("person_id"))

    return df1.agg(min(col("START")).alias("start_date"), max(col("STOP")).alias("end_date"))
