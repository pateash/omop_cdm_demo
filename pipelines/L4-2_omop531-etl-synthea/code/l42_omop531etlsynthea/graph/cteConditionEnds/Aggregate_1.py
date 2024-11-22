from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, by_person_id_and_condition_concept_id: DataFrame) -> DataFrame:
    df1 = by_person_id_and_condition_concept_id.groupBy(
        col("CONDITION_OCCURRENCE_ID"), 
        col("person_id"), 
        col("CONDITION_CONCEPT_ID"), 
        col("CONDITION_START_DATETIME")
    )

    return df1.agg(min(col("end_date")).alias("era_end_datetime"))
