from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *

def era_condition_occurrence_count(spark: SparkSession, cteConditionEnds: DataFrame) -> DataFrame:
    df1 = cteConditionEnds.groupBy(col("person_id"), col("CONDITION_CONCEPT_ID"), col("era_end_datetime"))

    return df1.agg(
        min(col("CONDITION_START_DATETIME")).alias("condition_era_start_datetime"), 
        count(lit(1)).alias("condition_occurrence_count")
    )
