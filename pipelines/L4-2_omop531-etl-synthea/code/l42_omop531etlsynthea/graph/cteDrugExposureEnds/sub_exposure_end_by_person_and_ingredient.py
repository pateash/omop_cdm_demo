from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def sub_exposure_end_by_person_and_ingredient(
        spark: SparkSession,
        by_person_and_ingredient_concept_id: DataFrame
) -> DataFrame:
    df1 = by_person_and_ingredient_concept_id.groupBy(
        col("drug_exposure_id"), 
        col("person_id"), 
        col("ingredient_concept_id"), 
        col("drug_exposure_start_datetime")
    )

    return df1.agg(min(col("end_datetime")).alias("drug_sub_exposure_end_datetime"))
