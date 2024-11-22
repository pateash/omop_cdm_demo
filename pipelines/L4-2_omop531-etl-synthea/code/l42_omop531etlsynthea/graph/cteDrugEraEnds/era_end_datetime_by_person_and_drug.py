from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def era_end_datetime_by_person_and_drug(spark: SparkSession, by_person_id_and_drug_concept_id: DataFrame) -> DataFrame:
    df1 = by_person_id_and_drug_concept_id.groupBy(
        col("person_id"), 
        col("drug_concept_id"), 
        col("drug_sub_exposure_start_datetime"), 
        col("drug_exposure_count"), 
        col("days_exposed")
    )

    return df1.agg(min(col("end_datetime")).alias("era_end_datetime"))
