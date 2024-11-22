from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_exposure_summary(spark: SparkSession, cteDrugExposureEnds: DataFrame) -> DataFrame:
    df1 = cteDrugExposureEnds.groupBy(col("person_id"), col("drug_concept_id"), col("drug_sub_exposure_end_datetime"))

    return df1.agg(
        min(col("drug_exposure_start_datetime")).alias("drug_sub_exposure_start_datetime"), 
        count(lit(1)).alias("drug_exposure_count")
    )
