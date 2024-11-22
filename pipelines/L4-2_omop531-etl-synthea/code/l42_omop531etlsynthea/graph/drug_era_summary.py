from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *

def drug_era_summary(spark: SparkSession, cteDrugEraEnds: DataFrame) -> DataFrame:
    df1 = cteDrugEraEnds.groupBy(col("person_id"), col("drug_concept_id"), col("era_end_datetime"))

    return df1.agg(
        min(col("drug_sub_exposure_start_datetime")).alias("drug_era_start_datetime"), 
        sum(col("drug_exposure_count")).alias("drug_exposure_count"), 
        datediff(
            date_add(
              col("era_end_datetime"), 
              - (
                datediff(col("era_end_datetime"), min(col("drug_sub_exposure_start_datetime")))
                - sum(col("days_exposed"))
              )\
                .cast(
                IntegerType()
              )
            ), 
            lit("1970-01-01")
          )\
          .alias("gap_days")
    )
