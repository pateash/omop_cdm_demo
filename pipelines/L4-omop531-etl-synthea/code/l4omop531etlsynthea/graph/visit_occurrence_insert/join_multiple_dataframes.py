from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_dataframes(spark: SparkSession, av: DataFrame, fv: DataFrame, p: DataFrame) -> DataFrame:
    return av\
        .alias("av")\
        .join(fv.alias("fv"), (col("av.visit_occurrence_id") == col("fv.VISIT_OCCURRENCE_ID_NEW")), "inner")\
        .join(p.alias("p"), (col("p.PERSON_SOURCE_VALUE") == col("av.patient")), "inner")\
        .select(col("av.encounter_id").alias("encounter_id"), col("av.patient").alias("patient"), col("av.encounterclass").alias("encounterclass"), col("av.VISIT_START_DATE").alias("VISIT_START_DATE"), col("av.VISIT_END_DATE").alias("VISIT_END_DATE"), col("av.visit_occurrence_id").alias("visit_occurrence_id"), col("fv.VISIT_OCCURRENCE_ID_NEW").alias("VISIT_OCCURRENCE_ID_NEW"), col("p.person_id").alias("person_id"), col("p.gender_concept_id").alias("gender_concept_id"), col("p.YEAR_OF_BIRTH").alias("YEAR_OF_BIRTH"), col("p.MONTH_OF_BIRTH").alias("MONTH_OF_BIRTH"), col("p.DAY_OF_BIRTH").alias("DAY_OF_BIRTH"), col("p.BIRTH_DATETIME").alias("BIRTH_DATETIME"), col("p.RACE_CONCEPT_ID").alias("RACE_CONCEPT_ID"), col("p.ETHNICITY_CONCEPT_ID").alias("ETHNICITY_CONCEPT_ID"), col("p.LOCATION_ID").alias("LOCATION_ID"), col("p.PROVIDER_ID").alias("PROVIDER_ID"), col("p.CARE_SITE_ID").alias("CARE_SITE_ID"), col("p.PERSON_SOURCE_VALUE").alias("PERSON_SOURCE_VALUE"), col("p.GENDER_SOURCE_VALUE").alias("GENDER_SOURCE_VALUE"), col("p.GENDER_SOURCE_CONCEPT_ID").alias("GENDER_SOURCE_CONCEPT_ID"), col("p.RACE_SOURCE_VALUE").alias("RACE_SOURCE_VALUE"), col("p.RACE_SOURCE_CONCEPT_ID").alias("RACE_SOURCE_CONCEPT_ID"), col("p.ETHNICITY_SOURCE_VALUE").alias("ETHNICITY_SOURCE_VALUE"), col("p.ETHNICITY_SOURCE_CONCEPT_ID").alias("ETHNICITY_SOURCE_CONCEPT_ID"))
