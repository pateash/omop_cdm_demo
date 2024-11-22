from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def reformat_data(spark: SparkSession, filter_gender_not_null: DataFrame) -> DataFrame:
    return filter_gender_not_null.select(
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("Id").asc())).alias("person_id"), 
        when((upper(col("GENDER")) == lit("M")), lit(8507))\
          .when((upper(col("GENDER")) == lit("F")), lit(8532))\
          .alias("gender_concept_id"), 
        year(col("BIRTHDATE")).alias("YEAR_OF_BIRTH"), 
        month(col("BIRTHDATE")).alias("MONTH_OF_BIRTH"), 
        expr("day(BIRTHDATE)").alias("DAY_OF_BIRTH"), 
        col("BIRTHDATE").alias("BIRTH_DATETIME"), 
        when((upper(col("RACE")) == lit("WHITE")), lit(8527))\
          .when((upper(col("RACE")) == lit("BLACK")), lit(8516))\
          .when((upper(col("RACE")) == lit("ASIAN")), lit(8515))\
          .otherwise(lit(0))\
          .alias("RACE_CONCEPT_ID"), 
        when((upper(col("RACE")) == lit("HISPANIC")), lit(38003563)).otherwise(lit(0)).alias("ETHNICITY_CONCEPT_ID"), 
        lit(None).cast(IntegerType()).alias("LOCATION_ID"), 
        lit(0).alias("PROVIDER_ID"), 
        lit(None).cast(IntegerType()).alias("CARE_SITE_ID"), 
        col("Id").alias("PERSON_SOURCE_VALUE"), 
        col("GENDER").alias("GENDER_SOURCE_VALUE"), 
        lit(0).alias("GENDER_SOURCE_CONCEPT_ID"), 
        col("RACE").alias("RACE_SOURCE_VALUE"), 
        lit(0).alias("RACE_SOURCE_CONCEPT_ID"), 
        col("ETHNICITY").alias("ETHNICITY_SOURCE_VALUE"), 
        lit(0).alias("ETHNICITY_SOURCE_CONCEPT_ID")
    )
