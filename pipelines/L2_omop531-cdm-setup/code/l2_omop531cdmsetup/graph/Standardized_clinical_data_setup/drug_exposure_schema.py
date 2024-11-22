from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def drug_exposure_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DRUG_EXPOSURE_ID", LongType(), True),
            StructField("PERSON_ID", LongType(), True),
            StructField("DRUG_CONCEPT_ID", LongType(), True),
            StructField("DRUG_EXPOSURE_START_DATE", DateType(), True),
            StructField("DRUG_EXPOSURE_START_DATETIME", TimestampType(), True),
            StructField("DRUG_EXPOSURE_END_DATE", DateType(), True),
            StructField("DRUG_EXPOSURE_END_DATETIME", TimestampType(), True),
            StructField("VERBATIM_END_DATE", DateType(), True),
            StructField("DRUG_TYPE_CONCEPT_ID", LongType(), True),
            StructField("STOP_REASON", StringType(), True),
            StructField("REFILLS", LongType(), True),
            StructField("QUANTITY", DoubleType(), True),
            StructField("DAYS_SUPPLY", LongType(), True),
            StructField("SIG", StringType(), True),
            StructField("ROUTE_CONCEPT_ID", LongType(), True),
            StructField("LOT_NUMBER", StringType(), True),
            StructField("PROVIDER_ID", LongType(), True),
            StructField("VISIT_OCCURRENCE_ID", LongType(), True),
            StructField("VISIT_DETAIL_ID", LongType(), True),
            StructField("DRUG_SOURCE_VALUE", StringType(), True),
            StructField("DRUG_SOURCE_CONCEPT_ID", LongType(), True),
            StructField("ROUTE_SOURCE_VALUE", StringType(), True),
            StructField("DOSE_UNIT_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
