from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def cost_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("COST_ID", LongType(), True),
            StructField("COST_EVENT_ID", LongType(), True),
            StructField("COST_DOMAIN_ID", StringType(), True),
            StructField("COST_TYPE_CONCEPT_ID", LongType(), True),
            StructField("CURRENCY_CONCEPT_ID", LongType(), True),
            StructField("TOTAL_CHARGE", DoubleType(), True),
            StructField("TOTAL_COST", DoubleType(), True),
            StructField("TOTAL_PAID", DoubleType(), True),
            StructField("PAID_BY_PAYER", DoubleType(), True),
            StructField("PAID_BY_PATIENT", DoubleType(), True),
            StructField("PAID_PATIENT_COPAY", DoubleType(), True),
            StructField("PAID_PATIENT_COINSURANCE", DoubleType(), True),
            StructField("PAID_PATIENT_DEDUCTIBLE", DoubleType(), True),
            StructField("PAID_BY_PRIMARY", DoubleType(), True),
            StructField("PAID_INGREDIENT_COST", DoubleType(), True),
            StructField("PAID_DISPENSING_FEE", DoubleType(), True),
            StructField("PAYER_PLAN_PERIOD_ID", LongType(), True),
            StructField("AMOUNT_ALLOWED", DoubleType(), True),
            StructField("REVENUE_CODE_CONCEPT_ID", LongType(), True),
            StructField("REVENUE_CODE_SOURCE_VALUE", StringType(), True),
            StructField("DRG_CONCEPT_ID", LongType(), True),
            StructField("DRG_SOURCE_VALUE", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
