from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def drug_strength_schema(spark: SparkSession) -> DataFrame:
    schema = StructType([
            StructField("DRUG_CONCEPT_ID", LongType(), True),
            StructField("INGREDIENT_CONCEPT_ID", LongType(), True),
            StructField("AMOUNT_VALUE", DoubleType(), True),
            StructField("AMOUNT_UNIT_CONCEPT_ID", LongType(), True),
            StructField("NUMERATOR_VALUE", DoubleType(), True),
            StructField("NUMERATOR_UNIT_CONCEPT_ID", LongType(), True),
            StructField("DENOMINATOR_VALUE", DoubleType(), True),
            StructField("DENOMINATOR_UNIT_CONCEPT_ID", LongType(), True),
            StructField("BOX_SIZE", LongType(), True),
            StructField("VALID_START_DATE", DateType(), True),
            StructField("VALID_END_DATE", DateType(), True),
            StructField("INVALID_REASON", StringType(), True)

    ])
    out0 = spark.createDataFrame([], schema)

    return out0
