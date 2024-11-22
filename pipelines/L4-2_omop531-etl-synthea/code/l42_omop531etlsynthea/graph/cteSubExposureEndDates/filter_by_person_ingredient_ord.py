from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def filter_by_person_ingredient_ord(spark: SparkSession, by_person_and_ingredient: DataFrame) -> DataFrame:
    return by_person_and_ingredient.filter((((lit(2) * col("start_ordinal")) - col("overall_ord")) == lit(0)))
