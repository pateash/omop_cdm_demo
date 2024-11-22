from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def by_visit_occurrence_id_new(spark: SparkSession, final_visit_ids_1: DataFrame) -> DataFrame:
    return final_visit_ids_1.dropDuplicates(["VISIT_OCCURRENCE_ID_NEW"])
