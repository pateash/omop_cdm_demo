from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def first_encounter_of_visit_occurrence(
        spark: SparkSession,
        encounter_id_and_visit_occurrence_id_projection: DataFrame
) -> DataFrame:
    return encounter_id_and_visit_occurrence_id_projection.filter((col("RN") == lit("1")))
