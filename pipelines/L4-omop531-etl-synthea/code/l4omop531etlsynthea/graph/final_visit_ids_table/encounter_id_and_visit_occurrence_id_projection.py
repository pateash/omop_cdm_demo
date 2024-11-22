from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounter_id_and_visit_occurrence_id_projection(
        spark: SparkSession,
        first_encounter_of_visit_occurrence: DataFrame
) -> DataFrame:
    return first_encounter_of_visit_occurrence.select(col("encounter_id"), col("VISIT_OCCURRENCE_ID_NEW"))
