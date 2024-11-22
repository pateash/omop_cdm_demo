from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def encounters_and_visits_join(spark: SparkSession, E: DataFrame, V: DataFrame, ) -> DataFrame:
    return E\
        .alias("E")\
        .join(
          V.alias("V"),
          (
            ((col("E.patient") == col("V.PATIENT")) & (col("E.encounterclass") == col("V.ENCOUNTERCLASS")))
            & (col("E.END_DATE") > col("V.START"))
          ),
          "inner"
        )\
        .select(col("E.END_DATE").alias("END_DATE"), col("V.Id").alias("Id"), col("V.START").alias("START"), col("V.STOP").alias("STOP"), col("V.PATIENT").alias("PATIENT"), col("V.PROVIDER").alias("PROVIDER"), col("V.ENCOUNTERCLASS").alias("ENCOUNTERCLASS"), col("V.CODE").alias("CODE"), col("V.DESCRIPTION").alias("DESCRIPTION"), col("V.COST").alias("COST"), col("V.REASONCODE").alias("REASONCODE"), col("V.REASONDESCRIPTION").alias("REASONDESCRIPTION"))
