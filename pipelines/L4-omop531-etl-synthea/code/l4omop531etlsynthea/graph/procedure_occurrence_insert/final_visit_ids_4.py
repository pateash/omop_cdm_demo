from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def final_visit_ids_4(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.catalog_name}`.`{Config.database_name}`.`final_visit_ids`")
