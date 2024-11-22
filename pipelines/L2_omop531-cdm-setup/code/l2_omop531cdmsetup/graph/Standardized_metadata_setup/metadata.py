from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def metadata(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("append").saveAsTable(f"`{Config.catalog_name}`.`{Config.database_name}`.`metadata`")
