from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def concept(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.catalog_name}`.`{Config.database_name}`.`concept`")
