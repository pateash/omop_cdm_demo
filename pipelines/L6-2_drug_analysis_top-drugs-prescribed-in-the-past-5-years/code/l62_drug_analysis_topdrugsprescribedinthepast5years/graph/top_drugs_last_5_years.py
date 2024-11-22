from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def top_drugs_last_5_years(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("error")\
        .saveAsTable(f"`{Config.catalog_name}`.`l1_silver_omop531results`.`top_drugs_last_5_years`")
