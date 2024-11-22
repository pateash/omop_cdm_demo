from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def filter_by_recent_years(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((year(current_date()) - col("year_of_era")) <= lit(5)))
