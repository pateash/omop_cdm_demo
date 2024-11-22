from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *

def drug_count_by_concept_and_year(spark: SparkSession, filter_by_recent_years: DataFrame) -> DataFrame:
    df1 = filter_by_recent_years.groupBy(col("DRUG_CONCEPT_NAME"), col("year_of_era"), col("DRUG_CONCEPT_ID"))

    return df1.agg(count(lit(1)).alias("drug_count"))
