from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l6_druganalysis.config.ConfigStore import *
from l6_druganalysis.udfs.UDFs import *
from prophecy.utils import *
from l6_druganalysis.graph import *

def pipeline(spark: SparkSession) -> None:
    df_person = person(spark)
    df_drug_era = drug_era(spark)
    df_concept = concept(spark)
    df_join_multiple_dataframes = join_multiple_dataframes(spark, df_drug_era, df_person, df_concept, df_concept)
    df_after_2000 = after_2000(spark, df_join_multiple_dataframes)
    df_drug_count_by_demographics = drug_count_by_demographics(spark, df_after_2000)
    df_drug_count_demographics_projection = drug_count_demographics_projection(spark, df_drug_count_by_demographics)
    df_by_age_year_drug = by_age_year_drug(spark, df_drug_count_demographics_projection)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L6_drug-analysis")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L6_drug-analysis")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L6_drug-analysis", config = Config)(pipeline)

if __name__ == "__main__":
    main()
