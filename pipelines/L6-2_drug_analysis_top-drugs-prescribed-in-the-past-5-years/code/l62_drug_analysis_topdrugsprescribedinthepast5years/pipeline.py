from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.config.ConfigStore import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.udfs.UDFs import *
from prophecy.utils import *
from l62_drug_analysis_topdrugsprescribedinthepast5years.graph import *

def pipeline(spark: SparkSession) -> None:
    df_concept = concept(spark)
    df_drug_era = drug_era(spark)
    df_by_drug_concept_id = by_drug_concept_id(spark, df_drug_era, df_concept)
    df_filter_by_recent_years = filter_by_recent_years(spark, df_by_drug_concept_id)
    df_drug_count_by_concept_and_year = drug_count_by_concept_and_year(spark, df_filter_by_recent_years)
    df_drug_count_by_concept_and_year_projection = drug_count_by_concept_and_year_projection(
        spark, 
        df_drug_count_by_concept_and_year
    )
    df_by_year_and_drug_count_desc_nulls_first = by_year_and_drug_count_desc_nulls_first(
        spark, 
        df_drug_count_by_concept_and_year_projection
    )
    top_drugs_last_5_years(spark, df_by_year_and_drug_count_desc_nulls_first)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L6-2_drug_analysis_top-drugs-prescribed-in-the-past-5-years")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/L6-2_drug_analysis_top-drugs-prescribed-in-the-past-5-years"
    )
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/L6-2_drug_analysis_top-drugs-prescribed-in-the-past-5-years",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
