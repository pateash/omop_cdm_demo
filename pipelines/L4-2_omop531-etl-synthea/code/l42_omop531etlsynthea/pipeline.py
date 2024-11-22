from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l42_omop531etlsynthea.config.ConfigStore import *
from l42_omop531etlsynthea.udfs.UDFs import *
from prophecy.utils import *
from l42_omop531etlsynthea.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ctePreDrugTarget = ctePreDrugTarget(spark, Config.ctePreDrugTarget)
    df_cteSubExposureEndDates = cteSubExposureEndDates(spark, Config.cteSubExposureEndDates, df_ctePreDrugTarget)
    df_cteDrugExposureEnds = cteDrugExposureEnds(
        spark, 
        Config.cteDrugExposureEnds, 
        df_ctePreDrugTarget, 
        df_cteSubExposureEndDates
    )
    df_cteSubExposures = cteSubExposures(spark, Config.cteSubExposures, df_cteDrugExposureEnds)
    df_cteFinalTarget = cteFinalTarget(spark, Config.cteFinalTarget, df_cteSubExposures)
    df_cteEndDates2 = cteEndDates2(spark, Config.cteEndDates2, df_cteFinalTarget)
    df_cteDrugEraEnds = cteDrugEraEnds(spark, Config.cteDrugEraEnds, df_cteFinalTarget, df_cteEndDates2)
    df_drug_era_summary = drug_era_summary(spark, df_cteDrugEraEnds)
    df_drug_era_summary_reformat = drug_era_summary_reformat(spark, df_drug_era_summary)
    drug_era(spark, df_drug_era_summary_reformat)
    df_cteConditionTarget = cteConditionTarget(spark, Config.cteConditionTarget)
    df_cteEndDates1 = cteEndDates1(spark, Config.cteEndDates1, df_cteConditionTarget)
    df_cteConditionEnds = cteConditionEnds(spark, Config.cteConditionEnds, df_cteConditionTarget, df_cteEndDates1)
    df_era_condition_occurrence_count = era_condition_occurrence_count(spark, df_cteConditionEnds)
    df_era_condition_occurrence_count_projection = era_condition_occurrence_count_projection(
        spark, 
        df_era_condition_occurrence_count
    )
    condition_era(spark, df_era_condition_occurrence_count_projection)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L4-2_omop531-etl-synthea")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L4-2_omop531-etl-synthea", config = Config)(pipeline)

if __name__ == "__main__":
    main()
