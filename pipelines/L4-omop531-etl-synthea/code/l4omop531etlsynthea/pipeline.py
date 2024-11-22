from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l4omop531etlsynthea.config.ConfigStore import *
from l4omop531etlsynthea.udfs.UDFs import *
from prophecy.utils import *
from l4omop531etlsynthea.graph import *

def pipeline(spark: SparkSession) -> None:
    condition_occurrence_insert(spark, Config.condition_occurrence_insert)
    drug_exposure_insert(spark, Config.drug_exposure_insert)
    all_visits_table(spark, Config.all_visits_table)
    observation_period_insert(spark, Config.observation_period_insert)
    final_visit_ids_table(spark, Config.final_visit_ids_table)
    measurement_insert(spark, Config.measurement_insert)
    assign_all_visit_ids_table(spark, Config.assign_all_visit_ids_table)
    visit_occurrence_insert(spark, Config.visit_occurrence_insert)
    person_insert(spark, Config.person_insert)
    procedure_occurrence_insert(spark, Config.procedure_occurrence_insert)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L4-omop531-etl-synthea")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L4-omop531-etl-synthea", config = Config)(pipeline)

if __name__ == "__main__":
    main()
