from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l4omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def CTE_END_DATES(spark: SparkSession, subgraph_config: SubgraphConfig) -> DataFrame:
    Config.update(subgraph_config)
    df_encounters_1 = encounters_1(spark)
    df_inpatient_encounters_1 = inpatient_encounters_1(spark, df_encounters_1)
    df_reformatted_inpatient_encounters = reformatted_inpatient_encounters(spark, df_inpatient_encounters_1)
    df_encounters = encounters(spark)
    df_inpatient_encounters = inpatient_encounters(spark, df_encounters)
    df_encounters_by_patient = encounters_by_patient(spark, df_inpatient_encounters)
    df_RAWDATA = RAWDATA(spark, df_encounters_by_patient, df_reformatted_inpatient_encounters)
    df_E = E(spark, df_RAWDATA)
    df_filter_by_start_and_overall_ord = filter_by_start_and_overall_ord(spark, df_E)
    df_end_dates_by_event_date = end_dates_by_event_date(spark, df_filter_by_start_and_overall_ord)
    subgraph_config.update(Config)

    return df_end_dates_by_event_date
