from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteEndDates2(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_condition_start_ordinal = condition_start_ordinal(spark, in0)
    df_condition_events_projection = condition_events_projection(spark, in0)
    df_RAWDATA_1 = RAWDATA_1(spark, df_condition_start_ordinal, df_condition_events_projection)
    df_e = e(spark, df_RAWDATA_1)
    df_filter_by_start_ordinal = filter_by_start_ordinal(spark, df_e)
    df_by_person_condition_date = by_person_condition_date(spark, df_filter_by_start_ordinal)
    subgraph_config.update(Config)

    return df_by_person_condition_date
