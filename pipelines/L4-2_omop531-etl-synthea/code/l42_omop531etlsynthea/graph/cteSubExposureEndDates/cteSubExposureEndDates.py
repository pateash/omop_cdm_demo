from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l42_omop531etlsynthea.udfs.UDFs import *
from . import *
from .config import *

def cteSubExposureEndDates(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_by_person_and_ingredient_start_date = by_person_and_ingredient_start_date(spark, in0)
    df_format_drug_exposure_data = format_drug_exposure_data(spark, in0)
    df_SetOperation_4 = SetOperation_4(spark, df_by_person_and_ingredient_start_date, df_format_drug_exposure_data)
    df_latest_ingredient_events = latest_ingredient_events(spark, df_SetOperation_4)
    df_filter_by_person_ingredient_ord = filter_by_person_ingredient_ord(spark, df_latest_ingredient_events)
    df_by_person_and_ingredient = by_person_and_ingredient(spark, df_filter_by_person_ingredient_ord)
    subgraph_config.update(Config)

    return df_by_person_and_ingredient
