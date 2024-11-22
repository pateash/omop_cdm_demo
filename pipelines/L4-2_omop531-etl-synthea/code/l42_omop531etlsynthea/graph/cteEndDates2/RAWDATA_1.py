from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l42_omop531etlsynthea.udfs.UDFs import *

def RAWDATA_1(
        spark: SparkSession,
        condition_start_ordinal: DataFrame,
        condition_events_projection: DataFrame,
) -> DataFrame:
    nonEmptyDf = [x for x in [condition_start_ordinal, condition_events_projection] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionAll(inDF)

    return res
