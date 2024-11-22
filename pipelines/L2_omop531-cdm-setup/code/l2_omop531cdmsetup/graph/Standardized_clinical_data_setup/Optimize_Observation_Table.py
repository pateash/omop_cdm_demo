from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l2_omop531cdmsetup.udfs.UDFs import *

def Optimize_Observation_Table(spark: SparkSession):
    if not ("SColumnExpression" in locals()):
        from delta.tables import DeltaTable
        spark.sql(
            (
              f"OPTIMIZE "
              + f"`{Config.catalog_name}`.`{Config.database_name}`.`observation`"
              + "ZORDER BY {}".format(",".join(["person_id"]))
            )
        )
