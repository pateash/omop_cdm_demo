from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l64_pairwiseassociation.config.ConfigStore import *
from l64_pairwiseassociation.udfs.UDFs import *

def drugs(spark: SparkSession, d1: DataFrame, d2: DataFrame, ) -> DataFrame:
    return d1\
        .alias("d1")\
        .join(
          d2.alias("d2"),
          (
            ((col("d1.PERSON_ID") == col("d2.PERSON_ID")) & (abs(datediff(col("d1.DRUG_ERA_START_DATE"), col("d2.DRUG_ERA_START_DATE"))) <= lit(5)))
            & (abs(datediff(col("d1.DRUG_ERA_END_DATE"), col("d2.DRUG_ERA_END_DATE"))) <= lit(5))
          ),
          "inner"
        )\
        .select(col("d1.DRUG_CONCEPT_ID").alias("drug_id1"), col("d2.DRUG_CONCEPT_ID").alias("drug_id2"), col("d1.DRUG_CONCEPT_NAME").alias("drug1"), col("d2.DRUG_CONCEPT_NAME").alias("drug2"), col("d1.DRUG_ERA_START_DATE").alias("d1_start"), col("d1.DRUG_ERA_END_DATE").alias("d1_end"), col("d2.DRUG_ERA_START_DATE").alias("d2_start"), col("d2.DRUG_ERA_END_DATE").alias("d2_end"))
