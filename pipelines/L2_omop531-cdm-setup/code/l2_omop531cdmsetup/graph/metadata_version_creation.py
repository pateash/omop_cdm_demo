from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l2_omop531cdmsetup.config.ConfigStore import *
from l2_omop531cdmsetup.udfs.UDFs import *

def metadata_version_creation(spark: SparkSession) -> DataFrame:
    data = [(0, 0, 'OHDSI OMOP CDM Version', '5.3.1', 0, datetime.now().date(), datetime.now())]
    # Define schema for the DataFrame
    schema = ["METADATA_CONCEPT_ID", "METADATA_TYPE_CONCEPT_ID", "NAME", "VALUE_AS_STRING", "VALUE_AS_CONCEPT_ID",
              "METADATA_DATE", "METADATA_DATETIME"]
    # Create DataFrame
    out0 = spark.createDataFrame(data, schema)

    return out0
