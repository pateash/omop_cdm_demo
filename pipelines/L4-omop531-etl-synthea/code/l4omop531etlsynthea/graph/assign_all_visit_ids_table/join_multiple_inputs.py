from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l4omop531etlsynthea.udfs.UDFs import *

def join_multiple_inputs(spark: SparkSession, E: DataFrame, AV: DataFrame, ) -> DataFrame:
    return E\
        .alias("E")\
        .join(
          AV.alias("AV"),
          (
            ((col("AV.patient") == col("E.PATIENT")) & (col("E.START") >= col("AV.VISIT_START_DATE")))
            & (col("E.START") <= col("AV.VISIT_END_DATE"))
          ),
          "inner"
        )\
        .select(col("E.Id").alias("encounter_id"), col("E.PATIENT").alias("person_source_value"), col("E.START").alias("date_service"), col("E.STOP").alias("date_service_end"), col("E.ENCOUNTERCLASS").alias("encounterclass"), col("AV.encounterclass").alias("VISIT_TYPE"), col("AV.VISIT_START_DATE").alias("VISIT_START_DATE"), col("AV.VISIT_END_DATE").alias("VISIT_END_DATE"), col("AV.visit_occurrence_id").alias("VISIT_OCCURRENCE_ID"), when(
          ((col("E.ENCOUNTERCLASS") == lit("inpatient")) & (col("AV.encounterclass") == lit("inpatient"))), 
          col("AV.visit_occurrence_id")
        )\
        .when(
          col("E.ENCOUNTERCLASS").isin(lit("emergency"), lit("urgent")), 
          when(
              ((col("AV.encounterclass") == lit("inpatient")) & (col("E.START") > col("AV.VISIT_START_DATE"))), 
              col("AV.visit_occurrence_id")
            )\
            .when(
              (
                col("AV.encounterclass").isin(lit("emergency"), lit("urgent"))
                & (col("E.START") == col("AV.VISIT_START_DATE"))
              ), 
              col("AV.visit_occurrence_id")
            )\
            .otherwise(lit(None))
        )\
        .when(
          col("E.ENCOUNTERCLASS").isin(lit("ambulatory"), lit("wellness"), lit("outpatient")), 
          when(
              ((col("AV.encounterclass") == lit("inpatient")) & (col("E.START") >= col("AV.VISIT_START_DATE"))), 
              col("AV.visit_occurrence_id")
            )\
            .when(
              col("AV.encounterclass").isin(lit("ambulatory"), lit("wellness"), lit("outpatient")), 
              col("AV.visit_occurrence_id")
            )\
            .otherwise(lit(None))
        )\
        .otherwise(lit(None))\
        .alias("VISIT_OCCURRENCE_ID_NEW"), lit(120).cast(DoubleType()).alias("VISIT_OCCURRENCE_COST"))
