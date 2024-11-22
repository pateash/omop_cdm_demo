from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l2_omop531cdmsetup.udfs.UDFs import *
from . import *
from .config import *

def Standardized_Vocabulary_setup(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_attribute_definition_schema = attribute_definition_schema(spark)
    attribute_definition(spark, df_attribute_definition_schema)
    df_concept_relationship_schema = concept_relationship_schema(spark)
    df_concept_synonym_schema = concept_synonym_schema(spark)
    concept_synonym(spark, df_concept_synonym_schema)
    df_concept_ancestor_schema = concept_ancestor_schema(spark)
    df_drug_strength_schema = drug_strength_schema(spark)
    drug_strength(spark, df_drug_strength_schema)
    df_source_to_concept_schema = source_to_concept_schema(spark)
    source_to_concept_map(spark, df_source_to_concept_schema)
    df_vocabulary_schema = vocabulary_schema(spark)
    vocabulary(spark, df_vocabulary_schema)
    df_relationship_schema = relationship_schema(spark)
    relationship(spark, df_relationship_schema)
    df_concept_class_schema = concept_class_schema(spark)
    df_domain_schema = domain_schema(spark)
    domain(spark, df_domain_schema)
    concept_relationship(spark, df_concept_relationship_schema)
    df_concept_schema = concept_schema(spark)
    concept_ancestor(spark, df_concept_ancestor_schema)
    concept(spark, df_concept_schema)
    concept_class(spark, df_concept_class_schema)
    subgraph_config.update(Config)
