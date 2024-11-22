from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from l1_cdm_setup.udfs.UDFs import *
from . import *
from .config import *


class TableIterator_1_1(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_source_path_1 = source_path_1(spark)
        target_table_1(spark, df_source_path_1)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("source_path", "source_path"), ("target_table", "target_table"), ("timestamp", "timestamp")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
