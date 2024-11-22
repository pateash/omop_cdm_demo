from bronze_ingest.graph.TableIterator_1.config.Config import SubgraphConfig as TableIterator_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, TableIterator_1: dict=None, **kwargs):
        self.spark = None
        self.update(TableIterator_1)

    def update(self, TableIterator_1: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.TableIterator_1 = self.get_config_object(
            prophecy_spark, 
            TableIterator_1_Config(prophecy_spark = prophecy_spark), 
            TableIterator_1, 
            TableIterator_1_Config
        )
        pass
