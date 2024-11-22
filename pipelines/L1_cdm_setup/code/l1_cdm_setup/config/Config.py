from l1_cdm_setup.graph.TableIterator_1_1.config.Config import SubgraphConfig as TableIterator_1_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, database_name: str=None, TableIterator_1_1: dict=None, **kwargs):
        self.spark = None
        self.update(database_name, TableIterator_1_1)

    def update(self, database_name: str="L1_silver_layer", TableIterator_1_1: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.database_name = database_name
        self.TableIterator_1_1 = self.get_config_object(
            prophecy_spark, 
            TableIterator_1_1_Config(prophecy_spark = prophecy_spark), 
            TableIterator_1_1, 
            TableIterator_1_1_Config
        )
        pass
