from l4omop531etlsynthea.graph.all_visits_table.IP_VISITS.CTE_END_DATES.config.Config import (
    SubgraphConfig as CTE_END_DATES_Config
)
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, CTE_END_DATES: dict={}, **kwargs):
        self.CTE_END_DATES = self.get_config_object(
            prophecy_spark, 
            CTE_END_DATES_Config(prophecy_spark = prophecy_spark), 
            CTE_END_DATES, 
            CTE_END_DATES_Config
        )
        pass

    def update(self, updated_config):
        self.CTE_END_DATES = updated_config.CTE_END_DATES
        pass

Config = SubgraphConfig()
