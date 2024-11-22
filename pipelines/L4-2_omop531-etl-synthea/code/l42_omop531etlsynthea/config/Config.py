from l42_omop531etlsynthea.graph.ctePreDrugTarget.config.Config import SubgraphConfig as ctePreDrugTarget_Config
from l42_omop531etlsynthea.graph.cteSubExposureEndDates.config.Config import (
    SubgraphConfig as cteSubExposureEndDates_Config
)
from l42_omop531etlsynthea.graph.cteDrugExposureEnds.config.Config import SubgraphConfig as cteDrugExposureEnds_Config
from l42_omop531etlsynthea.graph.cteSubExposures.config.Config import SubgraphConfig as cteSubExposures_Config
from l42_omop531etlsynthea.graph.cteFinalTarget.config.Config import SubgraphConfig as cteFinalTarget_Config
from l42_omop531etlsynthea.graph.cteEndDates2.config.Config import SubgraphConfig as cteEndDates2_Config
from l42_omop531etlsynthea.graph.cteDrugEraEnds.config.Config import SubgraphConfig as cteDrugEraEnds_Config
from l42_omop531etlsynthea.graph.cteConditionTarget.config.Config import SubgraphConfig as cteConditionTarget_Config
from l42_omop531etlsynthea.graph.cteEndDates1.config.Config import SubgraphConfig as cteEndDates1_Config
from l42_omop531etlsynthea.graph.cteConditionEnds.config.Config import SubgraphConfig as cteConditionEnds_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            catalog_name: str=None,
            database_name: str=None,
            cteConditionTarget: dict=None,
            cteConditionEnds: dict=None,
            cteEndDates1: dict=None,
            ctePreDrugTarget: dict=None,
            cteEndDates2: dict=None,
            cteSubExposureEndDates: dict=None,
            cteDrugExposureEnds: dict=None,
            cteSubExposures: dict=None,
            cteFinalTarget: dict=None,
            cteDrugEraEnds: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            catalog_name, 
            database_name, 
            cteConditionTarget, 
            cteConditionEnds, 
            cteEndDates1, 
            ctePreDrugTarget, 
            cteEndDates2, 
            cteSubExposureEndDates, 
            cteDrugExposureEnds, 
            cteSubExposures, 
            cteFinalTarget, 
            cteDrugEraEnds
        )

    def update(
            self,
            catalog_name: str="omop",
            database_name: str="l1_silver_omop531",
            cteConditionTarget: dict={},
            cteConditionEnds: dict={},
            cteEndDates1: dict={},
            ctePreDrugTarget: dict={},
            cteEndDates2: dict={},
            cteSubExposureEndDates: dict={},
            cteDrugExposureEnds: dict={},
            cteSubExposures: dict={},
            cteFinalTarget: dict={},
            cteDrugEraEnds: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.cteConditionTarget = self.get_config_object(
            prophecy_spark, 
            cteConditionTarget_Config(prophecy_spark = prophecy_spark), 
            cteConditionTarget, 
            cteConditionTarget_Config
        )
        self.cteConditionEnds = self.get_config_object(
            prophecy_spark, 
            cteConditionEnds_Config(prophecy_spark = prophecy_spark), 
            cteConditionEnds, 
            cteConditionEnds_Config
        )
        self.cteEndDates1 = self.get_config_object(
            prophecy_spark, 
            cteEndDates1_Config(prophecy_spark = prophecy_spark), 
            cteEndDates1, 
            cteEndDates1_Config
        )
        self.ctePreDrugTarget = self.get_config_object(
            prophecy_spark, 
            ctePreDrugTarget_Config(prophecy_spark = prophecy_spark), 
            ctePreDrugTarget, 
            ctePreDrugTarget_Config
        )
        self.cteEndDates2 = self.get_config_object(
            prophecy_spark, 
            cteEndDates2_Config(prophecy_spark = prophecy_spark), 
            cteEndDates2, 
            cteEndDates2_Config
        )
        self.cteSubExposureEndDates = self.get_config_object(
            prophecy_spark, 
            cteSubExposureEndDates_Config(prophecy_spark = prophecy_spark), 
            cteSubExposureEndDates, 
            cteSubExposureEndDates_Config
        )
        self.cteDrugExposureEnds = self.get_config_object(
            prophecy_spark, 
            cteDrugExposureEnds_Config(prophecy_spark = prophecy_spark), 
            cteDrugExposureEnds, 
            cteDrugExposureEnds_Config
        )
        self.cteSubExposures = self.get_config_object(
            prophecy_spark, 
            cteSubExposures_Config(prophecy_spark = prophecy_spark), 
            cteSubExposures, 
            cteSubExposures_Config
        )
        self.cteFinalTarget = self.get_config_object(
            prophecy_spark, 
            cteFinalTarget_Config(prophecy_spark = prophecy_spark), 
            cteFinalTarget, 
            cteFinalTarget_Config
        )
        self.cteDrugEraEnds = self.get_config_object(
            prophecy_spark, 
            cteDrugEraEnds_Config(prophecy_spark = prophecy_spark), 
            cteDrugEraEnds, 
            cteDrugEraEnds_Config
        )
        pass
