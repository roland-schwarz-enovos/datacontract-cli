# FIXME: some more documents are needed in this topic.
# sample of how the quality entry for a custom test type may look.
"""
        quality:
        - type: custom
          description: Test that the difference between columnA(row=1) and columnB(row=1) is in a specific time range.
          dimension: conformity
          severity: error
          businessImpact: operational
          engine: custom_python_engine
          #               package.function ?
          implementation: |
            column: values
            plugin: OnJsonTestTimeDelta
"""
from datacontract.data_contract import DataContract

class DataQualityPluginRegistry(type):
    plugins:list    = []
    PluginName:str  = "DataQualityPluginRegistry"

    def __init__(cls, name, bases, attrs):
        if name != "DataQualityPluginRegistry":
            DataQualityPluginRegistry.plugins.append(cls)


class DataQualityAbstractBasePlugin(object, metaclass=DataQualityPluginRegistry):
    PluginName = "DataQualityAbstractBasePlugin"
    def __init__(self, _duckdbconnection=None, _run=None, _ctl:list|None=None, _datacontract:DataContract|None=None):
        """Initialize the plugin. Deliver duckdb, we'll work out what else is needed.
        probably tables and columns to mangle.
        """
        self.duckdbconnection = _duckdbconnection
        self.run = _run
        self.customtestlist = _ctl
        self.datacontract = _datacontract
        self.modelname = ""
        ## fixme: this is a POC-dirt galore.
        for model_name, model in self.datacontract.models.items():
            self.modelname = model_name
