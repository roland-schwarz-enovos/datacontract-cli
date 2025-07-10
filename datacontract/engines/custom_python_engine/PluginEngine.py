# Fixme: some docs.


class DataQualityPluginRegistry(type):
    plugins = []
    PluginName = "DataQualityPluginRegistry"

    def __init__(cls, name, bases, attrs):
        if name != "DataQualityPluginRegistry":
            DataQualityPluginRegistry.plugins.append(cls)


class DataQualityAbstractBasePlugin(object, metaclass=DataQualityPluginRegistry):
    def __init__(self, _duckdbconnection=None, _run=None, _ctl=None, _datacontract=None):
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
