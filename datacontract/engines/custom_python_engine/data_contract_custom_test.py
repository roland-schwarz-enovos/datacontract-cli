"""_summary_

Struct: Note: Test what happens with test that have a custom engine defined.
run.checks gets filtered for one type of engine.

check.implementation

"""

import imp
import os
import typing

import yaml

if typing.TYPE_CHECKING:
    from pyspark.sql import SparkSession

from typing import Final

from duckdb.duckdb import DuckDBPyConnection

from datacontract.engines.custom_python_engine.PluginEngine import (
    DataQualityAbstractBasePlugin,
    DataQualityPluginRegistry,
)
from datacontract.engines.soda.connections.duckdb_connection import get_duckdb_connection
from datacontract.model.data_contract_specification import DataContractSpecification, Server
from datacontract.model.run import Check, ResultEnum, Run

ENGINE_KEYWORD: Final[str] = "custom_python_engine"


def discover_plugins(dirs):
    """Discover the plugin classes contained in Python files, given a
    list of directory names to scan. Return a list of plugin classes.
    """
    ## Fixme: this shit does not recurse through folders.
    # python is f**in useless in the way it is used here. This "for x in filename-call" dismantles a string into all its elements, which fluffers up the basic idea - run through directories.
    for dir in dirs:
        for filename in os.listdir(dir):
            ## if is folder -> call self
            testpath = os.path.join(dir, filename)
            if os.path.isdir(testpath):
                discover_plugins([testpath])

            modname, ext = os.path.splitext(filename)
            if ext == ".py":
                ## old variant, imp doesn't exist any loinger
                file, path, descr = imp.find_module(modname, [dir])
                # modspec =  importlib.util.find_spec(modname, [dir])
                ##file, path, descr =
                if file:
                    # Loading the module registers the plugin in
                    # "PluginRegistry"
                    imp.load_module(modname, file, path, descr)
    return DataQualityPluginRegistry.plugins


def check_custom_python_engine_execute(
    run: Run,
    data_contract: DataContractSpecification,
    server: Server,
    spark: "SparkSession" = None,
    duckdb_connection: DuckDBPyConnection = None,
):
    from soda.common.config_helper import ConfigHelper

    ConfigHelper.get_instance().upsert_value("send_anonymous_usage_stats", False)

    if data_contract is None:
        run.log_warn("Cannot run custom python engine, as data contract is invalid.")
        return

    ## get the custom plugin folder stored in   DATACONTRACT_CUSTOMPLUGIN_FOLDER
    basefolderByMasterconfig: str = ""
    try:
        basefolderByMasterconfig = os.environ["DATACONTRACT_CUSTOMPLUGIN_FOLDER"]
    except Exception:
        ## no plugins configured, you can return
        ## Without a custom plugin folder there a no custom tests.
        run.log_warn(
            "Cannot run custom python engine, as no plugin folder is configured. Check for environment var 'DATACONTRACT_CUSTOMPLUGIN_FOLDER'. "
        )
        return

    run.log_info("Running engine 'custom_python_engine'")
    ## extract all checks with tag engine == custom_python keyword.
    custom_python_tests: list = [x for x in run.checks if (x.engine == ENGINE_KEYWORD)]
    ## if nothing, leavo.
    if len(custom_python_tests) == 0:
        ## nothing to test, bye.
        run.log_info(f"No custom tests for {ENGINE_KEYWORD} defined, leaving.")
        return

    ## setup duckdb, basically a copy, paste and remove elements I have not figured out yet. Taken from the original soda-engine-file.
    NewDuckdbConnection = None
    if server.type in ["s3", "gcs", "azure", "local"]:
        if server.format in ["json", "parquet", "csv", "delta"]:
            run.log_info(f"Configuring engine custom-python to connect to {server.type} {server.format} with duckdb")
            NewDuckdbConnection = get_duckdb_connection(data_contract, server, run, duckdb_connection)
        else:
            run.checks.append(
                Check(
                    type="general",
                    name="Check that format is supported",
                    result=ResultEnum.warning,
                    reason=f"Format {server.format} not yet supported by datacontract CLI",
                    engine="datacontract - custom_python_engine",
                )
            )
            run.log_warn(f"Format {server.format} not yet supported by datacontract CLI")
            ## Bail out, no connection yet
            return

    if duckdb_connection is None:
        duckdb_connection = NewDuckdbConnection

    ## Fixme: dirt. Configure this through environment or some other way.
    searchfolders: list[str] = [basefolderByMasterconfig]
    knownDQPluginsList = discover_plugins(searchfolders)

    # pattern of the definition
    # - type: custom
    #    description: Test that the difference between columnA(row=1) and columnB(row=1) is in a specific time range.
    # (..)
    #    engine: custom_python_engine
    #    implementation: |
    #      column: values
    #      plugin: OnJsonTestTimeDelta
    # FIXME: Change the column to a list datatype,
    ## pte -> plugins to execute
    pte: list = []

    # Available custom tests, initialize the Plugin class instances.
    # FIXME: change this in a way, that I can
    plugins: list = [P(duckdb_connection, run, custom_python_tests, data_contract) for P in knownDQPluginsList]
    ## need to initialize the plugin with the corresponding values for the columns to be tested.

    for ch in custom_python_tests:
        impl_yaml = yaml.safe_load(ch.implementation)
        print(impl_yaml)
        plugin_name = impl_yaml["plugin"]
        for knp in plugins:
            if not isinstance(knp, DataQualityAbstractBasePlugin):
                continue

            print(knp)
            if knp.PluginName == plugin_name:
                ## found a correct one. Set yaml implementation
                knp.dc_implementation = ch.implementation
                knp.yaml_implementation = impl_yaml
                pte.append(knp)
                ## keep track of plugins that have been found.

    for pg in pte:
        pg.runDataQualityTest()

    ## cleanup - disconnect duckdb
    duckdb_connection.close()
