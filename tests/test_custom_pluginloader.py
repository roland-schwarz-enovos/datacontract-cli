from inspect import currentframe
from pathlib import Path

import yaml

# Fixme: Logging
# logging.basicConfig(level=logging.DEBUG, force=True)

"""
NOTE:   The number of passed and failed checks will change with the amount of implemented tests in this whole framework.
        The pass/fail numbers will have to be adapted.
Test cases:

-- test plugin discovery / directory handling
-- test loading of plugin
-- test execution of sample
-- test evaluation of results ?
"""

# Stage 1: Local file tests

debug: bool = True
basefolder: str = "/home/roland.schwarz/projects/datacontract-cli-fork/datacontract-cli/"


# FIXME: import odcs, parse path and look if file exists
def _setup_datacontract(file):
    with open(file) as data_contract_file:
        data_contract_str = data_contract_file.read()
    # no man
    datasource_exists: bool = False
    # port = postgres.get_exposed_port(5432)
    # data_contract_str = data_contract_str.replace("5432", port)
    # Fixme: parse contract for path , test if file exists.
    dcs_yamlstruct = yaml.safe_load(data_contract_str)
    servers_all = dcs_yamlstruct["servers"]
    server = servers_all[0]
    datasourcepath = server["path"]
    fsp = Path(datasourcepath)
    if fsp.is_file():
        datasource_exists = True
    return (data_contract_str, datasource_exists)


## evaluation helpers
def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno  # <--- Line of caller


def _filterResults(_run):
    listOfFailed: list = [x for x in _run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in _run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in _run.checks if (x.result is None)]
    if debug:
        print(len(listOfFailed))
        print(len(listOfPassed))
        print(len(listOfNonEval))
    return listOfFailed, listOfPassed, listOfNonEval


## NOTE to FIXME: the file structure seems not wo work within the test environment.
## Running this through a python script call in VS Code works perfectly, running uv run pytest on this test script results in 'no such file or directory' for all tests.


def _test_local_empty_file():
    pass


def _test_local_wrong_file_format():
    pass
