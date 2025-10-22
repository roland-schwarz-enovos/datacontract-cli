from inspect import currentframe
from pathlib import Path

import yaml

from datacontract.data_contract import DataContract
from datacontract.model.run import ResultEnum

# Fixme: Logging
# logging.basicConfig(level=logging.DEBUG, force=True)

"""
NOTE:   The number of passed and failed checks will change with the amount of implemented tests in this whole framework.
        The pass/fail numbers will have to be adapted.
Test cases:
[ 'local file', 'S3', 'any other data source' ].foreach{ input_location ->
    - empty csv file
            -> Fixme: fix the expected outcome
    - no columns headers present
            -> FIXME: Implement a test.
    - no errors present
            -> no messages expected
    - mismatch in number of columns
            -> specific message or excpetion
    - Number of columns match, names differ
            -> number of exceptions is equal to the number of mismatched column names
    - Number of columns match, types differ
            -> number of exceptions is equal to the number of mismatched column types
    - Number of columns match, names and types differ
            -> number of exceptions is equal to the number of ( mismatched column types + mismatched column names )
    - Number of columns match, names and types match, data does not match.
            -> Test to be done in quality ?
    - wrong file format or anything else
            ->FIXME: not yet implemented: what type of error to expect ?
}

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


def test_local_empty_file():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_empty_file.yaml"
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")
    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 20 non-evaluated.
    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 20


def _test_local_wrong_file_format():
    pass


def test_local_no_errors_present():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_no_errors.yaml"
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")
    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.passed
    ## no failure expected, all pass
    assert len(listOfFailed) == 0
    assert len(listOfPassed) == 21
    assert len(listOfNonEval) == 0


def test_local_number_of_columns_mismatch():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_mismatch_column_amount.yaml"
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")
    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 20 non-evaluated.
    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 20


def test_local_numbers_match_names_differ():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_differ.yaml"
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")
    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.failed
    ## four failures expected, none passed, 20 non-evaluated.
    assert len(listOfFailed) == 4
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 20


def test_local_numbers_match_types_differ():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_types_differ.yaml"
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")

    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.failed
    ## three failures expected, none passed, 20 non-eval
    assert len(listOfFailed) == 3
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 20


def test_local_numbers_match_names_and_types_differ():
    datacontract_file = (
        "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_and_types_differ.yaml"
    )
    data_contract_str, sourceExists = _setup_datacontract(datacontract_file)
    if not sourceExists:
        assert False, f"{get_linenumber()}: source file missing."

    data_contract = DataContract(data_contract_str=data_contract_str)
    run = data_contract.test()

    # TODO: evaluate the amout of failures that should be returned.
    if debug:
        print(run.pretty())
        print(f"ln = {get_linenumber()}")
        print(f"run result = {run.result}")

    listOfFailed, listOfPassed, listOfNonEval = _filterResults(run)

    assert run.result == ResultEnum.failed
    ## expected schema error with 3 errors ( three differences in names, types wont be checked any more), zero passed, 20 non-evaluated.
    assert len(listOfFailed) == 3
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 20


"""
test_local_empty_file()
test_local_no_errors_present()
test_local_number_of_columns_mismatch()
test_local_numbers_match_names_and_types_differ()
test_local_numbers_match_names_differ()
test_local_numbers_match_types_differ()
test_local_numbers_match_names_and_types_differ()
"""
# Stage 2: Testing on S3 buckets
# FIXME: Change the obvious absence of missing test cases. And maybe the missing implementation throughout the implementation in the csv schema test routines.
