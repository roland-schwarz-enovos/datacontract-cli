# from testcontainers.postgres import PostgresContainer

from datacontract.data_contract import DataContract

# from typer.testing import CliRunner
from datacontract.model.run import ResultEnum

# Fixme: Logging ?
# logging.basicConfig(level=logging.DEBUG, force=True)

"""
Test cases:
[ 'local file', 'S3', 'any other data source' ].foreach{ input_location ->
    - empty csv file
            -> Fixme: fix the expected outcome
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
            ->fixme: what type of error to expect ?
}

"""

## Local file tests
## have to initialize a complete test to run all steps of the stage ?


## fixme: import odcs ?
def _setup_datacontract(file):
    with open(file) as data_contract_file:
        data_contract_str = data_contract_file.read()
    # no man
    # port = postgres.get_exposed_port(5432)
    # data_contract_str = data_contract_str.replace("5432", port)
    return data_contract_str


## NOTE to fixme: the file structure seems not wo work within the test environment.
def test_local_empty_file():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_empty_file.yaml"
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("72")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))
    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 21


def _test_local_wrong_file_format():
    assert 0 == 0


def test_local_no_errors_present():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_no_errors.yaml"
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("103")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 0
    # NOTE: This one will change with the amount of implemented tests
    assert len(listOfPassed) == 22
    assert len(listOfNonEval) == 0


def test_local_number_of_columns_mismatch():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_mismatch_column_amount.yaml"
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("133")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 1
    # NOTE: This one will change with the amount of implemented tests
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == 21
    assert 0 == 0


def test_local_numbers_match_names_differ():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_differ.yaml"
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("164")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 0
    # NOTE: This one will change with the amount of implemented tests
    assert len(listOfPassed) == 22
    assert len(listOfNonEval) == 0
    assert 0 == 0


def test_local_numbers_match_types_differ():
    datacontract_file = "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_types_differ.yaml"
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("194")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))

    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 0
    # NOTE: This one will change with the amount of implemented tests
    assert len(listOfPassed) == 22
    assert len(listOfNonEval) == 0
    assert 1 == 1


def test_local_numbers_match_names_and_types_differ():
    datacontract_file = (
        "./tests/fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_and_types_differ.yaml"
    )
    data_contract_str = _setup_datacontract(datacontract_file)
    data_contract = DataContract(data_contract_str=data_contract_str)

    run = data_contract.test()
    print(run.pretty())
    if run.result == ResultEnum.error:
        ## some problem running this, fail the test.
        assert False, "Hard error running this test."

    ## if this runs through, I expect a failed. empty file with no columns or column definition at all must fail with a schema validation error.
    # TODO: evaluate the amout of failures that should be returned.
    listOfFailed: list = [x for x in run.checks if (x.result is not None and x.result.name == "failed")]
    listOfPassed: list = [x for x in run.checks if (x.result is not None and x.result.name == "passed")]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    print("225")
    print(len(listOfFailed))
    print(len(listOfPassed))
    print(len(listOfNonEval))
    assert run.result == ResultEnum.failed
    ## one failure expected, none passed, 21 non-evaluated.
    assert len(listOfFailed) == 0
    # NOTE: This one will change with the amount of implemented tests
    assert len(listOfPassed) == 22
    assert len(listOfNonEval) == 0
    assert 0 == 0


## S3 tests ?

test_local_empty_file()
test_local_no_errors_present()
test_local_number_of_columns_mismatch()
test_local_numbers_match_names_and_types_differ()
test_local_numbers_match_names_differ()
test_local_numbers_match_types_differ()
test_local_numbers_match_names_and_types_differ()
