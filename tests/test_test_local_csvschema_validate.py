
import pytest
from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract
from datacontract.model.run import ResultEnum, Run

runner = CliRunner()


@pytest.mark.skip(reason="https://github.com/sodadata/soda-core/issues/1992")
def _test_cli():
    result = runner.invoke(app, ["test", "./fixtures/local-json/datacontract.yaml"])
    assert result.exit_code == 0


## No skips.
# Error message makes no sense at all, test inactive
"""
FileNotFoundError: [Errno 2] No such file or directory: './tests/fixtures/csv_schema_validate/data/sample_data_no_errors.csv'
ERROR    root:run.py:87 [Errno 2] No such file or directory: './tests/fixtures/csv_schema_validate/data/sample_data_no_errors.csv'
"""
def test_local_csv_contract_OK_File_Clean():
    file = "fixtures/csv_schema_validate/csv_datacontract_no_errors.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 0   ## some dubious erro is present ...
    assert len(listOfPassed) == len(run.checks)
    assert len(listOfNonEval) == 0
    assert run.result == ResultEnum.passed

def test_csv_schema_validate_csv_datacontract_mismatch_column_amount():
    file = "fixtures/csv_schema_validate/csv_datacontract_mismatch_column_amount.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 1   ## Fix: Test if the error type/message matches ?
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == len( run.checks ) - len(listOfFailed)
    assert run.result == ResultEnum.failed

def test_csv_schema_validate_csv_datacontract_empty_file():
    file = "fixtures/csv_schema_validate/csv_datacontract_empty_file.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    ## Shall be an error as the query engine can't select anything nor create some kind of schema representation.
    assert run.result == ResultEnum.failed  ## is a fail, no idea why.
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == len( run.checks ) - len(listOfFailed)  ## one is in the list of failed.

## Still to fix
def test_csv_schema_validate_csv_datacontract_columnnumbers_match_types_differ():
    file = "fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_types_differ.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    assert run.result == ResultEnum.failed  ## is a fail, no idea why.
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 3   ## manually counted - should be the number of columns found as wrong.
    ## Fix this,
    assert len(listOfPassed) > 4    ## four columns are okay, error starts in c6, c5 is completly empty.
    assert len(listOfPassed) < 6
    assert len(listOfNonEval) >= 8  ## all columns after the error 'times two', as there are two tests per column


def _test_csv_schema_validate_csv_datacontract_columnnumbers_match_names_differ():
    file = "fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_differ.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    assert run.result == ResultEnum.failed  ## is a fail, no idea why.
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == len( run.checks ) - len(listOfFailed)  ## one is in the list of failed.


def _test_csv_schema_validate_csv_datacontract_columnnumbers_match_names_and_types_differ():
    file = "fixtures/csv_schema_validate/csv_datacontract_columnnumbers_match_names_and_types_differ.yaml"
    data_contract = DataContract(data_contract_file=file)
    run = data_contract.test()
    assert run.result == ResultEnum.failed  ## is a fail, no idea why.
    listOfFailed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.failed )]
    listOfPassed: list  = [x for x in run.checks if (x.result is not None and x.result.name == ResultEnum.passed )]
    listOfNonEval: list = [x for x in run.checks if (x.result is None)]

    assert len(listOfFailed) == 1
    assert len(listOfPassed) == 0
    assert len(listOfNonEval) == len( run.checks ) - len(listOfFailed)  ## one is in the list of failed.

