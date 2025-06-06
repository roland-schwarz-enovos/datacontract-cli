import pytest
from typer.testing import CliRunner

from datacontract.cli import app
from datacontract.data_contract import DataContract

runner = CliRunner()


@pytest.mark.skip(reason="https://github.com/sodadata/soda-core/issues/1992")
def _test_cli():
    result = runner.invoke(app, ["test", "./fixtures/local-json/datacontract.yaml"])
    assert result.exit_code == 0


@pytest.mark.skip(reason="https://github.com/sodadata/soda-core/issues/1992")
def _test_local_json():
    data_contract = DataContract(data_contract_file="fixtures/local-json/datacontract.yaml")
    run = data_contract.test()
    print(run)
    assert run.result == "passed"


"""
Idea of this test: create a nested json structure to verify that the new recursive routine finds all elements nested in the
fields structure.

"""


def test_local_json_complex_number_of_checks():
    expected_number_of_checks_created = 12  ## depends on the checks defined in the contract.
    fl: str = "fixtures/local-json-nested/datacontract.yaml"
    data_contract = DataContract(data_contract_file=fl)
    run = data_contract.test()
    number_of_checks_created: int = len(run.checks)

    # print ( f"current test: '_test_local_json_complex_number_of_checks' \tnumber of checks created = {number_of_checks_created}")
    assert number_of_checks_created == expected_number_of_checks_created


# test_local_json_complex_number_of_checks()
