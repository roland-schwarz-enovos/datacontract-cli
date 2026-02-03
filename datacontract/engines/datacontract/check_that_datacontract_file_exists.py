import os
import uuid
from datacontract.model.run import Check, Run


def check_that_datacontract_file_exists(run: Run, file_path: str):
    if file_path is None:
        return
    if file_path.startswith("http://") or file_path.startswith("https://"):
        return
    if not os.path.exists(file_path):
        run.checks.append(
            Check(
                id=str(uuid.uuid4()),
                type="lint",
                name="Check that data contract file exists",
                result="failed",
                reason=f"The file '{file_path}' does not exist.",
                engine="datacontract-cli",
            )
        )
        raise Exception(f"The file '{file_path}' does not exist.")
