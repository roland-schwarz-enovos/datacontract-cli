# TODO: evaluate structure
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum  ## not yet referenced

## Fix this, that may be the reason why no data are transmitted, casting errors ...

class CSVSchemaValidationException( DataContractException) :
    def __init__(self,
                type,
                name,
                reason,
                engine,
                model=None,
                original_exception=None,
                result: ResultEnum = ResultEnum.failed,
                message="Run operation failed" ):
        super().__init__( type, name, reason, engine, model, original_exception, result, message )


class CSVSchemaInputFileException(CSVSchemaValidationException):
    def __init__(self,
                type,
                name,
                reason,
                engine,
                model=None,
                original_exception=None,
                result: ResultEnum = ResultEnum.failed,
                message="Run operation failed" ):

        super().__init__( type, name, reason, engine, model, original_exception, result, message )

class CSVSchemaValueException(CSVSchemaValidationException):
    def __init__(self,
                type,
                name,
                reason,
                engine,
                model=None,
                original_exception=None,
                result: ResultEnum = ResultEnum.failed,
                message="Run operation failed" ):

        super().__init__( type, name, reason, engine, model, original_exception, result, message )


