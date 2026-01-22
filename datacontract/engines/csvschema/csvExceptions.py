# TODO: evaluate structure


class CSVSchemaValidationException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CSVSchemaInputFileException(CSVSchemaValidationException):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CSVSchemaValueException(CSVSchemaValidationException):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
