"""_summary_

Note:   This one is a pile of copy and paste, taken from the json validation and modified to do something similar on csv.
        Work in progress ;).

Raises:
    last_exception: _description_
    DataContractException: _description_

Returns:
    _type_: _description_

Yields:
    _type_: _description_
"""

import io
import logging
import os
from typing import List, Optional, Dict
import duckdb

## Re-use this to read from S3, should work also with csv.
from datacontract.engines.fastjsonschema.s3.s3_read_files import yield_s3_files_with_file_object
from datacontract.model.data_contract_specification import DataContractSpecification, Server, Field, Model
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import Check, ResultEnum, Run                           ## not yet referenced
from datacontract.engines.csvschema.csvExceptions import CSVSchemaValueException    ##, CSVSchemaInputFileException, CSVSchemaValidationException

def process_exceptions(run, exceptions: List[DataContractException], _file_locator = None):
    if not exceptions:
        return

    # Define the maximum number of errors to process (can be adjusted by defining an ENV variable).
    try:
        error_limit = int(os.getenv("DATACONTRACT_MAX_ERRORS", 500))
    except ValueError:
        # Fallback to default if environment variable is invalid.
        error_limit = 500

    # Calculate the effective limit to avoid index out of range
    limit = min(len(exceptions), error_limit)
    # Add all exceptions up to the limit - 1 to `run.checks`.
    DEFAULT_ERROR_MESSAGE = "An error occurred during validation phase. See the logs for more details."
    for exception in exceptions:
        msg_base        = exception.message or DEFAULT_ERROR_MESSAGE
        msg_enhanced    = f"CSV-Validation, Error at location '{_file_locator}', message: \n{msg_base}"
        cx = Check(
                type=exception.type,
                name=exception.name,
                result=exception.result,
                reason=exception.reason,
                model=exception.model,
                engine=exception.engine,
                message=msg_enhanced,
            )
        run.checks.extend( cx )
    # NOTE RS: Don't raise an exeption here, as all other files won't be processed.

## maps duckdb type to data contract.
## problem: mapping function used is not injective :-/, so this will have to work with some assumptions.
def map_duckdb_type_to_datacontract_type( columndatatype: str) -> str:
    cdt: str = columndatatype.lower()
    ##in:   [ 'double'  ]
    if cdt == 'double' or cdt == 'decimal' or cdt == 'number':
        return 'number'
    if cdt == 'bigint': # or cdt == 'decimal' or cdt == 'number':
        return 'integer'
    ## possible out: ['array', 'boolean', 'integer', 'number', 'date', 'object', 'string' ]
    if cdt == 'date':
        return 'date'
    if cdt == 'boolean' or cdt == 'bool':
        return 'boolean'
    if cdt == 'timestamp':
        return 'timestamp*'     ## fixme: test wildcard for timestamp_ntz, timestamp_tz
    # default
    return 'string'

# Fixme: write a test for this
def is_float_regex(s):
    import re
    float_regex = r'^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$'
    return bool(re.match(float_regex, s))


## Fixme: mismatch in datafile_in
def run_datatest_for_schema_validation( datafile_in, column_index_to_test: int, column_name_to_test: str, required_data_type: str) -> dict:

    conn        = duckdb.connect()
    tempTableName:str = 'ddb_temptable'
    currentFileNameDataIn: str = ''
    if isinstance(datafile_in,io.TextIOWrapper):
        currentFileNameDataIn = str(datafile_in.name)
    else:
        currentFileNameDataIn = datafile_in
    sql_c1:str  = f"""CREATE VIEW '{tempTableName}' AS SELECT * FROM read_csv('{currentFileNameDataIn}', hive_partitioning=1);"""

    r1          = conn.execute(sql_c1)
    sql_c2: str = f"""select "{column_name_to_test}" from "{tempTableName}" """     ## csv - we may have spaces in our column name
    qr          = conn.query(sql_c2).fetchall()

    success: list       = []
    failed:  list       = []
    cantDetermine: list = []
    lc1: int        = 0
    runresult       = True
    ## Note: the result of the query and fetch is some kind of tuple-shit.
    for se in qr:
        # Data precheck.
        # if nothing is present, there is no way to verify the data type.
        if se == '' or se is None:
            cantDetermine.append(lc1)
            continue
        ## next iteration of no values there - "tupled none's"
        forLoopShiftedContinue: bool = False
        if isinstance(se,tuple):
            for si in se:
                # check if none. if one item is none, dump it.
                ## third iteration of nothingness. Tuple of Strings with spaces.
                preprocessedInput: str = str(si).strip()
                if si == None or (preprocessedInput == '' or preprocessedInput is None):
                    cantDetermine.append(lc1)
                    forLoopShiftedContinue = True

        if True == forLoopShiftedContinue:
            continue

        ## try parse.
        ## date.
        if required_data_type == "date":
            from dateutil.parser import parse, ParserError
            try:
                testvalue: str = ''
                if isinstance(se, tuple):
                    testvalue = se[0]
                else:
                    testvalue = se
                result = parse(testvalue)
                print ( f"result with value = {result} is of type: {type(result)}" )
                success.append( lc1 )
            except ParserError as pax:
                print ( pax )
                failed.append( lc1 )
                runresult = False
        ## fixme: next data type that could be misinterpreted ?
        if required_data_type == "number":
            try:
                testvalue: str = ''
                if isinstance(se, tuple):
                    testvalue = se[0]
                else:
                    testvalue = se
                result = float(testvalue)
                print ( f"result with value = {result} is of type: {type(result)}" )
                success.append( lc1 )
            except ValueError as vex:
                print ( vex )
                failed.append( lc1 )
                runresult = False
        ## next in line
        lc1 +=1

    r1.close()
    NoDataInColumn: bool = False
    if len (cantDetermine) == len (qr):
        NoDataInColumn  = True
        runresult       = False
    res: dict            = dict()
    res['result']        = runresult
    res['NoDataInColumn']= NoDataInColumn
    res['failed_rows']   = failed
    res['success_rows']  = success
    res['cantDetermine'] = cantDetermine
    res['numberOfItemsTestet'] = len(qr)
    return res

def fieldresult_pattern_a( _columnindex, _matchfieldNameMap:str , _matchBoolean:bool, _localmessage, _schemaColumnName, _currentFieldName ):
    fieldresult: dict = dict()
    fieldresult['columnindex']          = _columnindex
    fieldresult[_matchfieldNameMap]     = _matchBoolean
    fieldresult['errorMessage']         = _localmessage
    fieldresult['columnNameContract']   = _schemaColumnName
    fieldresult['columnNameDatasource'] = _currentFieldName
    return fieldresult

# Fixme: the delimiter is not used at the moment.
def validate( csv_obj, schema: dict, model_name: str, _delimiter = None ) -> dict:
    import duckdb
    ## key = column name, values -> dictionary of props ( type: number etc.. )
    fields_from_model: dict             = schema['properties']
    required_fields_from_model: dict    = schema['required']
    csvSchema_test_results: dict        = dict()
    ##
    csvddb = None
    # fix potential datatype issue.
    # cvs_obj is io.TextIoThingy. works here, but later on a file name is needed for duckdb read_csv
    csvddb      = duckdb.read_csv( csv_obj )

    ## Use column name and type by index.
    col_names   = csvddb.columns
    col_types   = csvddb.types
    #print ( col_names )
    #print ( col_types )

    ## section names for dictionary keys
    key_column_name: str        = 'column_name'
    key_column_type: str        = 'column_datatype_from_duckdb'
    key_column_type_remap: str  = 'column_datatype_remapped_to_dccli'
    key_column_required: str    = 'column_datatype_required'

    datafields: dict    = dict()
    counter: int        = 0
    for colname in col_names:
        m: str = str( col_types[counter] )
        ndf: dict = dict()
        ndf[key_column_name]          = colname
        ndf[key_column_type]          = col_types[counter]
        ndf[key_column_type_remap]    = map_duckdb_type_to_datacontract_type(m)
        datafields[counter]           = ndf
        counter += 1
    ####
    ## apply model from schema.properties into this struct.

    ## print (f"118 \n\n {datafields}")
    columnindex: int = 0
    nameErrorsCounter: int   = 0
    typeErrorsCounter: int   = 0
    inconclusiveCounter: int = 0
    number_elements_in_datasource:  int = len( datafields )
    number_elements_in_contract:    int = len(fields_from_model)
    if ( number_elements_in_datasource != number_elements_in_contract):
        print( "number of elements between data contract and given source does not match")
        ## fixme: bail out, this error case is a no-go
        NumberOfColumnsDoesNotMatchMessage: str = \
            f"Number of columns does not match. datasource has {number_elements_in_datasource} columns, #{number_elements_in_contract} are specified in the contract"
        raise CSVSchemaValueException(NumberOfColumnsDoesNotMatchMessage)
    # Test for diff in described schema and autodetected schema.
    fieldsClear:list            = []
    listOfFieldResults: list    = []
    errorMessage: str           = ''
    ## Fixme: remove f'in code duplicates.
        # key, value
    for columnName, columnMetaData in fields_from_model.items():
        currentFieldSchema = datafields[columnindex]
        schemalogicalType  = columnMetaData[ 'type' ]
        schemaColumnName   = columnName     ## columndesc[ 'name' ]
        ## required_fields_from_model, fix this later if needed.
        ## note: need to possibly remap data types for dc-cli & duckdb interaction.
        currentFieldType:str        = currentFieldSchema[key_column_type_remap]
        currentFieldName:str        = currentFieldSchema[key_column_name]
        currentFieldTypeDuckDB: str = currentFieldSchema[key_column_type]
        columnNameMismatch: bool    = False
        if schemaColumnName != currentFieldName:
            localmessage = f"{errorMessage}\n Column name mismatch at {columnindex}, name: {schemaColumnName} does not match {currentFieldName}"
            errorMessage = f"{errorMessage}\n {localmessage}"
            nameErrorsCounter+=1
            mx = fieldresult_pattern_a(columnindex, 'isNameMismatch', True, localmessage, schemaColumnName, currentFieldName )
            listOfFieldResults.append( mx )
            columnNameMismatch = True
        if schemalogicalType != currentFieldType:
            # this gets dangerous, schema expects type date for this column, we have a mismatch, so go on and test all values of they can be parsed as type X
            if columnNameMismatch:
                localmessage = f"Column type mismatch at {columnindex} with name: {schemaColumnName}, type: {schemalogicalType} does not match expected {currentFieldType}"
                errorMessage = f"{errorMessage}\n {localmessage}"
                typeErrorsCounter+=1
                mx = fieldresult_pattern_a(columnindex, 'isTypeMismatch', True, localmessage, schemaColumnName, currentFieldName )
                listOfFieldResults.append( mx )

            if not columnNameMismatch:
                datatestresult = run_datatest_for_schema_validation( csv_obj, columnindex, schemaColumnName, schemalogicalType)
                if False == datatestresult['result'] and False == datatestresult['NoDataInColumn']:
                    ## schema mismatch and data mismatch, leavo with Exceptio
                    localmessage = f"Column type mismatch at {columnindex} with name: {schemaColumnName}, type: {schemalogicalType} does not match expected {currentFieldType}"
                    errorMessage = f"{errorMessage}\n {localmessage}"
                    typeErrorsCounter+=1
                    mx = fieldresult_pattern_a(columnindex, 'isTypeMismatch', True, localmessage, schemaColumnName, currentFieldName )
                    listOfFieldResults.append( mx )
                elif False == datatestresult['result'] and True == datatestresult['NoDataInColumn']:
                    ## For the moment: I have no data in the column to test, so the expected data type can't be validated.
                    ## duckdb is assuming a varchar/string as default in this case, so we will assume, that the data type matches and don't throw this as error.
                    ## Fixme: How to treat this. log and warn ?
                    localmessage = f"Potential type mismatch at {columnindex}, type: {schemalogicalType} does not match {currentFieldType}, can't be testet with data as no values are present"
                    errorMessage = f"{errorMessage}\n {localmessage}"
                    inconclusiveCounter+=1
                    ## Collect columns with type errors
                    mx = fieldresult_pattern_a(columnindex, 'isInconclusive', True, localmessage, schemaColumnName, currentFieldName )
                    listOfFieldResults.append( mx )
                elif True == datatestresult['result']:
                    ## ran through the values, this column is valid with the data types from the contract.
                    fieldsClear.append(columnindex)
                    errorMessage = f"{errorMessage}\ntype mismatch by autodetection, cleared up with testing the values at {columnindex}, type from schema: {schemalogicalType} mismatched with autodetect: {currentFieldType}, mismatch ignored."
                else:
                    ## schema data type mismatch, but data may fit  ( string is parseable as date )
                    ## if number of errors is 0 -> interpret with warning as useable.
                    ## else: raise as exception in form of collected messages.
                    rows_failed = datatestresult['failed_rows']
                    if  len(rows_failed) > 0:
                        errorMessage = f"{errorMessage}\n Warning: Column type mismatch at {columnindex}, name: {schemalogicalType} does not match {currentFieldType}, but given values are parseable as {schemalogicalType}."
                    else:
                        ## schema mismatch and at least a partial data mismatch, leavo with Exceptio
                        localmessage = f"Column type mismatch at {columnindex}, name: {schemaColumnName} does not match {currentFieldName}"
                        errorMessage = f"{errorMessage}\n {localmessage}"
                        typeErrorsCounter+=1
                        mx = fieldresult_pattern_a(columnindex, 'isTypeMismatch', True, localmessage, schemaColumnName, currentFieldName )
                        listOfFieldResults.append( mx )

        columnindex += 1

    csvSchema_test_results['field_results'] = listOfFieldResults
    return csvSchema_test_results


## Note: This one validated single json data object, it does not work with csv this way, it would get only one line of csv,
# which will make the validation not possible. With csv, for the schema validation the whole file is needed.
def precheck_file( csv_filename: str) -> dict:
    result: dict = dict()
    result['precheck_passed'] = True
    ## load file, check size. if null, deny further action.
    csv_raw = read_csv_file(csv_filename)
    if len(csv_raw) <= 3:
        # literally no content, don't bother to load. Minimal size: one column and one data entry, both size '1'.
        # Yes, in theory the file could consist of one column with the title 'a' and exactly one entry with i.e. the value '8'.
        # Without the quotes, otherwise we have more than 3 characters ;).
        result['precheck_passed']   = False
        result['fail_reason']       = f'File has content size of {len(csv_raw)} <= 3, which is kind of a pointless csv file.'
    return result

def validate_csv(
    schema: dict, model_name: str, csv_filenames: list[str], delimiter: str
) -> List[DataContractException]:

    logging.info(f"Validating csv stream for model: '{model_name}'.")
    exceptions: List[DataContractException] = []
    for csv_filename in csv_filenames:
        validationresults: dict = dict()
        ## fixme: execute some prechecks. is the file empty.
        pretest = precheck_file(csv_filename)
        if ( pretest['precheck_passed'] == True):
            try:
                ## this one calls the new CSV validator
                validationresults = validate(csv_filename, schema, model_name, delimiter)
            except CSVSchemaValueException as e:
                logging.warning(f"Validation failed for CSV object with type: '{model_name}'.")
                exceptions.append(
                    DataContractException(
                        type="schema",
                        name="Check that CSV has valid schema",
                        result=ResultEnum.failed,
                        reason=f"Schema validation failed, look at message for details. \n{e.message}",
                        model=model_name,
                        engine="csvschema",
                        message=e.message,
                    )
                )
        else:
            exceptions.append(
                    DataContractException(
                        type="schema",
                        name="Check that CSV has valid schema",
                        result=ResultEnum.failed,
                        reason=f"Precheck of the CSV failed - File empty ?",
                        model=model_name,
                        engine="csvschema",
                        message='Is the File empty ?',
                    )
                )
        ## evaluate the results in dictionary validationresults
        if len(validationresults) > 0 and 'field_results' in validationresults:
            for single_field in validationresults['field_results']:
                errortype: str = ''
                if 'isNameMismatch' in single_field and single_field['isNameMismatch'] == True:
                    errortype = 'Column names don\'t match'
                elif 'isTypeMismatch' in single_field and single_field['isTypeMismatch'] == True:
                    errortype = 'Column types don\'t match'
                elif 'isInconclusive' in single_field and single_field['isInconclusive'] == True:
                    errortype = 'Column data types between Contract and field are inconcolusive.'
                else:
                    errortype = 'None yet'
                column_name_failed: str = f"local datasource: {single_field['columnNameDatasource']}, contract: {single_field['columnNameContract']}"
                ##fixme: result: if only inconclusive -> make it a warning.
                dcx = DataContractException(
                    type="schema",
                    name="Checking CSV schema failed with: {errortype}",
                    result=ResultEnum.failed,
                    reason=f"Schema validation failed at {column_name_failed}, look at message for details. \n{single_field['errorMessage']}",
                    model=model_name,
                    engine="csvschema",
                    message=single_field['errorMessage'],
                )
                exceptions.append(dcx)

    if not exceptions:
        logging.info(f"All CSV objects in the stream passed validation for model: '{model_name}'.")
    return exceptions


def read_csv_file(file):
    ## file open -> return content
    rd: str = ''
    with open(file,'r') as csv_file:
        rd = csv_file.read()
    return rd
    ## note RS: removed the yield, will ruin the attempt

# write to tmp folder, return tmp file name.
# Note for a Fixme: store the tmp file names for later cleaning up the mess.
def store_csv_to_temp(file_content: str|bytes):
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open( tmp.name, 'w') as fmx:
        localfc: str = ''
        if isinstance(file_content, str):
            localfc = file_content
        if isinstance(file_content, bytes):
            localfc = file_content.decode("utf-8")      ## I just assume AWSS3 delivers UTF-8
        else:
            raise EncodingWarning("check_csv_schema.py@412:: Unexpected data type for file_content given by caller.Fix this ;)")
        fmx.write( localfc )
        fmx.flush()
        fmx.close()
    return tmp.name

def process_csv_file(run, schema, model_name, file, delimiter):
    # Fixme: What if file is "huge"

    # Validate the csv stream and collect exceptions.
    exceptions = validate_csv(schema, model_name, [file], delimiter)
    # Handle all errors from schema validation.
    process_exceptions(run, exceptions)


def process_local_file(run, server, schema, model_name):
    path = server.path
    if "{model}" in path:
        path = path.format(model=model_name)

    if os.path.isdir(path):
        return process_directory(run, path, server, model_name)
    else:
        logging.info(f"Processing file {path}")
        with open(path, "r") as file:
            process_csv_file(run, schema, model_name, file, server.delimiter)


def process_directory(run, path, server, model_name):
    success = True
    for filename in os.listdir(path):
        if filename.endswith(".csv"):  # or make this a parameter
            file_path = os.path.join(path, filename)
            with open(file_path, "r") as file:
                if not process_csv_file(run, model_name, validate, file, server.delimiter):
                    success = False
                    break
    return success


def process_s3_file(run, server, schema, model_name) -> bool:
    s3_endpoint_url = server.endpointUrl
    s3_location = server.location
    if "{model}" in s3_location:
        s3_location = s3_location.format(model=model_name)
    csv_files_found:bool = False
    exceptions: List[DataContractException] = []
    for fo, file_content in yield_s3_files_with_file_object(s3_endpoint_url, s3_location):
        localFile       = store_csv_to_temp(file_content)
        csv_files_found = True
        ## contains one file content, evaluate
        exceptions      = validate_csv(schema, model_name, [localFile], server.delimiter)
        # Handle all errors from schema validation.
        process_exceptions(run, exceptions, fo)

    # message. don't crash the whole execution if no file has been found.
    if csv_files_found is False:
        run.checks.append(
            Check(
                type="schema",
                name="Check that csv has valid schema",
                result=ResultEnum.warning,
                reason=f"Cannot find any file in {s3_location}",
                engine="datacontract"
            )
        )
        return False
    else:
        return True


def check_csvschema(run: Run, data_contract: DataContractSpecification, server: Server):
    run.log_info("Running engine csvschema")

    # Early exit conditions
    if server.format != "csv":
        run.checks.append(
            Check(
                type="schema",
                name="Check that csv has valid schema",
                result=ResultEnum.warning,
                reason="Server format is not 'csv'. Skip validating csvschema.",
                engine="csvschema",
            )
        )
        run.log_warn("csvschema: Server format is not 'csv'. Skip csvschema checks.")
        return

    if not data_contract.models:
        run.log_warn("csvschema: No models found. Skip csvschema checks.")
        return

    for model_name, model in iter(data_contract.models.items()):
        # Process the model
        run.log_info(f"csvschema: Converting model {model_name} to csv Schema")
        ## fixme: create mapper to a schema, whatever this is.
        schema = to_csvschema(model_name, model)
        run.log_info(f"csvschema: {schema}")

        # Process files based on server type
        if server.type == "local":
            process_local_file(run, server, schema, model_name )
        elif server.type == "s3":
            process_s3_file(run, server, schema, model_name )
            # NOTE: Is there anything else to evaluate ?
        elif server.type == "gcs":
            run.checks.append(
                Check(
                    type="schema",
                    name="Check that csv has valid schema",
                    model=model_name,
                    result=ResultEnum.info,
                    reason="csv Schema check skipped for GCS, as GCS is currently not supported",
                    engine="csvschema",
                )
            )
        elif server.type == "azure":
            run.checks.append(
                Check(
                    type="schema",
                    name="Check that csv has valid schema",
                    model=model_name,
                    result=ResultEnum.info,
                    reason="csv Schema check skipped for azure, as azure is currently not supported",
                    engine="csvschema",
                )
            )
        else:
            run.checks.append(
                Check(
                    type="schema",
                    name="Check that csv has valid schema",
                    model=model_name,
                    result=ResultEnum.warning,
                    reason=f"Server type {server.type} not supported",
                    engine="csvschema",
                )
            )
            return

        run.checks.append(
            Check(
                type="schema",
                name="Check that csv has valid schema",
                model=model_name,
                result=ResultEnum.passed,
                reason="All csv entries are valid.",
                engine="csvschema",
            )
        )

#####
## Dirty copy & paste section from jsonschema_converter.

def to_properties(fields: Dict[str, Field]) -> dict:
    properties = {}
    for field_name, field in fields.items():
        properties[field_name] = to_property(field)
    return properties

## fixme: remove all non csv-data formats
def to_property(field: Field) -> dict:
    property = {}
    json_type, json_format = convert_type_format(field.type, field.format)
    if json_type is not None:
        """
        if not field.required:

            From: https://json-schema.org/understanding-json-schema/reference/type
            The type keyword may either be a string or an array:

            If it's a string, it is the name of one of the basic types above.
            If it is an array, it must be an array of strings, where each string
            is the name of one of the basic types, and each element is unique.
            In this case, the JSON snippet is valid if it matches any of the given types.

            property["type"] = [json_type, "null"]
        else:
        """
        property["type"] = json_type
    if json_format is not None:
        property["format"] = json_format
    if field.primaryKey:
        property["primaryKey"] = field.primaryKey
    if field.unique:
        property["unique"] = True
    ##csv - does not exist
    #if json_type == "object":
    #    # TODO: any better idea to distinguish between properties and patternProperties?
    #    if field.fields.keys() and next(iter(field.fields.keys())).startswith("^"):
    #        property["patternProperties"] = to_properties(field.fields)
    #    else:
    #        property["properties"] = to_properties(field.fields)
    #    property["required"] = to_required(field.fields)

    if json_type == "array":
        property["items"] = to_property(field.items)
    if field.pattern:
        property["pattern"] = field.pattern
    if field.enum:
        property["enum"] = field.enum
    if field.minLength is not None:
        property["minLength"] = field.minLength
    if field.maxLength is not None:
        property["maxLength"] = field.maxLength
    if field.title:
        property["title"] = field.title
    if field.description:
        property["description"] = field.description
    if field.exclusiveMinimum is not None:
        property["exclusiveMinimum"] = field.exclusiveMinimum
    if field.exclusiveMaximum is not None:
        property["exclusiveMaximum"] = field.exclusiveMaximum
    if field.minimum is not None:
        property["minimum"] = field.minimum
    if field.maximum is not None:
        property["maximum"] = field.maximum
    if field.tags:
        property["tags"] = field.tags
    if field.pii:
        property["pii"] = field.pii
    if field.classification is not None:
        property["classification"] = field.classification

    # TODO: all constraints
    return property


def to_required(fields: Dict[str, Field]):
    required = []
    for field_name, field in fields.items():
        if field.required is True:
            required.append(field_name)
    return required

## fucks up the tests as it translates from a clearly defined data type to some shit.
## fixme: remove or change this, isn't helping at all.
def convert_type_format(type, format) -> tuple[str|None, str|None]:
    if type is None:
        return None, None
    if type.lower() in ["string", "varchar", "text"]:
        return "string", format
    if type.lower() in ["timestamp", "timestamp_tz", "date-time", "datetime"]:
        return "string", "date-time"
    if type.lower() in ["timestamp_ntz"]:
        return "string", None
    if type.lower() in ["date"]:
        return "date", "date"
        ##this does not help me.
        ##return "string", "date"
    if type.lower() in ["time"]:
        return "string", "time"
    if type.lower() in ["number", "decimal", "numeric", "float", "double"]:
        return "number", None
    if type.lower() in ["integer", "int", "long", "bigint"]:
        return "integer", None
    if type.lower() in ["boolean"]:
        return "boolean", None
    #if type.lower() in ["object", "record", "struct"]:
    #    return "object", None
    if type.lower() in ["array"]:
        return "array", None
    return None, None

def convert_format(self, format):
    if format is None:
        return None
    if format.lower() in ["uri"]:
        return "uri"
    if format.lower() in ["email"]:
        return "email"
    if format.lower() in ["uuid"]:
        return "uuid"
    if format.lower() in ["boolean"]:
        return "boolean"
    return None


def to_csvschema(model_key, model_value: Model) -> dict:

    model = {
        #"$schema": "http://json-schema.org/draft-07/schema#",
        #"$schema": "https://digital-preservation.github.io/csv-schema/csv-schema-1.2.html", ## that would be at least the future plan of this mess.
        "$schema": "none at the moment.",
        "type": "object",
        "properties": to_properties(model_value.fields),
        "required": to_required(model_value.fields),
    }
    if model_value.title:
        model["title"] = model_value.title
    if model_value.description:
        model["description"] = model_value.description

    return model
