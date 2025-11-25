import re
import uuid
from dataclasses import dataclass
from typing import List
from venv import logger

import yaml

from datacontract.export.sql_type_converter import convert_to_sql_type
from datacontract.model.data_contract_specification import DataContractSpecification, Quality, Server
from datacontract.model.run import Check


@dataclass
class QuotingConfig:
    quote_field_name: bool = False
    quote_model_name: bool = False
    quote_model_name_with_backticks: bool = False
    rewrite_csv_columnnames: bool = False


def create_checks(data_contract_spec: DataContractSpecification, server: Server) -> List[Check]:
    checks: List[Check] = []
    for model_key, model_value in data_contract_spec.models.items():
        model_checks = to_model_checks(model_key, model_value, server)
        checks.extend(model_checks)
    checks.extend(to_servicelevel_checks(data_contract_spec))
    checks.append(to_quality_check(data_contract_spec))
    return [check for check in checks if check is not None]


def to_model_checks(model_key, model_value, server: Server) -> List[Check]:
    checks: List[Check] = []
    server_type = server.type if server and server.type else None
    model_name = to_model_name(model_key, model_value, server_type)
    fields = model_value.fields

    check_types = is_check_types(server)
    type1       = server.type if server and server.type else None
    config = QuotingConfig(
        quote_field_name=type1 in ["postgres", "sqlserver"],
        quote_model_name=type1 in ["postgres", "sqlserver"],
        quote_model_name_with_backticks=type1 == "bigquery",
        rewrite_csv_columnnames=False
    )
    quoting_config = config
    rcres = to_model_checks_rc(0, "", fields, model_name, model_value, check_types, quoting_config, server_type, server)
    checks.extend(rcres)
    return checks

def to_model_checks_rc(
    level, super_field_name, fields, model_name, model_value, check_types, quoting_config, server_type, server: Server ) -> List[Check]:
    checks: List[Check] = []
    server_format = server.format if server.format is not None else ""
    level = level + 1

    for _field_name, field in fields.items():
        # Explanation: This addon is for json structures with nested structures. It modifies the field name to allow duckdb to query for a field name
        # with SQL. If the server_format equals json, the fieldname is build up with the field names from the recursive call structure build up
        # by the calls through those two functions.
        # to query inside of json object structures, we need to name the fields in an object-like name pattern, separated by dots. This is done here.
        field_name: str = _field_name
        if server_format == "json":
            if (level - 1) >= 1:  # If we are on the initial object level, don't apply any changes.
                field_name = f"{super_field_name}.{_field_name}"
        if server_format == "csv" and (not field_name.isalnum()):
            quoting_config.rewrite_csv_columnnames    = True
##            quoting_config['rewrite_csv_columnnames'] = True
        # FIXME: Fix the logic to avoid this empty if expression
        if server_format == "json" and ((level - 1) >= 1):
            None  ## "nop()"
        else:
            checks.append(check_field_is_present(model_name, field_name, quoting_config))   ## FIXME: review if the field_name used is correct.
        if check_types and field.type is not None:
            sql_type: str = convert_to_sql_type(field, server_type)
            checks.append(check_field_type(model_name, field_name, sql_type, quoting_config))
        if field.required:
            checks.append(check_field_required(model_name, field_name, quoting_config))
        if field.unique:
            checks.append(check_field_unique(model_name, field_name, quoting_config))
        if field.minLength is not None:
            checks.append(check_field_min_length(model_name, field_name, field.minLength, quoting_config))
        if field.maxLength is not None:
            checks.append(check_field_max_length(model_name, field_name, field.maxLength, quoting_config))
        if field.minimum is not None:
            checks.append(check_field_minimum(model_name, field_name, field.minimum, quoting_config))
        if field.maximum is not None:
            checks.append(check_field_maximum(model_name, field_name, field.maximum, quoting_config))
        if field.exclusiveMinimum is not None:
            checks.append(check_field_minimum(model_name, field_name, field.exclusiveMinimum, quoting_config))
            checks.append(check_field_not_equal(model_name, field_name, field.exclusiveMinimum, quoting_config))
        if field.exclusiveMaximum is not None:
            checks.append(check_field_maximum(model_name, field_name, field.exclusiveMaximum, quoting_config))
            checks.append(check_field_not_equal(model_name, field_name, field.exclusiveMaximum, quoting_config))
        if field.pattern is not None:
            print ("93 :: pattern create check entered.")
            checks.append(check_field_regex(model_name, field_name, field.pattern, quoting_config))
        if field.enum is not None and len(field.enum) > 0:
            checks.append(check_field_enum(model_name, field_name, field.enum, quoting_config))
        if field.quality is not None and len(field.quality) > 0:
            quality_list = check_quality_list(model_name, field_name, field.quality, quoting_config)
            if (quality_list is not None) and len(quality_list) > 0:
                checks.extend(quality_list)
        # TODO references: str = None
        # TODO format
        # Addon to identify the need for a recursive call for json structures, append it to the existing checks.
        if len(field.fields) > 0 and server_format == "json":
            tmr = to_model_checks_rc(
                level,
                field_name,
                field.fields,
                model_name,
                model_value,
                check_types,
                quoting_config,
                server_type,
                server,
            )
            checks.extend(tmr)

    if model_value.quality is not None and len(model_value.quality) > 0:
        quality_list = check_quality_list(model_name, None, model_value.quality)
        if (quality_list is not None) and len(quality_list) > 0:
            checks.extend(quality_list)

    return checks


def checks_for(model_name: str, quoting_config: QuotingConfig, check_type: str) -> str:
    if quoting_config.quote_model_name:
        return f'checks for "{model_name}"'
    elif quoting_config.quote_model_name_with_backticks and check_type not in ["field_is_present", "field_type"]:
        return f"checks for `{model_name}`"
    return f"checks for {model_name}"


def is_check_types(server: Server) -> bool:
    if server is None:
        return True
    return server.format != "json" and server.format != "csv" and server.format != "avro"


def to_model_name(model_key, model_value, server_type):
    if server_type == "databricks":
        if model_value.config is not None and "databricksTable" in model_value.config:
            return model_value.config["databricksTable"]
    if server_type == "snowflake":
        if model_value.config is not None and "snowflakeTable" in model_value.config:
            return model_value.config["snowflakeTable"]
    if server_type == "sqlserver":
        if model_value.config is not None and "sqlserverTable" in model_value.config:
            return model_value.config["sqlserverTable"]
    if server_type == "postgres" or server_type == "postgresql":
        if model_value.config is not None and "postgresTable" in model_value.config:
            return model_value.config["postgresTable"]
    return model_key


def check_field_is_present(model_name, field_name, quoting_config: QuotingConfig = QuotingConfig()) -> Check:
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )
    check_type = "field_is_present"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                "schema": {
                    "name": check_key,
                    "fail": {
                        "when required column missing": [field_name],
                    },
                }
            }
        ]
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field '{field_name}' is present",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_type(
    model_name: str, field_name: str, expected_type: str, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )
    check_type = "field_type"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                "schema": {
                    "name": check_key,
                    "fail": {
                        "when wrong column type": {
                            field_name: expected_type,
                        },
                    },
                }
            }
        ]
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has type {expected_type}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_required(model_name: str, field_name: str, quoting_config: QuotingConfig = QuotingConfig()):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_required"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"missing_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has no missing values",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_unique(model_name: str, field_name: str, quoting_config: QuotingConfig = QuotingConfig()):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_unique"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"duplicate_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that unique field {field_name} has no duplicate values",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_min_length(
    model_name: str, field_name: str, min_length: int, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_min_length"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid min length": min_length,
                },
            }
        ]
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has a min length of {min_length}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_max_length(
    model_name: str, field_name: str, max_length: int, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_max_length"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid max length": max_length,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has a max length of {max_length}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_minimum(
    model_name: str, field_name: str, minimum: int, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name


    check_type = "field_minimum"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid min": minimum,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has a minimum of {minimum}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_maximum(
    model_name: str, field_name: str, maximum: int, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_maximum"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid max": maximum,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} has a maximum of {maximum}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_not_equal(
    model_name: str, field_name: str, value: int, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_not_equal"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "invalid values": [value],
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} is not equal to {value}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_enum(model_name: str, field_name: str, enum: list, quoting_config: QuotingConfig = QuotingConfig()):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_enum"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid values": enum,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} only contains enum values {enum}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_regex(model_name: str, field_name: str, pattern: str, quoting_config: QuotingConfig = QuotingConfig()):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_regex"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) = 0": {
                    "name": check_key,
                    "valid regex": pattern,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that field {field_name} matches regex pattern {pattern}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_row_count(model_name: str, threshold: str, quoting_config: QuotingConfig = QuotingConfig()):
    check_type = "row_count"
    check_key = f"{model_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"row_count {threshold}": {"name": check_key},
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="schema",
        type=check_type,
        name=f"Check that model {model_name} has row_count {threshold}",
        model=model_name,
        field=None,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_model_duplicate_values(
    model_name: str, cols: list[str], threshold: str, quoting_config: QuotingConfig = QuotingConfig()
):
    check_type = "model_duplicate_values"
    check_key = f"{model_name}__{check_type}"
    col_joined = ", ".join(cols)
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"duplicate_count({col_joined}) {threshold}": {"name": check_key},
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="quality",
        type=check_type,
        name=f"Check that model {model_name} has duplicate_count {threshold} for columns {col_joined}",
        model=model_name,
        field=None,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_duplicate_values(
    model_name: str, field_name: str, threshold: str, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_duplicate_values"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"duplicate_count({field_name_for_soda}) {threshold}": {
                    "name": check_key,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="quality",
        type=check_type,
        name=f"Check that field {field_name} has duplicate_count {threshold}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_null_values(
    model_name: str, field_name: str, threshold: str, quoting_config: QuotingConfig = QuotingConfig()
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_null_values"
    check_key = f"{model_name}__{field_name}__{check_type}"
    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"missing_count({field_name_for_soda}) {threshold}": {
                    "name": check_key,
                },
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="quality",
        type=check_type,
        name=f"Check that field {field_name} has missing_count {threshold}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_invalid_values(
    model_name: str,
    field_name: str,
    threshold: str,
    valid_values: list = None,
    quoting_config: QuotingConfig = QuotingConfig(),
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_invalid_values"
    check_key = f"{model_name}__{field_name}__{check_type}"

    sodacl_check_config = {
        "name": check_key,
    }

    if valid_values is not None:
        sodacl_check_config["valid values"] = valid_values

    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"invalid_count({field_name_for_soda}) {threshold}": sodacl_check_config,
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="quality",
        type=check_type,
        name=f"Check that field {field_name} has invalid_count {threshold}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_field_missing_values(
    model_name: str,
    field_name: str,
    threshold: str,
    missing_values: list = None,
    quoting_config: QuotingConfig = QuotingConfig(),
):
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    check_type = "field_missing_values"
    check_key = f"{model_name}__{field_name}__{check_type}"

    sodacl_check_config = {
        "name": check_key,
    }

    if missing_values is not None:
        # Filter out null/None values as SodaCL handles these automatically
        filtered_missing_values = [v for v in missing_values if v is not None]
        if filtered_missing_values:
            sodacl_check_config["missing values"] = filtered_missing_values

    sodacl_check_dict = {
        checks_for(model_name, quoting_config, check_type): [
            {
                f"missing_count({field_name_for_soda}) {threshold}": sodacl_check_config,
            }
        ],
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="quality",
        type=check_type,
        name=f"Check that field {field_name} has missing_count {threshold}",
        model=model_name,
        field=field_name,
        fieldname_original=field_name_orig,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def check_quality_list(
    model_name, field_name, quality_list: List[Quality], quoting_config: QuotingConfig = QuotingConfig()
) -> List[Check]:
    field_name_orig: str = field_name
    if quoting_config.rewrite_csv_columnnames == True:
        ## rewrite with function 'borrowed' from duckdb cpp code
        field_name = NormalizeColumnName( field_name )

    checks: List[Check] = []

    count = 0
    for quality in quality_list:
        if quality.type == "sql":
            if field_name is None:
                check_key = f"{model_name}__quality_sql_{count}"
                check_type = "field_quality_sql"
            else:
                check_key = f"{model_name}__{field_name}__quality_sql_{count}"
                check_type = "model_quality_sql"
            threshold = to_sodacl_threshold(quality)
            query = prepare_query(quality, model_name, field_name, quoting_config)
            if query is None:
                logger.warning(f"Quality check {check_key} has no query")
                continue
            if threshold is None:
                logger.warning(f"Quality check {check_key} has no valid threshold")
                continue

            if quoting_config.quote_model_name:
                model_name_for_soda = f'"{model_name}"'
            else:
                model_name_for_soda = model_name
            sodacl_check_dict = {
                f"checks for {model_name_for_soda}": [
                    {
                        f"{check_key} {threshold}": {
                            f"{check_key} query": query,
                            "name": check_key,
                        },
                    }
                ]
            }
            checks.append(
                Check(
                    id=str(uuid.uuid4()),
                    key=check_key,
                    category="quality",
                    type=check_type,
                    name=quality.description if quality.description is not None else "Quality Check",
                    model=model_name,
                    field=field_name,
                    fieldname_original=field_name_orig,
                    engine="soda",
                    language="sodacl",
                    implementation=yaml.dump(sodacl_check_dict),
                )
            )
        elif quality.metric is not None:
            threshold = to_sodacl_threshold(quality)

            if threshold is None:
                logger.warning(f"Quality metric {quality.metric} has no valid threshold")
                continue

            if quality.metric == "rowCount":
                checks.append(check_row_count(model_name, threshold, quoting_config))
            elif quality.metric == "duplicateValues":
                if field_name is None:
                    # TODO check that quality.arguments.get("properties") is a list of strings and contains at lease one property
                    checks.append(
                        check_model_duplicate_values(
                            model_name, quality.arguments.get("properties"), threshold, quoting_config
                        )
                    )
                else:
                    checks.append(check_field_duplicate_values(model_name, field_name, threshold, quoting_config))
            elif quality.metric == "nullValues":
                if field_name is not None:
                    checks.append(check_field_null_values(model_name, field_name, threshold, quoting_config))
                else:
                    logger.warning("Quality check nullValues is only supported at field level")
            elif quality.metric == "invalidValues":
                if field_name is not None:
                    valid_values = quality.arguments.get("validValues") if quality.arguments else None
                    checks.append(
                        check_field_invalid_values(model_name, field_name, threshold, valid_values, quoting_config)
                    )
                else:
                    logger.warning("Quality check invalidValues is only supported at field level")
            elif quality.metric == "missingValues":
                if field_name is not None:
                    missing_values = quality.arguments.get("missingValues") if quality.arguments else None
                    checks.append(
                        check_field_missing_values(model_name, field_name, threshold, missing_values, quoting_config)
                    )
                else:
                    logger.warning("Quality check missingValues is only supported at field level")
            else:
                logger.warning(f"Quality check {quality.metric} is not yet supported")

        count += 1

    return checks


def prepare_query(
    quality: Quality, model_name: str, field_name: str = None, quoting_config: QuotingConfig = QuotingConfig()
) -> str | None:
    if quality.query is None:
        return None
    if quality.query == "":
        return None

    query = quality.query

    if quoting_config.quote_field_name:
        field_name_for_soda = f'"{field_name}"'
    else:
        field_name_for_soda = field_name

    if quoting_config.quote_model_name:
        model_name_for_soda = f'"{model_name}"'
    elif quoting_config.quote_model_name_with_backticks:
        model_name_for_soda = f"`{model_name}`"
    else:
        model_name_for_soda = model_name

    query = re.sub(r'["\']?\{model}["\']?', model_name_for_soda, query)
    query = re.sub(r'["\']?{schema}["\']?', model_name_for_soda, query)
    query = re.sub(r'["\']?{table}["\']?', model_name_for_soda, query)

    if field_name is not None:
        query = re.sub(r'["\']?{field}["\']?', field_name_for_soda, query)
        query = re.sub(r'["\']?{column}["\']?', field_name_for_soda, query)
        query = re.sub(r'["\']?{property}["\']?', field_name_for_soda, query)

    return query


def to_sodacl_threshold(quality: Quality) -> str | None:
    if quality.mustBe is not None:
        return f"= {quality.mustBe}"
    if quality.mustNotBe is not None:
        return f"!= {quality.mustNotBe}"
    if quality.mustBeGreaterThan is not None:
        return f"> {quality.mustBeGreaterThan}"
    if quality.mustBeGreaterOrEqualTo is not None:
        return f">= {quality.mustBeGreaterOrEqualTo}"
    if quality.mustBeGreaterThanOrEqualTo is not None:
        return f">= {quality.mustBeGreaterThanOrEqualTo}"
    if quality.mustBeLessThan is not None:
        return f"< {quality.mustBeLessThan}"
    if quality.mustBeLessOrEqualTo is not None:
        return f"<= {quality.mustBeLessOrEqualTo}"
    if quality.mustBeLessThanOrEqualTo is not None:
        return f"<= {quality.mustBeLessThanOrEqualTo}"
    if quality.mustBeBetween is not None:
        if len(quality.mustBeBetween) != 2:
            logger.warning(
                f"Quality check has invalid mustBeBetween, must have exactly 2 integers in an array: {quality.mustBeBetween}"
            )
            return None
        return f"between {quality.mustBeBetween[0]} and {quality.mustBeBetween[1]}"
    if quality.mustNotBeBetween is not None:
        if len(quality.mustNotBeBetween) != 2:
            logger.warning(
                f"Quality check has invalid mustNotBeBetween, must have exactly 2 integers in an array: {quality.mustNotBeBetween}"
            )
            return None
        return f"not between {quality.mustNotBeBetween[0]} and {quality.mustNotBeBetween[1]}"
    return None


def to_servicelevel_checks(data_contract_spec: DataContractSpecification) -> List[Check]:
    checks: List[Check] = []
    if data_contract_spec.servicelevels is None:
        return checks
    if data_contract_spec.servicelevels.freshness is not None:
        checks.append(to_servicelevel_freshness_check(data_contract_spec))
    if data_contract_spec.servicelevels.retention is not None:
        checks.append(to_servicelevel_retention_check(data_contract_spec))
    # only return checks that are not None
    return [check for check in checks if check is not None]


def to_servicelevel_freshness_check(data_contract_spec: DataContractSpecification) -> Check | None:
    if data_contract_spec.servicelevels.freshness.timestampField is None:
        return None
    freshness_threshold = data_contract_spec.servicelevels.freshness.threshold
    if freshness_threshold is None:
        logger.info("servicelevel.freshness.threshold is not defined")
        return None

    if not (
        "d" in freshness_threshold
        or "D" in freshness_threshold
        or "h" in freshness_threshold
        or "H" in freshness_threshold
        or "m" in freshness_threshold
        or "M" in freshness_threshold
    ):
        logger.info("servicelevel.freshness.threshold must be in days, hours, or minutes (e.g., PT1H, or 1h)")
        return None
    timestamp_field_fully_qualified = data_contract_spec.servicelevels.freshness.timestampField
    if "." not in timestamp_field_fully_qualified:
        logger.info("servicelevel.freshness.timestampField is not fully qualified, skipping freshness check")
        return None
    if timestamp_field_fully_qualified.count(".") > 1:
        logger.info(
            "servicelevel.freshness.timestampField contains multiple dots, which is currently not supported, skipping freshness check"
        )
        return None
    model_name = timestamp_field_fully_qualified.split(".")[0]
    field_name = timestamp_field_fully_qualified.split(".")[1]
    threshold = freshness_threshold
    threshold = threshold.replace("P", "")
    threshold = threshold.replace("T", "")
    threshold = threshold.lower()
    if model_name not in data_contract_spec.models:
        logger.info(f"Model {model_name} not found in data_contract_spec.models, skipping freshness check")
        return None

    check_type = "servicelevel_freshness"
    check_key = "servicelevel_freshness"

    sodacl_check_dict = {
        checks_for(model_name, QuotingConfig(), check_type): [
            {
                f"freshness({field_name}) < {threshold}": {
                    "name": check_key,
                },
            }
        ]
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="servicelevel",
        type=check_type,
        name="Freshness",
        model=model_name,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def to_servicelevel_retention_check(data_contract_spec) -> Check | None:
    if data_contract_spec.servicelevels.retention is None:
        return None
    if data_contract_spec.servicelevels.retention.unlimited is True:
        return None
    if data_contract_spec.servicelevels.retention.timestampField is None:
        logger.info("servicelevel.retention.timestampField is not defined")
        return None
    if data_contract_spec.servicelevels.retention.period is None:
        logger.info("servicelevel.retention.period is not defined")
        return None
    timestamp_field_fully_qualified = data_contract_spec.servicelevels.retention.timestampField
    if "." not in timestamp_field_fully_qualified:
        logger.info("servicelevel.retention.timestampField is not fully qualified, skipping retention check")
        return None
    if timestamp_field_fully_qualified.count(".") > 1:
        logger.info(
            "servicelevel.retention.timestampField contains multiple dots, which is currently not supported, skipping retention check"
        )
        return None

    model_name = timestamp_field_fully_qualified.split(".")[0]
    field_name = timestamp_field_fully_qualified.split(".")[1]
    period = data_contract_spec.servicelevels.retention.period
    period_in_seconds = period_to_seconds(period)
    if model_name not in data_contract_spec.models:
        logger.info(f"Model {model_name} not found in data_contract_spec.models, skipping retention check")
        return None
    check_type = "servicelevel_retention"
    check_key = "servicelevel_retention"
    sodacl_check_dict = {
        checks_for(model_name, QuotingConfig(), check_type): [
            {
                f"orders_servicelevel_retention < {period_in_seconds}": {
                    "orders_servicelevel_retention expression": f"TIMESTAMPDIFF(SECOND, MIN({field_name}), CURRENT_TIMESTAMP)",
                    "name": check_key,
                }
            },
        ]
    }
    return Check(
        id=str(uuid.uuid4()),
        key=check_key,
        category="servicelevel",
        type=check_type,
        name=f"Retention: Oldest entry has a max age of {period}",
        model=model_name,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(sodacl_check_dict),
    )


def period_to_seconds(period: str) -> int | None:
    import re

    # if period is None:
    #     return None
    # if period is in form "30d" or "24h" or "60m"
    if re.match(r"^\d+[dhm]$", period):
        if period[-1] == "d":
            return int(period[:-1]) * 86400
        if period[-1] == "h":
            return int(period[:-1]) * 3600
        if period[-1] == "m":
            return int(period[:-1]) * 60
    # if it is in iso period format (do not use isodate, can also be years)
    iso_period_regex = re.compile(
        r"P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<days>\d+)D)?"
        r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?"
    )
    match = iso_period_regex.match(period)
    if match:
        years = int(match.group("years") or 0)
        months = int(match.group("months") or 0)
        days = int(match.group("days") or 0)
        hours = int(match.group("hours") or 0)
        minutes = int(match.group("minutes") or 0)
        seconds = int(match.group("seconds") or 0)

        # Convert everything to seconds
        total_seconds = (
            years * 365 * 86400  # Approximate conversion of years to seconds
            + months * 30 * 86400  # Approximate conversion of months to seconds
            + days * 86400
            + hours * 3600
            + minutes * 60
            + seconds
        )
        return total_seconds

    return None


# These are deprecated root-level quality specifications, use the model-level and field-level quality fields instead
def to_quality_check(data_contract_spec) -> Check | None:
    if data_contract_spec.quality is None:
        return None
    if data_contract_spec.quality.type is None:
        return None
    if data_contract_spec.quality.type.lower() != "sodacl":
        return None
    if isinstance(data_contract_spec.quality.specification, str):
        quality_specification = yaml.safe_load(data_contract_spec.quality.specification)
    else:
        quality_specification = data_contract_spec.quality.specification

    return Check(
        id=str(uuid.uuid4()),
        key="quality__sodacl",
        category="quality",
        type="quality",
        name="Quality Check",
        model=None,
        engine="soda",
        language="sodacl",
        implementation=yaml.dump(quality_specification),
    )

## Borrowed from duckdb "normalize".
def NormalizeColumnName( col_name : str) -> str:
    ncol_name:      str = ''
    col_name_index: int = 0
    for item in col_name:
        if (item == '_' or (item >= '0' and item <= '9') or (item >= 'A' and item <= 'Z') or (item >= 'a' and item <= 'z')) :
            ncol_name = f"{ncol_name}{item}"
        elif item.isspace():
            ncol_name = f"{ncol_name} "

    col_name_trimmed: str = ncol_name.strip()
    col_name_cleaned: str = ""
    in_whitespace:   bool = False
    for character in col_name_trimmed:
        if character == ' ':
            if ( not in_whitespace ):
                col_name_cleaned = f"{col_name_cleaned}_"
                in_whitespace = True
        else:
            col_name_cleaned =  f"{col_name_cleaned}{character}"
            in_whitespace = False

    ##// don't leave string empty; if not empty, make lowercase
    if len(col_name_cleaned) == 0:
        col_name_cleaned = "_"
    else:
        col_name_cleaned = col_name_cleaned.lower()

    return col_name_cleaned

""" normalize_function from duckdb
static string NormalizeColumnName(const string &col_name) {
	// normalize UTF8 characters to NFKD
	auto nfkd = utf8proc_NFKD(reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str()),
	                          NumericCast<utf8proc_ssize_t>(col_name.size()));
	const string col_name_nfkd = string(const_char_ptr_cast(nfkd), strlen(const_char_ptr_cast(nfkd)));
	free(nfkd);

	// only keep ASCII characters 0-9 a-z A-Z and replace spaces with regular whitespace
	string col_name_ascii = "";
	for (idx_t i = 0; i < col_name_nfkd.size(); i++) {
		if (col_name_nfkd[i] == '_' || (col_name_nfkd[i] >= '0' && col_name_nfkd[i] <= '9') ||
		    (col_name_nfkd[i] >= 'A' && col_name_nfkd[i] <= 'Z') ||
		    (col_name_nfkd[i] >= 'a' && col_name_nfkd[i] <= 'z')) {
			col_name_ascii += col_name_nfkd[i];
		} else if (StringUtil::CharacterIsSpace(col_name_nfkd[i])) {
			col_name_ascii += " ";
		}
	}

	// trim whitespace and replace remaining whitespace by _
	string col_name_trimmed = TrimWhitespace(col_name_ascii);
	string col_name_cleaned = "";
	bool in_whitespace = false;
	for (idx_t i = 0; i < col_name_trimmed.size(); i++) {
		if (col_name_trimmed[i] == ' ') {
			if (!in_whitespace) {
				col_name_cleaned += "_";
				in_whitespace = true;
			}
		} else {
			col_name_cleaned += col_name_trimmed[i];
			in_whitespace = false;
		}
	}

	// don't leave string empty; if not empty, make lowercase
	if (col_name_cleaned.empty()) {
		col_name_cleaned = "_";
	} else {
		col_name_cleaned = StringUtil::Lower(col_name_cleaned);
	}

	// prepend _ if name starts with a digit or is a reserved keyword
	auto keyword = KeywordHelper::KeywordCategoryType(col_name_cleaned);

	if (NormalizeThis(keyword, col_name_cleaned) || (col_name_cleaned[0] >= '0' && col_name_cleaned[0] <= '9')) {
		col_name_cleaned = "_" + col_name_cleaned;
	}
	return col_name_cleaned;
}
"""
