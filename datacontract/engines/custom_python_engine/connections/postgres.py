"""_summary_
    Postgres connection, handled by duckdb-postgres interface.
"""


import os

import duckdb
from open_data_contract_standard.model import Server


def connect_to_postgres_via_duckdb(server:Server|None):
    if server is None:
        raise ValueError("parameter server can't be None (null), bailout with Exception.")
    con = duckdb.connect()
    # Note to Devs: hostaddr requires somethingy like an IP, host can take a name.
    con.sql(f"""
        INSTALL postgres;
        LOAD postgres;
        CREATE SECRET (
            TYPE postgres,
            HOST '{server.host}',
            PORT {str(server.port)},
            DATABASE {server.database},
            USER '{os.getenv("DATACONTRACT_POSTGRES_USERNAME")}',
            PASSWORD '{os.getenv("DATACONTRACT_POSTGRES_PASSWORD")}'
        );
        ATTACH 'dbname={server.database}' AS {server.database}_db (TYPE postgres, READ_ONLY);
        """)
    return con
