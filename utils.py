"""
Utility library to offer a smoother experience when working with Airflow.
"""


from airflow.hooks.base import BaseHook
from urllib.parse import quote_plus

def get_postgres_uri_from_conn(conn_id: str) -> str:
    """
    Given an Airflow connection ID, return a PostgreSQL URI string
    
    Args:
        conn_id (str): The Airflow connection ID for the Postgres DB.
    
    Returns:
        str: A full PostgreSQL URI in the format postgresql://user:pass@host:port/db

    Examples:
    >>> uri = get_postgres_uri_from_conn('mypg')
    >>> con = duckdb.connect()
    >>> con.execute(f"ATTACH '{uri}' AS mypg (TYPE postgres);")
    >>> con.execute("SELECT * FROM table;")
    """
    conn = BaseHook.get_connection(conn_id)

    # Properly escape special characters in user/password
    user = quote_plus(conn.login or "")
    password = quote_plus(conn.password or "")
    host = conn.host or "localhost"
    port = conn.port or 5432
    database = conn.schema or ""

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

def gen_duckdb_s3_secret_from_conn(conn_id: str) -> str:
    """
    Given an Airflow connection ID, returns necessary secret string to
    be executed by DuckDB to allow it to copy or read from S3 servers

    Args: 
        conn_id (str): The Airflow connection ID for the S3 server

    Returns:
        str: The full text of the secret to be executed in a local DuckDB

    Examples:
    >>> s3_secret = gen_duckdb_s3_secret_from_conn('mys3')
    >>> con = duckdb.connect()
    >>> con.execute(s3_secret)
    >>> bucket_name = 'bob'
    >>> con.execute(f"COPY table TO 's3://{bucket_name}/mytable.parquet' (FORMAT PARQUET)")
    """
    conn = BaseHook.get_connection(conn_id)
    access_key = conn.login
    secret_key = conn.password
    extra = conn.extra_dejson
    endpoint = extra.get("endpoint", "http://localhost:9000")
    region = extra.get("region_name", "us-east-1")

    out=f"""
        CREATE OR REPLACE SECRET s3_secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{access_key}',
        SECRET '{secret_key}',
        ENDPOINT '{endpoint}',
        REGION '{region}',
        URL_STYLE 'path',
        USE_SSL false
        );
        """
    return out
