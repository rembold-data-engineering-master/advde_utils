"""
Utility library to offer a smoother experience when working with Airflow.
"""


from airflow.hooks.base import BaseHook
from urllib.parse import quote_plus
import pendulum
import requests
import boto3
from botocore.client import Config

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


def discord_success_alert(context):
    """Callback function to send a success notification to a proxy server, which
    will relay the signal to the correct group webhook."""
    payload = {
        "task_name": context['task_instance'].task_id,
        "dag_name": context['task_instance'].dag_id,
        "date": pendulum.instance(context['logical_date']).to_iso8601_string(),
    }
    resp = requests.post("http://sources.advde:8000/airflow/success", json=payload)

def discord_failure_alert(context):
    """Callback function to send a failure notification to a proxy server, which
    will relay the signal to the correct group webhook."""
    payload = {
        "task_name": context['task_instance'].task_id,
        "dag_name": context['task_instance'].dag_id,
        "date": pendulum.instance(context['logical_date']).to_iso8601_string(),
    }
    resp = requests.post("http://sources.advde:8000/airflow/failure", json=payload)


def get_minio_boto_client(conn_id):
    """Creates and returns a boto3 client connected to the given conn_id

    Args:
        conn_id (str): The Airflow connection ID for the MinIO server
    
    Returns:
        boto3.client: A boto3 client connect to the server and ready for use


    Examples:
    >>> client = get_minio_boto_client("minio_s3")
    >>> client.put_object(...)
    """
    conn = BaseHook.get_connection(conn_id)

    access_key = conn.login
    secret_key = conn.password

    extras = conn.extra_dejson
    endpoint = extras.get("endpoint", conn.host)
    if not endpoint.startswith('http://'):
        endpoint = 'http://' + endpoint
    region = extras.get("region_name", "us-east-1")

    return boto3.client(
        "s3",
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        region_name = region,
        endpoint_url = endpoint,
        config=Config(s3={'addressing_style': 'path'})
    )
