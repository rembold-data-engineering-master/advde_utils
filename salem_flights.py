"""
DAG to acquire the data on the planes currently in the sky above Salem and write that
information to both a database and an S3 bucket.

Note that due to the nature of this API endpoint, this pipeline is not currently
fully idempotent, and that running it multiple times in succession _might_ result in
duplicate information being added to the database. In practice, because the API is
updated more often than this is polling the API, that would probably not happen, but
checks could be built into this to ensure that the rows to be added did not already
exist in the database before insertion.
"""

import requests
import pendulum
import pandas as pd
import duckdb

from airflow.sdk import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils import get_postgres_uri_from_conn, gen_duckdb_s3_secret_from_conn

@dag(
    start_date=pendulum.datetime(2025, 5, 11, tz="America/Los_Angeles"),
    catchup=False,
    schedule="*/15 * * * *",
    tags=["testing"]
)
def process_planes():
    
    @task
    def get_data():
        """Accessing the API and returning the data as JSON"""
        URL = "https://opensky-network.org/api/states/all?lamin=44.715514&lomin=-123.343506&lamax=45.160737&lomax=-122.744751"
        resp = requests.get(URL)
        resp.raise_for_status() #If there is a problem with the response, error out
        out = resp.json()
        print(out)
        return out

    @task
    def no_aircraft():
        """Sometimes the API returns no airplanes. This task just reports that."""
        print("There were no flights found at this time.")

    @task.branch(task_id="are_flights_returned")
    def are_flights_returned(data_dict):
        """Because the returned JSON does not always actually contain flights, this task
        checks the contents of the 'states' key and if nothing is in it, directs us to
        the no_aircraft task instead of the create_table task.
        """
        if data_dict['states']:
            return ['create_table']
        else:
            return ['no_aircraft']

    @task
    def create_table(data_dict):
        """Creates a Pandas dataframe from the flight JSON."""
        array_columns = [
                "icao", 'callsign', 'origin_country', 'time_position', 'last_contact',
                'longitude', 'latitude', 'geo_altitude', 'on_ground', 'velocity',
                'true_track', 'vertical_rate', 'sensors', 'baro_altitude', 'squawk',
                'spi', 'position_source'
                ]
        current = pd.DataFrame.from_dict(data_dict)
        current[array_columns] = pd.DataFrame(current.states.to_list(), index=current.index)
        current = current.drop('states', axis='columns')

        print(current.head())
        return current

    @task
    def clean_and_verify_table(table):
        """Cleans up the given Pandas dataframe by stripping out unnecessary whitespace."""
        table.callsign = table.callsign.str.strip()

        return table

    # Creates the flights table in the database if needed
    create_sql_table_task = SQLExecuteQueryOperator(
        task_id="create_flights_table_if_needed",
        conn_id="postgres_dataeng",
        sql="""
            CREATE TABLE IF NOT EXISTS flights_test (
                unix_time BIGINT,
                icao TEXT,
                callsign TEXT,
                origin_country TEXT,
                time_position BIGINT,
                last_contact BIGINT,
                longitude FLOAT,
                latitude FLOAT,
                geo_altitude FLOAT,
                on_ground BOOL,
                velocity FLOAT,
                true_track FLOAT,
                vertical_rate FLOAT,
                sensors TEXT,
                baro_altitude FLOAT,
                squawk TEXT,
                spi BOOL,
                position_source SMALLINT,
                PRIMARY KEY (unix_time, icao)
            );"""
    )

    @task
    def write_to_database(pd_table):
        """Takes the given Pandas dataframe and writes it to the database using DuckDB.
        This could also have been accomplished with SQLAlchemy and Panda's native
        capabilities.
        """
        uri = get_postgres_uri_from_conn("postgres_dataeng")
        print(uri)

        con = duckdb.connect()
        con.execute(f"ATTACH '{uri}' AS pgdb (TYPE postgres);")
        # DuckDB correctly can parse the pd_table in the SQL below
        con.execute("""
            INSERT INTO pgdb.flights_test
            SELECT *
            FROM pd_table;
            """)

    @task(trigger_rule="none_failed_min_one_success")
    def all_done():
        """Brings the split dag back together at a single endpoint. Checks simply that
        all dependencies didn't fail (skipping is ok) and at least one had success.
        """
        print('Success!')


    @task
    def write_parquet(pd_table):
        """ Writes the given Pandas table to an S3 instance."""
        s3_secret = gen_duckdb_s3_secret_from_conn('local_s3')
        con = duckdb.connect()
        con.execute(s3_secret)
        current_timestamp = pd_table.values[0][0]
        # Adding the Pandas dataframe (with DuckDB can work with natively) to the testing bucket
        con.execute(f"COPY pd_table TO 's3://testing/{current_timestamp}.parquet' (FORMAT PARQUET)")

            

    # Tasks that otherwise are not called (or are referenced multiple places)
    null_task = no_aircraft()
    all_done_task = all_done()
    new_data = get_data()
    tab = create_table(new_data)

    # Assigning the rest of the dependencies
    branch_choice = are_flights_returned(new_data)
    # Branching operators must point to multiple other operators
    branch_choice >> null_task
    branch_choice >> tab
    cleaned = clean_and_verify_table(tab)
    create_sql_table_task >> write_to_database(cleaned) >> write_parquet(cleaned) >> all_done_task
    null_task >> all_done_task


dag = process_planes()
