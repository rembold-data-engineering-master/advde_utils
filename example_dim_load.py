
from airflow.decorators import task, dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable

import duckdb
import pandas as pd
import pendulum

from utils import get_postgres_uri_from_conn, gen_duckdb_s3_secret_from_conn

@dag(
    start_date = pendulum.datetime(2025, 6, 5, tz='America/Los_Angeles'),
    schedule = None,
    catchup = False,
    tags = ['student_examples']
)
def example_dim_load(desired_file: str = 'people_v1', debug: bool = False):
    """
    Example DAG meant to illustrate how to:
        - Utilize DAG query parameters
        - Read data from a parquet file in S3 storage
        - Populate a dimension table with Type-2 SCD

    Note that this is not demonstrating creating/updating a corresponding
    fact table, but creating/updating the fact table is usually simple once
    you have updated the dimension tables. Generally you can just join your desired new
    facts to the dimension tables via whatever constraints (e.g. name, date, time, etc. NOT the
    surrogate keys at this point, you are joining to match your fact rows UP to the correct surrogate keys),
    and then just select out the surrogate keys and facts to create your rows to add to your fact table.

    If you have a dimension table that just has Type-1 SCD values, you really just need to tweak the update section.
    (Dimension tables with Type-1 SCD don't really handle deletions great unless you still add an expiry column.)

    Usage:
        There are three parquet files (people_v1, people_v2, people_v3) that you should upload to your S3 storage
        for this to work. You may need to also adjust the warehouse and s3 connection names as appropriate to your
        situation below. If you then manually trigger this DAG, you'll see places where you can type the desired
        filename to read (without the .parquet suffix) and toggle on debugging for nicer printing. You should be
        able to cycle through running the DAG with v1, v2, v3 (and back to v1 if you want) to see the desired
        effects.
    """

    # Creating our dimension table if we need it
    create_dim_table_if_needed = SQLExecuteQueryOperator(
        task_id = 'create_dim_table_if_needed',
        conn_id = 'warehouse',
        sql = """
              CREATE TABLE IF NOT EXISTS dim_people (
                  people_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, --SERIAL could also work here
                  pid INT,
                  name TEXT,
                  address TEXT,
                  effective_dt TIMESTAMPTZ DEFAULT now(),
                  expired_dt TIMESTAMPTZ,
                  currently_active BOOL DEFAULT TRUE
              );"""
    )

    @task
    def update_dim(**context):
        """Handles reading in the data, computing what needs to change and how in the dimension table, and
        then making the necessary changes.

        This could be broken up into subfunctions for cleaner coding, but I would probably NOT break it into
        separate tasks. Mainly because of the added difficulty in passing around the duckdb connection from
        task to task.
        """
        # Connection info and setup
        warehouse_uri = get_postgres_uri_from_conn('warehouse') # My connection info, update to yours
        s3_secret = gen_duckdb_s3_secret_from_conn('minio_s3') # My connection info, update to yours
        db = duckdb.connect()
        db.execute(f"ATTACH '{warehouse_uri}' AS wh (TYPE postgres)")
        db.execute(s3_secret)
        bucket = Variable.get('s3_bucket_name') # My connection info, update to yours
        #reading in the DAG parameters
        target_file = context['params']['desired_file'] 
        debug_mode = context['params']['debug']

        # Getting the new data from the lake
        new_from_lake = db.sql(
            f"""
            SELECT *
            FROM 's3://{bucket}/{target_file}.parquet'
            """) #if you need multiple parquet files, you can comma separate them (assuming the same schema)
        if debug_mode:
            print('New from data lake'.center(50, '='))
            new_from_lake.show()

        # Comparing data to warehouse dimension table
        # Here we use EXCEPT in both directions to get the symmetric difference between our current and old table
        added_and_modified_rows = db.sql(
            """
            SELECT * from new_from_lake --the new
            EXCEPT
            SELECT pid, name, address FROM wh.dim_people WHERE currently_active=TRUE --the existing, minus the key
            """)
        removed_and_modified_rows = db.sql(
            """
            SELECT pid, name, address FROM wh.dim_people WHERE currently_active=TRUE--the existing, minus the key
            EXCEPT
            SELECT * from new_from_lake --the new
            """)
        
        # Determining Insert/Update/Remove
        # Further breaking down the above to get just specific pieces
        # Draw yourself pictures if you are having trouble understanding the set operations here
        new_to_insert = db.sql(
            """
            SELECT * FROM added_and_modified_rows
            WHERE pid NOT IN (SELECT pid FROM wh.dim_people WHERE currently_active=TRUE)
            """)
        new_to_update = db.sql(
            """
            SELECT * FROM added_and_modified_rows
            WHERE pid IN (SELECT pid FROM wh.dim_people WHERE currently_active=TRUE)
            """)
        old_to_update = db.sql(
            """
            SELECT * FROM removed_and_modified_rows
            WHERE pid IN (SELECT pid FROM added_and_modified_rows)
            """)
        old_to_remove = db.sql(
            """
            SELECT * FROM removed_and_modified_rows
            WHERE pid NOT IN (SELECT pid FROM added_and_modified_rows)
            """)
        if debug_mode:
            print('Added new rows to be inserted'.center(50, '='))
            new_to_insert.show()
            print('Updated new rows to be inserted'.center(50, '='))
            new_to_update.show()
            print('Updated old rows to be expired'.center(50, '='))
            old_to_update.show()
            print('Removed old rows to be expired'.center(50, '='))
            old_to_remove.show()


        # Updating the warehouse: use the above to actually make the changes
        # Insertions (easy peasy)
        db.execute(
            """
            INSERT INTO wh.dim_people (pid, name, address)
                SELECT * FROM new_to_insert;
            """)
        # Updates (two steps!)
        db.execute(
            """
            -- Add new entries
            INSERT INTO wh.dim_people (pid, name, address)
                SELECT * FROM new_to_update;
            
            -- Update old entries
            UPDATE wh.dim_people
            SET (expired_dt, currently_active) = (now(), FALSE)
            FROM old_to_update AS old
            WHERE wh.dim_people.pid = old.pid
              AND wh.dim_people.name = old.name
              AND wh.dim_people.address = old.address;
            """)
        # Removals (not actually deleting, just expiring)
        db.execute(
            """
            UPDATE wh.dim_people
            SET (expired_dt, currently_active) = (now(), FALSE)
            FROM old_to_remove AS old
            WHERE wh.dim_people.pid = old.pid
              AND wh.dim_people.name = old.name
              AND wh.dim_people.address = old.address;
            """)


    create_dim_table_if_needed >> update_dim()

dag = example_dim_load()




