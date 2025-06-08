"""
Every day the rides on the simulated Prague metro system are precomputed and added to
a queued table in a database. This DAG takes care of batch processing rides every 15
minutes, copying them from the queuing table and over into the official metro_source
database that students have access to. The same rides are then removed from the queuing
table, so that the DAG remains idempotent.
"""

from airflow.decorators import task, dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import duckdb
import pendulum

from utils import get_postgres_uri_from_conn


@dag(
    start_date=pendulum.datetime(2025, 6, 5, tz="America/Los_Angeles"),
    schedule=CronDataIntervalTimetable("*/15 * * * *", timezone=pendulum.timezone("America/Los_Angeles")),
    catchup=True,
    tags=["simulation"]
)
def batch_move_riders():

    # Ensures that the taps table exists in the student database
    taps_table_check_task = SQLExecuteQueryOperator(
            task_id = 'create_taps_table_if_neededed',
            conn_id = 'metro_db',
            sql = """
                  CREATE TABLE IF NOT EXISTS taps (
                    pid INT REFERENCES passengers,
                    station_id BIGINT REFERENCES stations,
                    tap_dt TIMESTAMPTZ
                    );
                  """
            )
    
    @task
    def copy_riders_over(data_interval_start=None, data_interval_end=None):
        """Copies rides in the interval from the queuing table to the student taps
        table.
        """
        print(f"Interval starts at: {data_interval_start}")
        print(f"Interval ends at : {data_interval_end}")
        if not data_interval_start or not data_interval_end:
            return
        uri_from = get_postgres_uri_from_conn('metro_control')
        uri_to = get_postgres_uri_from_conn('metro_db')
        db = duckdb.connect()
        db.execute(f"ATTACH '{uri_from}' AS pg_control (TYPE postgres)")
        db.execute(f"ATTACH '{uri_to}' AS pg_student (TYPE postgres)")
        data = db.sql(f"""
                          SELECT passenger_id, station_id, tap_dt
                          FROM pg_control.passenger_queue
                          WHERE tap_dt BETWEEN 
                            '{data_interval_start}' AND '{data_interval_end}'
                          """)
        data.show() #Just to visually see the added rows in the logs
        db.execute(f"""
                        INSERT INTO pg_student.taps
                          SELECT passenger_id, station_id, tap_dt
                          FROM pg_control.passenger_queue
                          WHERE tap_dt BETWEEN 
                            '{data_interval_start}' AND '{data_interval_end}'
                          """)
    @task
    def cleanup_queue(data_interval_start=None, data_interval_end=None):
        """Removes entries from the queuing table within ith given interval. Used to
        ensure that reruns of the DAG will not add multiple cases of the same ride to
        the student taps table.
        """
        if not data_interval_start or not data_interval_end:
            return
        uri_from = get_postgres_uri_from_conn('metro_control')
        db = duckdb.connect()
        db.execute(f"ATTACH '{uri_from}' AS pg_control (TYPE postgres)")
        db.execute(f"""
                   DELETE FROM pg_control.passenger_queue
                   WHERE tap_dt BETWEEN 
                     '{data_interval_start}' AND '{data_interval_end}'
                   """)

    taps_table_check_task >> copy_riders_over() >> cleanup_queue()


dag = batch_move_riders()
