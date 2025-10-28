from datetime import datetime
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='ELT_USER_DB_MACKEREL',
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    description='ETL using SnowflakeHook to create tables, stage, and load data into USER_DB_MACKEREL',
    tags=['etl', 'snowflake'],
) as dag:

    @task
    def setup_tables():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("USE WAREHOUSE MACKEREL_QUERY_WH;")
            cur.execute("USE DATABASE USER_DB_MACKEREL;")
            cur.execute("USE SCHEMA RAW;")

            cur.execute("BEGIN;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS USER_DB_MACKEREL.RAW.USER_SESSION_CHANNEL (
                    userId INT NOT NULL,
                    sessionId VARCHAR(32) PRIMARY KEY,
                    channel VARCHAR(32) DEFAULT 'direct'
                );
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS USER_DB_MACKEREL.RAW.SESSION_TIMESTAMP (
                    sessionId VARCHAR(32) PRIMARY KEY,
                    ts TIMESTAMP
                );
            """)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f"Setup failed: {str(e)}")
            raise

    @task
    def extract_stage():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("USE WAREHOUSE MACKEREL_QUERY_WH;")
            cur.execute("USE DATABASE USER_DB_MACKEREL;")
            cur.execute("USE SCHEMA RAW;")

            cur.execute("BEGIN;")
            cur.execute("""
                CREATE OR REPLACE STAGE USER_DB_MACKEREL.RAW.BLOB_STAGE
                URL = 's3://s3-geospatial/readonly/'
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f"Stage creation failed: {str(e)}")
            raise

    @task
    def load_data():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("USE WAREHOUSE MACKEREL_QUERY_WH;")
            cur.execute("USE DATABASE USER_DB_MACKEREL;")
            cur.execute("USE SCHEMA RAW;")

            cur.execute("BEGIN;")
            cur.execute("""
                COPY INTO USER_DB_MACKEREL.RAW.USER_SESSION_CHANNEL
                FROM @USER_DB_MACKEREL.RAW.BLOB_STAGE/user_session_channel.csv;
            """)
            cur.execute("""
                COPY INTO USER_DB_MACKEREL.RAW.SESSION_TIMESTAMP
                FROM @USER_DB_MACKEREL.RAW.BLOB_STAGE/session_timestamp.csv;
            """)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f"Data load failed: {str(e)}")
            raise

    setup = setup_tables()
    extract = extract_stage()
    load = load_data()

    setup >> extract >> load
