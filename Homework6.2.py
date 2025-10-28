from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

# 🎯 Airflow default args
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="ELT_session_summary_dag",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # every day at 3 AM
    catchup=False,
    description=" ELT DAG: Join user_session_channel & session_timestamp to build session_summary!",
    tags=["elt", "snowflake"],
) as dag:

    @task
    def join_raw_tables():
        """
        🧩 Step 1: Join two RAW tables into analytics.session_summary
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            logging.info("✨ Starting ELT fun join process...")

            # Context setup
            cur.execute("USE WAREHOUSE MACKEREL_QUERY_WH;")
            cur.execute("USE DATABASE USER_DB_MACKEREL;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS;")
            cur.execute("USE SCHEMA ANALYTICS;")

            cur.execute("BEGIN;")

            # Create or replace session_summary with join result
            cur.execute("""
                CREATE OR REPLACE TABLE USER_DB_MACKEREL.ANALYTICS.SESSION_SUMMARY AS
                SELECT 
                    usc.userId,
                    usc.sessionId,
                    usc.channel,
                    st.ts
                FROM USER_DB_MACKEREL.RAW.USER_SESSION_CHANNEL usc
                JOIN USER_DB_MACKEREL.RAW.SESSION_TIMESTAMP st
                  ON usc.sessionId = st.sessionId;
            """)

            cur.execute("COMMIT;")
            logging.info("✅ session_summary successfully created!")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f" ELT join failed: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    @task
    def remove_duplicates():
        """
        🧹 Step 2: Remove duplicate sessionIds (keep the latest ts)
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            logging.info("Checking for duplicates in session_summary...")
            cur.execute("USE WAREHOUSE MACKEREL_QUERY_WH;")
            cur.execute("USE DATABASE USER_DB_MACKEREL;")
            cur.execute("USE SCHEMA ANALYTICS;")

            cur.execute("BEGIN;")

            # Delete duplicates keeping the most recent ts
            cur.execute("""
                DELETE FROM USER_DB_MACKEREL.ANALYTICS.SESSION_SUMMARY
                WHERE (sessionId, ts) IN (
                    SELECT sessionId, ts FROM (
                        SELECT 
                            sessionId,
                            ts,
                            ROW_NUMBER() OVER (
                                PARTITION BY sessionId ORDER BY ts DESC
                            ) AS rn
                        FROM USER_DB_MACKEREL.ANALYTICS.SESSION_SUMMARY
                    )
                    WHERE rn > 1
                );
            """)

            cur.execute("COMMIT;")
            logging.info("🎉 Duplicate cleanup complete — all sessions are unique now!")
        except Exception as e:
            cur.execute("ROLLBACK;")
            logging.error(f" Duplicate cleanup failed: {e}")
            raise
        finally:
            cur.close()
            conn.close()


    join_raw_tables() >> remove_duplicates()
