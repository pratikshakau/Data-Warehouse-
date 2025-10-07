from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
import pandas as pd

SNOWFLAKE_CONN_ID = "snowflake_conn"
API_KEY = Variable.get("alpha_vantage_api_key")
ST_SYMBOL = Variable.get("st_symbol")  

#Error handling for missing symbols
if not ST_SYMBOL:
    raise ValueError("Stock symbol is not set. Please add 'stock_symbol' in Airflow Variables.")


TABLE_NAME = "USER_DB_MACKEREL.RAW.STOCK_DATA"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG("stock_price_pipeline", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:


    @task()
    def create_table_if_not_exists():
        """Creates the Snowflake table if it doesn’t exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            symbol STRING,
            PRIMARY KEY (symbol, date)
        );
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.run(create_table_sql)

    from datetime import datetime, timedelta


    @task()
    def extract_stock_data():
        """Fetches last 90 trading days of stock price data from Alpha Vantage"""
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ST_SYMBOL}&apikey={API_KEY}&outputsize=compact"
        response = requests.get(url)
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            raise ValueError("Error fetching stock data. Check API response.")
               
        df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
        df.reset_index(inplace=True)
        df.rename(columns={
            "index": "date",
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        }, inplace=True)
        df["symbol"] = ST_SYMBOL  
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(by="date", ascending=False)
        df = df.head(90)

        print(f"Total trading days extracted: {len(df)}")
        
        df["date"] = df["date"].dt.strftime('%Y-%m-%d')
       
        records = list(df.itertuples(index=False, name=None))
        return records  


    @task()
    def full_refresh_snowflake(records):
        """Implements Full Refresh using SQL Transaction"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        if not records:
            raise ValueError("No records to insert into Snowflake")

        
        delete_sql = f"DELETE FROM {TABLE_NAME} WHERE symbol = '{ST_SYMBOL}'"
        insert_sql = f"""
            INSERT INTO {TABLE_NAME} (date, open, high, low, close, volume, symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                cur.execute(delete_sql)  
                cur.executemany(insert_sql, records)  
                cur.execute("COMMIT;")  

    # TASK DEPENDENCIES
    Table_creation = create_table_if_not_exists()
    stock_data_extraction = extract_stock_data()
    task_refresh = full_refresh_snowflake(stock_data_extraction)

    # ORDER
    Table_creation >> stock_data_extraction >> task_refresh  