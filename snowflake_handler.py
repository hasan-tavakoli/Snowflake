import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeHandler:
    def __init__(self, username, password, account, warehouse, role):
        self.username = username
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.role = role
        self.conn = None

    def connect(self):
        try:
            self.conn = sf.connect(
                user=self.username,
                password=self.password,
                account=self.account,
                role=self.role,
            )
            logger.info("Connection successful!")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            exit()

    def setup_database(self, database, schema):
        try:
            cur = self.conn.cursor()
            cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {self.warehouse}")
            cur.execute(f"USE WAREHOUSE {self.warehouse}")
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute("SELECT CURRENT_DATABASE()")
            current_database = cur.fetchone()[0]

            if current_database == database.upper():
                logger.info("Connection has the correct database set.")
            else:
                logger.error("Connection does not have the correct database set.")
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            exit()

    def insert_dataframe(self, df, table_name, database, schema):
        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=self.conn,
                df=df,
                table_name=table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
            )
            if success:
                logger.info(
                    f"DataFrame inserted successfully! Number of rows inserted: {nrows}"
                )
            else:
                logger.error("Failed to insert DataFrame.")
        except Exception as e:
            logger.error(f"Failed to insert DataFrame: {e}")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")
