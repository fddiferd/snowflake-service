import os
import re
import snowflake.connector
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv
import hashlib

load_dotenv()  # Load environment variables from .env file


def get_private_key_from_file(
    private_key_path: str, private_key_passphrase: Optional[str] = None
):
    """
    Load the private key from a PEM file.

    Parameters:
    - private_key_path: Path to the private key file.
    - private_key_passphrase: Passphrase for the private key, if any.

    Returns:
    - The private key in DER (binary) format.
    """
    if not os.path.exists(private_key_path):
        print(f"Private key file not found at {private_key_path}")
        return None

    with open(private_key_path, "rb") as key_file:
        private_key_pem = key_file.read()

    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=private_key_passphrase.encode()
        if private_key_passphrase is not None
        else None,
        backend=default_backend(),
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection(
    user=None,
    account=None,
    role=None,
    warehouse=None,
    database=None,
    schema=None,
    private_key_path=None,
    private_key_passphrase=None,
):
    """
    Establish a connection to Snowflake using either private key or browser authentication.

    Parameters:
    - All parameters are optional and can be provided to override environment variables.

    Returns:
    - A Snowflake connection object.
    """
    user = user or os.getenv("SNOWFLAKE_USER")
    account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    role = role or os.getenv("SNOWFLAKE_ROLE")
    warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    database = database or os.getenv("SNOWFLAKE_DATABASE")
    schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
    private_key_path = private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    private_key_passphrase = (
        private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    )

    if private_key_path and os.path.exists(private_key_path):
        private_key = get_private_key_from_file(
            private_key_path, private_key_passphrase
        )

        print("Using private key to log in to Snowflake.")
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            private_key=private_key,
        )
    else:
        print("Using browser authentication, no private key found. Please log in to Snowflake.")
        conn = snowflake.connector.connect(
            user=user,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            authenticator="externalbrowser",
        )
    return conn


class SnowflakeUtils:
    """Utility class for connecting to Snowflake and executing queries."""

    def __init__(
        self,
        user=None,
        account=None,
        role=None,
        warehouse=None,
        database=None,
        schema=None,
        private_key_path=None,
        private_key_passphrase=None,
    ):
        self.conn = get_connection(
            user,
            account,
            role,
            warehouse,
            database,
            schema,
            private_key_path,
            private_key_passphrase,
        )

    def close(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()

    def fetch_data(
        self, query_input: str, variables: Dict = {}, cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch data from Snowflake, with optional caching and variable substitution.

        Parameters:
        - query_input: SQL query string or path to a .sql file.
        - variables: Dictionary of variables to substitute in the query.
        - cache: Whether to use cached results if available.

        Returns:
        - A Pandas DataFrame containing the query results.
        """
        print("Fetching data from Snowflake...")

        # Define cache directory
        cache_dir = Path("sql/caches")
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Determine if the input is a file or a query string
        is_file = query_input.lower().endswith(".sql")

        if is_file:
            # Prepend 'sql/' to query_input if it is a file and doesn't start with 'sql/'
            if not query_input.startswith("sql/"):
                query_input = "sql/" + query_input

            # Check if the file exists
            query_file = Path(query_input)
            if not query_file.exists():
                raise FileNotFoundError(f"File {query_input} not found")

            # Read the query from the file
            query = query_file.read_text()

            # Set cache filename based on the input filename
            cache_file = cache_dir / query_file.with_suffix(".parquet").name

        else:
            query = query_input.strip()
            is_query = query.lower().startswith(("select", "with"))
            if not is_query:
                raise ValueError(
                    "The input must be a filename ending with '.sql' or a SQL query starting with 'select' or 'with'"
                )
            # Set a unique cache filename based on the query hash
            query_hash = hashlib.md5(query.encode('utf-8')).hexdigest()
            cache_file = cache_dir / f"{query_hash}.parquet"

        # Use cached data if available
        if cache and cache_file.exists():
            return pd.read_parquet(cache_file)

        # Format the query with the provided variables
        query = self.format_variables(query)
        query = query.format(**variables)

        # Execute the query
        cursor = self.conn.cursor()
        try:
            print(f"Executing query:\n{query}")
            cursor.execute(query)
            df = self.convert_snowflake_response(cursor.fetch_pandas_all())
            # Save to cache
            df.to_parquet(cache_file, index=False)
            return df
        finally:
            cursor.close()

    def convert_snowflake_response(self, response: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the Snowflake response DataFrame by removing metadata columns and normalizing column names.

        Parameters:
        - response: The DataFrame returned from Snowflake.

        Returns:
        - Cleaned DataFrame.
        """
        # Remove metadata columns
        response = response.loc[:, ~response.columns.str.startswith("_")]
        # Normalize column names
        response.columns = [x.lower() for x in response.columns]
        return response

    def format_variables(self, query: str) -> str:
        """
        Replace SQL variables in the form $var with Python format placeholders.

        Parameters:
        - query: The SQL query string.

        Returns:
        - Formatted query string.
        """
        return re.sub(r"\$(\w+)", r"{\1}", query)

    def export_data(
        self,
        df: pd.DataFrame,
        database: str,
        schema: str,
        table: str,
        append: bool = True,
    ):
        """
        Export a Pandas DataFrame to a Snowflake table.

        Parameters:
        - df: DataFrame to export.
        - database: Target database in Snowflake.
        - schema: Target schema in Snowflake.
        - table: Target table in Snowflake.
        - append: Whether to append to the table if it exists.
        """
        if not append:
            self.drop_table(database, schema, table)

        if df.empty:
            print("DataFrame is empty. No data to export.")
            return

        # Convert column names to uppercase
        df.columns = [col.upper() for col in df.columns]

        # Set the database and schema
        self.conn.cursor().execute(f"USE DATABASE {database}")
        self.conn.cursor().execute(f"USE SCHEMA {schema}")

        cursor = self.conn.cursor()

        # Check if the table exists
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table.upper()}'
            """
        )
        table_exists = cursor.fetchone()[0]

        # Create table if it doesn't exist
        if table_exists == 0:
            column_defs = ", ".join(
                [
                    f"{col} {self.pandas_type_to_snowflake(df[col].dtype)}"
                    for col in df.columns
                ]
            )
            create_table_sql = f"""
            CREATE TABLE {schema}.{table} (
                {column_defs}
            )
            """
            print("Generated SQL for table creation:\n", create_table_sql)
            cursor.execute(create_table_sql)
            print(f"Table '{table}' created successfully.")
        else:
            print(f"Table '{table}' already exists.")

        print(f"Writing {len(df)} rows to Snowflake...")

        # Write the DataFrame to the Snowflake table
        success, _, nrows, _ = write_pandas(self.conn, df, table_name=table)

        if success:
            print(f"Successfully written {nrows} rows to Snowflake.")
        else:
            print("Failed to write DataFrame to Snowflake.")

    def pandas_type_to_snowflake(self, dtype) -> str:
        """
        Map Pandas data types to Snowflake data types.

        Parameters:
        - dtype: The data type of the Pandas column.

        Returns:
        - Corresponding Snowflake data type as a string.
        """
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMBER"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP_NTZ"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        else:
            return "STRING"

    def drop_table(self, database: str, schema: str, table: str):
        """
        Drop a table in Snowflake if it exists.

        Parameters:
        - database: Target database in Snowflake.
        - schema: Target schema in Snowflake.
        - table: Target table in Snowflake.
        """
        # Set the database and schema
        self.conn.cursor().execute(f"USE DATABASE {database}")
        self.conn.cursor().execute(f"USE SCHEMA {schema}")

        cursor = self.conn.cursor()
        # Check if the table exists
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table.upper()}'
            """
        )
        table_exists = cursor.fetchone()[0]

        if table_exists != 0:
            cursor.execute(f"DROP TABLE {schema}.{table}")
            print(f"Table '{table}' dropped.")
        else:
            print(f"Table '{table}' does not exist.")

    def execute_sql(self, file_path: str, variables: Dict = {}):
        """
        Execute SQL commands from a file, with optional variable substitution.

        Parameters:
        - file_path: Path to the .sql file.
        - variables: Dictionary of variables to substitute in the SQL.
        """
        # Read the SQL file
        with open(file_path, "r") as file:
            sql_commands = file.read()

        # Replace variables
        sql_commands = self.format_variables(sql_commands)
        sql_commands = sql_commands.format(**variables)

        # Split SQL commands
        sql_commands_list = sql_commands.strip().split(";")

        cursor = self.conn.cursor()
        # Execute each command
        for command in sql_commands_list:
            command = command.strip()
            if command:  # Only execute non-empty commands
                print(f"Executing SQL command:\n{command}")
                cursor.execute(command)
        cursor.close()