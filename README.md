# snowflake-service

**Configure Environment Variables**

Copy the example environment file and fill in your specific details:
```bash
cp .env.example .env
```

Open the .env file in a text editor and replace the placeholder values with your actual Snowflake credentials and configuration.
Example .env file:

```ini
SNOWFLAKE_USER=your_username
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_PRIVATE_KEY_PATH=path/to/your/private_key.p8  # Optional
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase        # Optional
```