from log_config import setup_logger
from scripts import config, warehouse

logger = setup_logger(__name__, str(config.LOGS_DIR / 'create_snowflake_resources.log'))


def create_snowflake_resources():
    """Create warehouse, database, schema and the star-schema tables.

    Locally this runs against DuckDB; in the cloud against Snowflake. Only the
    database/schema/warehouse DDL differs, the table DDL is shared.
    """
    logger.info('Starting creation of warehouse resources...')
    try:
        if config.USE_LOCAL:
            conn = warehouse.connect()
            cursor = conn
        else:
            import snowflake.connector as sc

            conn = sc.connect(
                user=config.SNOWFLAKE_USER,
                password=config.SNOWFLAKE_PASSWORD,
                account=config.SNOWFLAKE_ACCOUNT,
            )
            cursor = conn.cursor()

            logger.info(f'Creating warehouse: {config.SNOWFLAKE_WAREHOUSE}')
            cursor.execute(
                f"CREATE WAREHOUSE IF NOT EXISTS {config.SNOWFLAKE_WAREHOUSE} "
                "WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE"
            )

            logger.info(f'Creating database: {config.SNOWFLAKE_DATABASE}')
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS {config.SNOWFLAKE_DATABASE}')
            cursor.execute(f'USE DATABASE {config.SNOWFLAKE_DATABASE}')

            logger.info(f'Creating schema: {config.SNOWFLAKE_SCHEMA}')
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {config.SNOWFLAKE_SCHEMA}')
            cursor.execute(f'USE SCHEMA {config.SNOWFLAKE_SCHEMA}')

        logger.info('Creating date_dim table')
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_dim (
            date_key DATE PRIMARY KEY,
            year INT,
            month INT,
            day INT
        )
        """)

        logger.info('Creating product_dim table')
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_dim (
            product_key VARCHAR PRIMARY KEY,
            product_name VARCHAR
        )
        """)

        logger.info('Creating sales_fact table')
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_fact (
            date_key DATE,
            product_key VARCHAR,
            total_units_sold INT,
            total_revenue DOUBLE,
            PRIMARY KEY (date_key, product_key),
            FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
            FOREIGN KEY (product_key) REFERENCES product_dim(product_key)
        )
        """)

        logger.info('Creating processed_files table')
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP
        )
        """)

        if config.USE_LOCAL:
            conn.commit()
            conn.close()
        else:
            conn.close()

        logger.info('Warehouse resources created successfully.')

    except Exception as e:
        logger.exception(f'Error creating warehouse resources: {str(e)}')
        raise
