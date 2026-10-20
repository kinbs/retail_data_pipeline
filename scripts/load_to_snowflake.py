"""Load: processed CSV in the lake -> warehouse (DuckDB locally, Snowflake in the cloud)."""
from datetime import datetime
from pathlib import Path

import pandas as pd

from log_config import setup_logger
from scripts import config, storage
from scripts.create_snowflake_resources import create_snowflake_resources

logger = setup_logger(__name__, str(config.LOGS_DIR / 'load_to_snowflake.log'))

CSV_COLUMNS = ['date', 'year', 'month', 'day', 'product_key', 'total_units_sold', 'total_revenue']


def get_latest_s3_filename():
    return storage.get_lake().latest(config.PROCESSED_PREFIX)


# --------------------------------------------------------------------------- #
# Local warehouse (DuckDB)
# --------------------------------------------------------------------------- #
def _load_local(processed_key: str) -> None:
    from scripts import warehouse

    conn = warehouse.connect()
    try:
        conn.execute("""
            SELECT filename FROM processed_files WHERE filename = ?
        """, (processed_key,))
        if conn.query('SELECT filename FROM processed_files WHERE filename = ?',
                      (processed_key,))[1]:
            logger.info(f'File {processed_key} already processed. Skipping.')
            return

        local_copy = config.DATA_DIR / 'processed' / Path(processed_key).name
        storage.get_lake().download(processed_key, local_copy)

        pdf = pd.read_csv(local_copy, dtype={'date': str, 'product_key': str})
        missing = [c for c in CSV_COLUMNS if c not in pdf.columns]
        if missing:
            raise ValueError(f'Processed file is missing columns: {missing}')

        pdf['year'] = pdf['year'].astype('int64')
        pdf['month'] = pdf['month'].astype('int64')
        pdf['day'] = pdf['day'].astype('int64')
        pdf['total_units_sold'] = pdf['total_units_sold'].astype('int64')
        pdf['total_revenue'] = pdf['total_revenue'].astype('float64')
        conn.register_frame('temp_sales', pdf[CSV_COLUMNS])

        conn.execute('BEGIN TRANSACTION')
        try:
            # date_dim
            conn.execute("""
                INSERT OR REPLACE INTO date_dim (date_key, year, month, day)
                SELECT DISTINCT CAST(date AS DATE), CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)
                FROM temp_sales
            """)
            # product_dim
            conn.execute("""
                INSERT OR REPLACE INTO product_dim (product_key, product_name)
                SELECT DISTINCT product_key,
                       'Product ' || SUBSTR(product_key, 2, 1)
                FROM temp_sales
            """)
            # sales_fact
            conn.execute("""
                INSERT OR REPLACE INTO sales_fact
                    (date_key, product_key, total_units_sold, total_revenue)
                SELECT CAST(date AS DATE), product_key,
                       CAST(total_units_sold AS INT), CAST(total_revenue AS DOUBLE)
                FROM temp_sales
            """)
            # processed_files
            conn.execute(
                'INSERT OR REPLACE INTO processed_files (filename, processed_at) VALUES (?, ?)',
                (processed_key, datetime.now()),
            )
            conn.execute('COMMIT')
        except Exception:
            conn.execute('ROLLBACK')
            raise

        logger.info(f'Data from {processed_key} loaded successfully into the local warehouse.')
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Cloud warehouse (Snowflake, via COPY INTO from S3)
# --------------------------------------------------------------------------- #
def _load_snowflake(processed_key: str) -> None:
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
    )
    cursor = conn.cursor()

    cursor.execute('SELECT filename FROM processed_files WHERE filename = %s', (processed_key,))
    if cursor.fetchone():
        logger.info(f'File {processed_key} already processed. Skipping.')
        conn.close()
        return

    stage_name = f"temp_stage_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    cursor.execute(f'CREATE OR REPLACE TEMPORARY STAGE {stage_name}')
    cursor.execute("""
    CREATE OR REPLACE TEMPORARY TABLE temp_sales (
        date_str VARCHAR,
        year FLOAT,
        month FLOAT,
        day FLOAT,
        product_key VARCHAR,
        total_units_sold FLOAT,
        total_revenue FLOAT
    )
    """)

    cursor.execute(f"""
    COPY INTO temp_sales
    FROM 's3://{config.AWS_S3_BUCKET_NAME}/{processed_key}'
    CREDENTIALS = (AWS_KEY_ID='{config.AWS_ACCESS_KEY}' AWS_SECRET_KEY='{config.AWS_SECRET_KEY}')
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    """)

    cursor.execute("""
    MERGE INTO date_dim AS target
    USING (
        SELECT DISTINCT TO_DATE(date_str, 'YYYY-MM-DD') AS date_key,
               CAST(year AS INT) AS year,
               CAST(month AS INT) AS month,
               CAST(day AS INT) AS day
        FROM temp_sales
    ) AS source
    ON target.date_key = source.date_key
    WHEN MATCHED THEN UPDATE SET
        year = source.year, month = source.month, day = source.day
    WHEN NOT MATCHED THEN INSERT (date_key, year, month, day)
    VALUES (source.date_key, source.year, source.month, source.day)
    """)

    cursor.execute("""
    MERGE INTO product_dim AS target
    USING (
        SELECT DISTINCT product_key, CONCAT('Product ', SUBSTR(product_key, 2, 1)) AS product_name
        FROM temp_sales
    ) AS source
    ON target.product_key = source.product_key
    WHEN MATCHED THEN UPDATE SET product_name = source.product_name
    WHEN NOT MATCHED THEN INSERT (product_key, product_name)
    VALUES (source.product_key, source.product_name)
    """)

    cursor.execute("""
    MERGE INTO sales_fact AS target
    USING (
        SELECT TO_DATE(date_str, 'YYYY-MM-DD') AS date_key,
               product_key,
               CAST(total_units_sold AS INT) AS total_units_sold,
               total_revenue
        FROM temp_sales
    ) AS source
    ON target.date_key = source.date_key AND target.product_key = source.product_key
    WHEN MATCHED THEN UPDATE SET
        total_units_sold = source.total_units_sold,
        total_revenue = source.total_revenue
    WHEN NOT MATCHED THEN INSERT (date_key, product_key, total_units_sold, total_revenue)
    VALUES (source.date_key, source.product_key, source.total_units_sold, source.total_revenue)
    """)

    cursor.execute(
        'INSERT INTO processed_files (filename, processed_at) VALUES (%s, CURRENT_TIMESTAMP())',
        (processed_key,),
    )

    conn.commit()
    conn.close()
    logger.info(f'Data from {processed_key} loaded successfully into Snowflake.')


def load_to_snowflake(s3_filename: str | None = None) -> None:
    if not s3_filename:
        s3_filename = get_latest_s3_filename()
    if not s3_filename:
        logger.error('No processed file available in the lake - nothing to load.')
        return

    logger.info(f'Starting data load to the warehouse from {s3_filename}')
    try:
        create_snowflake_resources()
        if config.USE_LOCAL:
            _load_local(s3_filename)
        else:
            _load_snowflake(s3_filename)
    except Exception as e:
        logger.exception(f'Error during warehouse load: {str(e)}')
        raise


if __name__ == '__main__':
    load_to_snowflake(get_latest_s3_filename())
