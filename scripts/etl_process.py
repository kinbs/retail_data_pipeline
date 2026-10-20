from log_config import setup_logger
from scripts import config
from scripts.extract_data import extract_data
from scripts.load_to_s3 import load_to_s3
from scripts.load_to_snowflake import load_to_snowflake
from scripts.spark_utils import get_spark, stop_spark
from scripts.transform_data import transform_data

logger = setup_logger(__name__, str(config.LOGS_DIR / 'etl_process.log'))


def etl_task(spark=None):
    """Extract -> Transform -> Load (lake) -> Load (warehouse)."""
    own_spark = spark is None
    spark = spark or get_spark('RetailDataETL')
    try:
        logger.info('Starting ETL process...')

        logger.info('Extracting data...')
        df = extract_data(spark)

        logger.info('Transforming data...')
        df_transformed = transform_data(df)

        logger.info('Loading data to the data lake...')
        lake_key = load_to_s3(df_transformed)

        if lake_key is None:
            # Same payload already in the lake: fall back to the newest processed file
            # so the warehouse load stays idempotent instead of failing.
            from scripts.load_to_snowflake import get_latest_s3_filename

            lake_key = get_latest_s3_filename()
            if lake_key is None:
                raise RuntimeError('No processed file available in the lake after the load step.')
            logger.info(f'Payload unchanged; reusing latest processed file {lake_key}')

        logger.info(f"Loading data from lake file '{lake_key}' to the warehouse...")
        load_to_snowflake(lake_key)

        logger.info('ETL process completed successfully.')
        return lake_key
    except Exception as e:
        logger.exception(f'Error in ETL process: {str(e)}')
        raise
    finally:
        if own_spark:
            stop_spark(spark)


if __name__ == '__main__':
    etl_task()
