from pyspark.sql import SparkSession

from log_config import setup_logger
from scripts import config, storage
from scripts.spark_utils import get_spark

logger = setup_logger(__name__, str(config.LOGS_DIR / 'extract_data.log'))


def download_s3_file(file_key, download_path):
    """Pull the raw file out of the lake (S3 bucket or local folder)."""
    try:
        return storage.get_lake().download(file_key, download_path)
    except FileNotFoundError:
        # Empty lake: seed it from the CSV that ships with the repository.
        logger.warning(f'{file_key} missing from the lake - seeding it now')
        storage.ensure_raw_data()
        return storage.get_lake().download(file_key, download_path)


def extract_data(spark: SparkSession = None, file_key: str = None):
    """Extract: lake -> local CSV -> Spark DataFrame."""
    logger.info('Starting data extraction...')
    spark = spark or get_spark('RetailDataETL')
    file_key = file_key or config.RAW_FILE_KEY
    download_path = config.DATA_DIR / 'raw' / 'statsfinal.csv'

    try:
        download_s3_file(file_key, download_path)

        df = spark.read.csv(str(download_path), header=True, inferSchema=True)

        logger.info(
            f'Data extracted successfully from {file_key}. '
            f'Row count: {df.count()}, columns: {df.columns}'
        )
        return df
    except Exception as e:
        logger.exception(f'Error during data extraction: {str(e)}')
        raise
