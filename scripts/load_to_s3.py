"""Load: write the cleaned, transformed rows into the data lake.

Despite the historical name, this step is storage-agnostic: with ``USE_LOCAL=true``
the lake is the local ``lake/processed_data/`` folder, otherwise it is the S3
bucket configured in ``scripts/config.py``.

De-duplication is content based: the MD5 of the CSV payload is stored as object
metadata, and a payload that already exists is not uploaded twice.
"""
import hashlib
from datetime import datetime
from io import StringIO

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType

from log_config import setup_logger
from scripts import config, storage
from scripts.transform_data import OUTPUT_COLUMNS

logger = setup_logger(__name__, str(config.LOGS_DIR / 'load_to_s3.log'))


def calculate_hash(csv_content: str) -> str:
    return hashlib.md5(csv_content.encode('utf-8')).hexdigest()


def clean_and_validate_data(df: DataFrame) -> DataFrame:
    """Cast to warehouse-friendly types and drop unusable rows.

    Input columns come from ``transform_data``: date, year, month, day,
    product_key, total_units_sold, total_revenue.
    """
    logger.info('Cleaning and validating transformed data...')

    df = df.select(*OUTPUT_COLUMNS)

    df = (
        df.withColumn('year', col('year').cast(IntegerType()))
          .withColumn('month', col('month').cast(IntegerType()))
          .withColumn('day', col('day').cast(IntegerType()))
          .withColumn('total_units_sold', col('total_units_sold').cast(IntegerType()))
          .withColumn('total_revenue', col('total_revenue').cast(DoubleType()))
    )

    before = df.count()
    # A row without a date or a product key cannot be joined to the dimensions,
    # and a row without a measure is useless - drop anything incomplete.
    df = df.dropna()
    after = df.count()
    if after != before:
        logger.warning(f'Dropped {before - after} incomplete rows')

    return df


def _to_csv_bytes(df: DataFrame) -> tuple[bytes, int]:
    """Render the DataFrame as CSV bytes, with a stable date format."""
    pdf = df.toPandas()
    pdf['date'] = pd.to_datetime(pdf['date']).dt.strftime('%Y-%m-%d')
    pdf = pdf[OUTPUT_COLUMNS].sort_values(['date', 'product_key']).reset_index(drop=True)

    buffer = StringIO()
    pdf.to_csv(buffer, index=False)
    return buffer.getvalue().encode('utf-8'), len(pdf)


def load_to_s3(df: DataFrame):
    """Upload the processed CSV to the lake. Returns the object key (or None)."""
    logger.info('Starting data load to the data lake...')
    try:
        cleaned_df = clean_and_validate_data(df)
        payload, record_count = _to_csv_bytes(cleaned_df)

        current_hash = calculate_hash(payload.decode('utf-8'))
        logger.info(f'Calculated hash for the cleaned CSV content: {current_hash}')

        existing = storage.find_by_hash(config.PROCESSED_PREFIX, current_hash)
        if existing:
            logger.info(f'CSV with hash {current_hash} already exists ({existing}). Skipping upload.')
            return None

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f'{config.PROCESSED_PREFIX}output_{timestamp}.csv'

        storage.get_lake().put_bytes(
            payload,
            key,
            {
                'processeddate': datetime.now().isoformat(timespec='seconds'),
                'recordcount': record_count,
                'pipelineversion': '1.0',
                'hash': current_hash,
            },
        )

        logger.info(f'Object {key} uploaded successfully ({record_count} rows).')
        return key

    except Exception as e:
        logger.exception(f'Error while uploading to the data lake: {str(e)}')
        raise
