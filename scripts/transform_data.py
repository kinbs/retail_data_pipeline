"""Transform: one wide row per day -> one row per (day, product).

Source columns: ``Date, Q-P1..Q-P4, S-P1..S-P4`` (units sold and revenue for
four products). Output columns, in the exact order the warehouse expects:

    date, year, month, day, product_key, total_units_sold, total_revenue
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofmonth, month, try_to_date, year

from log_config import setup_logger
from scripts import config

logger = setup_logger(__name__, str(config.LOGS_DIR / 'transform_data.log'))

PRODUCTS = ('P1', 'P2', 'P3', 'P4')
OUTPUT_COLUMNS = ['date', 'year', 'month', 'day', 'product_key', 'total_units_sold', 'total_revenue']


def transform_data(df: DataFrame) -> DataFrame:
    logger.info('Starting data transformation...')
    try:
        logger.info(f'Initial DataFrame: {df.count()} rows, {len(df.columns)} columns')

        # 'dd-MM-yyyy' -> DateType (e.g. 13-06-2010).
        # A handful of rows use a single-digit month ('31-9-2010'); 'd-M-yyyy'
        # accepts both widths, and try_to_date() returns NULL instead of raising
        # when the calendar date itself does not exist (September has 30 days).
        df = df.withColumn('Date', try_to_date(col('Date'), 'd-M-yyyy'))

        invalid = df.filter(col('Date').isNull()).count()
        if invalid:
            logger.warning(
                f'Dropped {invalid} rows with an unparseable/impossible date '
                f"(e.g. '31-9-2010') - a known defect of the source CSV"
            )
            df = df.filter(col('Date').isNotNull())

        df = df.withColumn('year', year(col('Date'))) \
               .withColumn('month', month(col('Date'))) \
               .withColumn('day', dayofmonth(col('Date')))

        # Unpivot the four product columns into rows.
        frames = [
            df.selectExpr(
                'Date as date', 'year', 'month', 'day',
                f"'{product}' as product_key",
                f'`Q-{product}` as total_units_sold',
                f'`S-{product}` as total_revenue',
            )
            for product in PRODUCTS
        ]

        sales_data = frames[0]
        for frame in frames[1:]:
            sales_data = sales_data.unionByName(frame)

        sales_data = sales_data.select(*OUTPUT_COLUMNS)

        logger.info(
            f'Transformed DataFrame: {sales_data.count()} rows, {len(sales_data.columns)} columns'
        )
        logger.info('Data transformation completed successfully')
        return sales_data

    except Exception as e:
        logger.exception(f'Error during data transformation: {str(e)}')
        raise
