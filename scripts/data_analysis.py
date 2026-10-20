"""Analysis: read the star schema, aggregate with SparkSQL and plot the results."""
import calendar
import os

import matplotlib

matplotlib.use('Agg')  # headless rendering, no GUI needed
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from log_config import setup_logger
from scripts import config, warehouse
from scripts.spark_utils import get_spark, stop_spark  # noqa: E402

logger = setup_logger(__name__, str(config.LOGS_DIR / 'data_analysis.log'))


def load_data(spark, conn):
    """Read sales_fact from the warehouse into a Spark DataFrame."""
    columns, rows = conn.query('SELECT * FROM sales_fact')
    columns = [c.upper() for c in columns]
    logger.info(f'Fetched {len(rows)} rows from sales_fact.')

    pdf = pd.DataFrame(rows, columns=columns)
    if pdf.empty:
        raise ValueError('sales_fact is empty - run the ETL step first.')
    pdf['DATE_KEY'] = pd.to_datetime(pdf['DATE_KEY'])

    df = spark.createDataFrame(pdf)
    logger.info(f'Successfully loaded data. Count: {df.count()}')
    df.printSchema()
    df.show(5, truncate=False)
    return df


def analyze_data(df):
    logger.info('Starting data analysis...')

    logger.info('Calculating total sales by product...')
    total_sales = (
        df.groupBy('PRODUCT_KEY')
          .agg(F.sum('TOTAL_REVENUE').alias('total_sales'))
          .orderBy(F.desc('total_sales'))
    )
    total_sales.show()

    logger.info('Calculating monthly sales trend...')
    monthly_sales = (
        df.groupBy(F.date_trunc('month', F.col('DATE_KEY').cast('timestamp')).alias('month'))
          .agg(F.sum('TOTAL_REVENUE').alias('total_sales'),
               F.countDistinct('DATE_KEY').alias('days_covered'))
          .orderBy('month')
    )
    monthly_sales.show()

    logger.info('Identifying top selling products...')
    top_products = (
        df.groupBy('PRODUCT_KEY')
          .agg(F.sum('TOTAL_UNITS_SOLD').alias('total_units'))
          .orderBy(F.desc('total_units'))
          .limit(5)
    )
    top_products.show()

    logger.info('Calculating average revenue per sale...')
    avg_revenue = df.agg(F.avg('TOTAL_REVENUE').alias('avg_revenue'))
    avg_revenue.show()

    return total_sales, monthly_sales, top_products, avg_revenue


def visualize_data(total_sales, monthly_sales, output_dir=None):
    logger.info('Creating visualizations...')
    vis_folder = str(output_dir or config.VISUALIZATIONS_DIR)
    os.makedirs(vis_folder, exist_ok=True)

    total_sales_data = total_sales.collect()
    plt.figure(figsize=(10, 6))
    plt.bar([row['PRODUCT_KEY'] for row in total_sales_data],
            [row['total_sales'] for row in total_sales_data],
            color='#3b6fd4')
    plt.title('Total Sales by Product')
    plt.xlabel('Product Key')
    plt.ylabel('Total Sales (revenue)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_folder, 'total_sales_by_product.png'), dpi=150)
    plt.close()

    monthly_sales_data = monthly_sales.collect()

    # The dataset starts mid-June 2010 and stops at the beginning of February 2023.
    # Plotting those stubs makes the line dive to zero, so flag the months whose
    # day coverage is clearly below a full month and leave them out of the chart
    # only (the aggregates themselves stay untouched).
    def coverage(row) -> float:
        days_in_month = calendar.monthrange(row['month'].year, row['month'].month)[1]
        return row['days_covered'] / days_in_month

    full_months = [row for row in monthly_sales_data if coverage(row) >= 0.9]
    partial = [row['month'].strftime('%Y-%m') for row in monthly_sales_data
               if coverage(row) < 0.9]
    if partial:
        logger.warning(f'Excluded partial months from the trend chart: {", ".join(partial)}')

    plt.figure(figsize=(12, 6))
    plt.plot([row['month'] for row in full_months],
             [row['total_sales'] for row in full_months],
             marker='o', markersize=3, color='#d4622b')
    plt.title('Monthly Sales Trend')
    plt.xlabel('Month')
    plt.ylabel('Total Sales (revenue)')
    plt.xticks(rotation=45)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_folder, 'monthly_sales_trend.png'), dpi=150)
    plt.close()

    logger.info(f"Visualizations saved in the '{vis_folder}' folder.")


def main():
    spark = None
    conn = None
    try:
        spark = get_spark('SalesDataAnalysis')
        conn = warehouse.connect()
        df = load_data(spark, conn)
        total_sales, monthly_sales, top_products, avg_revenue = analyze_data(df)
        visualize_data(total_sales, monthly_sales)
        logger.info('Data analysis completed successfully.')
        # Return nothing on purpose: the DAG uses this as a PythonOperator callable
        # and Spark DataFrames are not picklable for XCom.
    except Exception as e:
        logger.exception(f'Error in data analysis: {str(e)}')
        raise
    finally:
        if conn:
            conn.close()
            logger.info('Warehouse connection closed.')
        if spark:
            stop_spark(spark)
            logger.info('Spark session stopped.')


if __name__ == '__main__':
    main()
