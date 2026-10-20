"""End-to-end entry point.

    python main.py            # extract -> transform -> load -> analyse
    python main.py etl        # stop after the load
    python main.py analysis   # only re-run the analysis

With the default ``USE_LOCAL=true`` nothing external is needed: the lake is the
``lake/`` folder and the warehouse is ``warehouse/retail_sales.duckdb``.
"""
import sys

from log_config import setup_logger
from scripts import config, storage
from scripts.create_snowflake_resources import create_snowflake_resources
from scripts.data_analysis import main as analyze_data
from scripts.etl_process import etl_task

logger = setup_logger(__name__, str(config.LOGS_DIR / 'main.log'))


def run_pipeline(stage: str = 'all') -> None:
    logger.info(f'Runtime mode: {config.describe()}')

    if stage in ('all', 'etl', 'analysis'):
        # A brand-new lake needs the raw CSV before anything else can read it.
        storage.ensure_raw_data()

    if stage in ('all', 'etl'):
        create_snowflake_resources()
        etl_task()

    if stage in ('all', 'analysis'):
        analyze_data()

    logger.info('Pipeline finished.')


if __name__ == '__main__':
    stage = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if stage not in ('all', 'etl', 'analysis'):
        print(__doc__)
        raise SystemExit(f'Unknown stage: {stage}')
    run_pipeline(stage)
