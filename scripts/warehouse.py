"""Warehouse access layer: DuckDB locally, Snowflake in the cloud.

Both back-ends expose the same tiny surface used by the pipeline:

    execute(sql, params=None)
    query(sql, params=None) -> (columns, rows)
    register_frame(name, df)   # expose a pandas frame as a temp table
    commit() / close()
"""
from __future__ import annotations

from typing import Any, Sequence

from log_config import setup_logger
from scripts import config

logger = setup_logger(__name__, str(config.LOGS_DIR / 'warehouse.log'))


class LocalWarehouse:
    """DuckDB file database that plays the role of the Snowflake warehouse."""

    dialect = 'duckdb'

    def __init__(self) -> None:
        import duckdb

        config.LOCAL_WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(config.LOCAL_WAREHOUSE_PATH))
        logger.info(f'Connected to local warehouse {config.LOCAL_WAREHOUSE_PATH}')

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        return self.conn.execute(sql, params) if params else self.conn.execute(sql)

    def query(self, sql: str, params: Sequence[Any] | None = None):
        cur = self.conn.execute(sql, params) if params else self.conn.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        return columns, cur.fetchall()

    def register_frame(self, name: str, df) -> None:
        self.conn.register(name, df)

    def commit(self) -> None:  # DuckDB autocommits; kept for interface parity
        pass

    def close(self) -> None:
        self.conn.close()


class SnowflakeWarehouse:
    dialect = 'snowflake'

    def __init__(self) -> None:
        import snowflake.connector

        self.conn = snowflake.connector.connect(
            user=config.SNOWFLAKE_USER,
            password=config.SNOWFLAKE_PASSWORD,
            account=config.SNOWFLAKE_ACCOUNT,
            warehouse=config.SNOWFLAKE_WAREHOUSE,
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_SCHEMA,
        )
        self._cursor = self.conn.cursor()
        logger.info('Connected to Snowflake')

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        return self._cursor.execute(sql, params) if params else self._cursor.execute(sql)

    def query(self, sql: str, params: Sequence[Any] | None = None):
        cur = self.execute(sql, params)
        columns = [d[0] for d in cur.description] if cur.description else []
        return columns, cur.fetchall()

    def register_frame(self, name: str, df) -> None:  # pragma: no cover - not used
        raise NotImplementedError('Snowflake back-end loads via COPY INTO from S3')

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def connect() -> LocalWarehouse | SnowflakeWarehouse:
    return LocalWarehouse() if config.USE_LOCAL else SnowflakeWarehouse()
