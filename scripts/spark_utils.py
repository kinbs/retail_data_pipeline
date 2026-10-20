"""Spark session factory (handles the JDK lookup on Windows)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from scripts import config


def _find_jdk() -> Path | None:
    """Return a usable JAVA_HOME, preferring the portable JDK shipped with the repo."""
    candidates: list[Path] = []

    env_home = os.getenv('JAVA_HOME')
    if env_home:
        candidates.append(Path(env_home))

    jdk_dir = config.BASE_DIR / '.jdk'
    if jdk_dir.exists():
        for child in sorted(jdk_dir.iterdir()):
            if child.is_dir():
                candidates.append(child)
                candidates.extend(sorted(p for p in child.iterdir() if p.is_dir()))

    for candidate in candidates:
        java_bin = candidate / 'bin' / ('java.exe' if sys.platform == 'win32' else 'java')
        if java_bin.exists():
            return candidate
    return None


def _configure_java() -> None:
    jdk = _find_jdk()
    if jdk is None:
        raise RuntimeError(
            'No JDK found. PySpark needs Java 17+. Either set JAVA_HOME, or unpack a JDK '
            f'into {config.BASE_DIR / ".jdk"} (a portable Temurin build works fine).'
        )
    os.environ['JAVA_HOME'] = str(jdk)
    os.environ['PATH'] = str(jdk / 'bin') + os.pathsep + os.environ.get('PATH', '')


def get_spark(app_name: str = 'RetailDataPipeline'):
    """Create (or reuse) the Spark session used by the ETL and the analysis."""
    _configure_java()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(config.SPARK_MASTER)
        .config('spark.driver.memory', config.SPARK_DRIVER_MEMORY)
        .config('spark.sql.shuffle.partitions', str(config.SPARK_SHUFFLE_PARTITIONS))
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.warehouse.dir', str(config.BASE_DIR / 'spark-warehouse'))
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.SPARK_LOG_LEVEL)
    return spark


def stop_spark(spark) -> None:
    if spark is not None:
        try:
            spark.stop()
        except Exception:  # pragma: no cover - best effort on shutdown
            pass
