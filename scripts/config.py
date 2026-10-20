"""Central configuration.

Two ways to run the pipeline:

* ``USE_LOCAL=true`` (default) - a fully local stack that needs no cloud account:
  the "data lake" is a mounted folder (``lake/``) and the "warehouse" is DuckDB
  (``warehouse/retail_sales.duckdb``). Same code path, same star schema.
* ``USE_LOCAL=false`` - the original design: Amazon S3 + Snowflake.

Every value can be overridden through environment variables or a ``.env`` file,
so no credential has to be hard-coded in the repository.
"""
import os
from pathlib import Path

try:  # python-dotenv is optional
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_env() -> None:
    env_file = BASE_DIR / ".env"
    if load_dotenv is not None and env_file.exists():
        load_dotenv(env_file)


_load_env()


def _flag(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def _path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser().resolve() if value else default


# --------------------------------------------------------------------------- #
# Runtime mode
# --------------------------------------------------------------------------- #
USE_LOCAL = _flag("USE_LOCAL", "true")

# --------------------------------------------------------------------------- #
# Amazon S3 (only used when USE_LOCAL=false)
# --------------------------------------------------------------------------- #
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", "your-aws-s3-bucket-name")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "your-aws-access-key")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "your-aws-secret-key")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Optional custom endpoint, handy for MinIO / LocalStack instead of real S3.
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or None

# --------------------------------------------------------------------------- #
# Snowflake (only used when USE_LOCAL=false)
# --------------------------------------------------------------------------- #
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "your-snowflake-username")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "your-snowflake-password")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "your-snowflake-account")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_SALES_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_SALES_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RETAIL_SALES_DATA_SCHEMA")

# --------------------------------------------------------------------------- #
# Local stack (only used when USE_LOCAL=true)
# --------------------------------------------------------------------------- #
# Folder that emulates the bucket: <lake>/data/... and <lake>/processed_data/...
LOCAL_LAKE_DIR = _path("LOCAL_LAKE_DIR", BASE_DIR / "lake")
LOCAL_WAREHOUSE_PATH = _path("LOCAL_WAREHOUSE_PATH", BASE_DIR / "warehouse" / "retail_sales.duckdb")

# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "WARN")

# --------------------------------------------------------------------------- #
# Layout inside the data lake
# --------------------------------------------------------------------------- #
RAW_PREFIX = os.getenv("RAW_PREFIX", "data/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "processed_data/")
RAW_FILE_KEY = f"{RAW_PREFIX}statsfinal.csv"

# Raw CSV shipped with the repository, used to seed an empty lake.
LOCAL_RAW_CSV = BASE_DIR / "data" / "statsfinal.csv"

# Where downloaded / generated artefacts land.
DATA_DIR = BASE_DIR / "data"
VISUALIZATIONS_DIR = BASE_DIR / "visualizations"
LOGS_DIR = BASE_DIR / "logs"


def describe() -> str:
    if USE_LOCAL:
        return f"LOCAL (lake={LOCAL_LAKE_DIR}, warehouse={LOCAL_WAREHOUSE_PATH})"
    return f"AWS+S3 (s3://{AWS_S3_BUCKET_NAME}) + Snowflake ({SNOWFLAKE_ACCOUNT})"
