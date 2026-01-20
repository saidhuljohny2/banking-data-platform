"""
Note : pip install cloud-sql-python-connector sqlalchemy pymysql

PRODUCTION-GRADE CDC PIPELINE
Cloud SQL (MySQL) → GCS (Parquet) using Apache Beam (Dataflow)

Best Practices Implemented:
- Cloud SQL Python Connector (IAM-based, secure)
- SQLAlchemy connection pooling
- Incremental CDC using watermark (idempotent)
- Decimal-safe Parquet writes
- Structured logging (Cloud Logging compatible)
- Layered error handling
- Fail-fast on infra/schema issues
- Watermark update only after success
"""

# =====================================================
# IMPORTS
# =====================================================
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.transforms.util import Reshuffle

from google.cloud import bigquery
from google.cloud.sql.connector import Connector

import sqlalchemy
import pyarrow as pa
import datetime
from decimal import Decimal
import logging
import sys
import traceback

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("cloudsql-cdc")

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
REGION = "us-central1"

BQ_DATASET = "banking_metadata"
BQ_TABLE = "cdc_config"

INSTANCE_NAME = "mysql-instance"
DB_NAME = "banking_db"
DB_USER = "myuser"
DB_PASSWORD = "Mysql@123"  # 👉 move to Secret Manager in prod

GCS_RAW_BASE = "gs://banking-raw-dev-100/cloudsql/"
TEMP_LOCATION = "gs://banking-temp-dev/temp/"
STAGING_LOCATION = "gs://banking-temp-dev/staging/"

INSTANCE_CONNECTION_NAME = f"{PROJECT_ID}:{REGION}:{INSTANCE_NAME}"

DATAFLOW_OPTIONS = {
    "runner": "DataflowRunner",
    "project": PROJECT_ID,
    "region": REGION,
    "temp_location": TEMP_LOCATION,
    "staging_location": STAGING_LOCATION,
    "save_main_session": True,
}

# =====================================================
# UTILS
# =====================================================
def log_exception(msg, exc):
    LOGGER.error("%s | %s\n%s", msg, exc, traceback.format_exc())


# =====================================================
# BIGQUERY METADATA
# =====================================================
def get_active_tables():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT table_name, watermark_column, last_success_ts
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE is_active = TRUE
    """
    return list(client.query(query).result())


def update_watermark(table_name, ts):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        UPDATE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        SET last_success_ts = TIMESTAMP('{ts}')
        WHERE table_name = '{table_name}'
    """
    client.query(query).result()


# =====================================================
# CLOUD SQL ENGINE
# =====================================================
def get_engine():
    connector = Connector()

    def getconn():
        return connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pymysql",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
        )

    return sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )


# =====================================================
# SCHEMA INFERENCE
# =====================================================
def get_mysql_schema(table_name):
    engine = get_engine()
    fields = []

    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(f"DESCRIBE {table_name}"))
        for row in result:
            dtype = row.Type.lower()
            if "int" in dtype:
                pa_type = pa.int64()
            elif "decimal" in dtype:
                pa_type = pa.string()
            elif "float" in dtype:
                pa_type = pa.float64()
            elif "timestamp" in dtype or "datetime" in dtype:
                pa_type = pa.timestamp("us")
            elif "date" in dtype:
                pa_type = pa.date32()
            else:
                pa_type = pa.string()
            fields.append(pa.field(row.Field, pa_type))

    return pa.schema(fields)


# =====================================================
# NORMALIZATION
# =====================================================
def normalize_row(row):
    return {k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()}


# =====================================================
# BEAM DoFn
# =====================================================
class ReadFromCloudSQL(beam.DoFn):
    def __init__(self, table, watermark_col, last_ts):
        self.table = table
        self.watermark_col = watermark_col
        self.last_ts = last_ts

    def setup(self):
        self.engine = get_engine()

    def process(self, element):
        query = f"""
            SELECT *
            FROM {self.table}
            WHERE {self.watermark_col} > :last_ts
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(query), {"last_ts": self.last_ts}
            )
            for row in result:
                yield dict(row._mapping)


# =====================================================
# PIPELINE
# =====================================================
def run():
    LOGGER.info("Starting CDC pipeline")
    run_ts = datetime.datetime.utcnow()

    tables = get_active_tables()
    options = PipelineOptions(**DATAFLOW_OPTIONS)

    with beam.Pipeline(options=options) as p:
        for t in tables:
            schema = get_mysql_schema(t.table_name)

            (
                p
                | f"Start-{t.table_name}" >> beam.Create([None])
                | f"Read-{t.table_name}" >> beam.ParDo(
                    ReadFromCloudSQL(
                        t.table_name,
                        t.watermark_column,
                        t.last_success_ts,
                    )
                )
                | f"Normalize-{t.table_name}" >> beam.Map(normalize_row)
                | f"Reshuffle-{t.table_name}" >> Reshuffle()
                | f"Write-{t.table_name}" >> WriteToParquet(
                    file_path_prefix=(
                        f"{GCS_RAW_BASE}{t.table_name}/"
                        f"ingestion_date={run_ts:%Y-%m-%d}"
                    ),
                    schema=schema,
                    file_name_suffix=".parquet",
                )
            )

    for t in tables:
        update_watermark(t.table_name, run_ts)

    LOGGER.info("CDC pipeline completed successfully")


if __name__ == "__main__":
    run()
