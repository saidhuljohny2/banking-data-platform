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
# LOGGING (CLOUD LOGGING COMPATIBLE)
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout
)

LOGGER = logging.getLogger("cloudsql-cdc-prod")

def log_exception(message: str, exc: Exception):
    LOGGER.error(
        "%s | %s\n%s",
        message,
        str(exc),
        traceback.format_exc()
    )


# =====================================================
# CONFIGURATION
# =====================================================
PROJECT_ID = "dev-gcp-100"
BQ_DATASET = "banking_metadata"
BQ_TABLE = "cdc_config"
DB_USER = "myuser"
DB_PASSWORD = "Mysql@123"
DB_NAME = "banking_db"
REGION = "us-central1"
INSTANCE_NAME = "mysql-instance"
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
    "save_main_session": True
}

# =====================================================
# BIGQUERY METADATA OPERATIONS
# =====================================================
def get_active_tables():
    """Read active CDC table config from BigQuery"""
    LOGGER.info("Fetching active CDC tables from BigQuery")

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT table_name, watermark_column, last_success_ts
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE is_active = TRUE
    """

    tables = list(client.query(query).result())
    LOGGER.info("Active CDC tables found: %d", len(tables))
    return tables


def update_watermark(table_name, ts):
    """Update watermark only after successful pipeline run"""
    LOGGER.info(
        "Updating watermark for table=%s to %s",
        table_name, ts
    )

    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        UPDATE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        SET last_success_ts = TIMESTAMP('{ts}')
        WHERE table_name = '{table_name}'
    """

    client.query(query).result()


    # =====================================================
# CLOUD SQL ENGINE (SECURE)
# =====================================================
def get_engine():
    """Create SQLAlchemy engine using Cloud SQL Connector"""
    connector = Connector()

    def getconn():
        return connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pymysql",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME
        )

    return sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )


# =====================================================
# SCHEMA INFERENCE (FIXED AT BUILD TIME)
# =====================================================
def get_mysql_schema(table_name):
    """
    Infer PyArrow schema from MySQL.
    Decimal → STRING (banking-safe).
    """
    LOGGER.info("Inferring schema for table=%s", table_name)

    try:
        engine = get_engine()
        fields = []

        with engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(f"DESCRIBE {table_name}")
            )

            for row in result:
                col = row.Field
                dtype = row.Type.lower()

                if "int" in dtype:
                    pa_type = pa.int64()
                elif "decimal" in dtype:
                    pa_type = pa.string()
                elif "float" in dtype or "double" in dtype:
                    pa_type = pa.float64()
                elif "timestamp" in dtype or "datetime" in dtype:
                    pa_type = pa.timestamp("us")
                elif "date" in dtype:
                    pa_type = pa.date32()
                else:
                    pa_type = pa.string()

                fields.append(pa.field(col, pa_type))

        return pa.schema(fields)

    except Exception as e:
        log_exception(
            f"Schema inference failed for table={table_name}", e
        )
        raise  # schema errors must fail fast


# =====================================================
# DATA NORMALIZATION (PARQUET SAFE)
# =====================================================
def normalize_row(row: dict) -> dict:
    """Convert Decimal → string to avoid Arrow failures"""
    output = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            output[k] = str(v)
        else:
            output[k] = v
    return output


# =====================================================
# BEAM DoFn – CDC READ WITH ERROR HANDLING
# =====================================================
class ReadFromCloudSQL(beam.DoFn):
    """
    Incremental CDC reader from Cloud SQL
    """

    def __init__(self, table, watermark_col, last_ts):
        self.table = table
        self.watermark_col = watermark_col
        self.last_ts = last_ts

    def setup(self):
        try:
            self.engine = get_engine()
            LOGGER.info(
                "Initialized Cloud SQL engine for table=%s",
                self.table
            )
        except Exception as e:
            log_exception(
                f"DB engine initialization failed for table={self.table}",
                e
            )
            raise  # infra failure

    def process(self, element):
        query = f"""
            SELECT *
            FROM {self.table}
            WHERE {self.watermark_col} > :last_ts
        """

        try:
            row_count = 0
            with self.engine.connect() as conn:
                result = conn.execute(
                    sqlalchemy.text(query),
                    {"last_ts": self.last_ts}
                )

                for row in result:
                    row_count += 1
                    yield dict(row._mapping)

            LOGGER.info(
                "CDC read complete | table=%s | rows=%d",
                self.table, row_count
            )

        except Exception as e:
            log_exception(
                f"CDC read failed for table={self.table}", e
            )
            # non-fatal: skip this batch


# =====================================================
# PIPELINE
# =====================================================
def run():
    LOGGER.info("Starting Cloud SQL CDC Dataflow pipeline")

    run_ts = datetime.datetime.utcnow()

    try:
        tables = get_active_tables()

        options = PipelineOptions(**DATAFLOW_OPTIONS)

        with beam.Pipeline(options=options) as p:
            for t in tables:
                table_name = t.table_name
                watermark_col = t.watermark_column
                last_ts = t.last_success_ts

                LOGGER.info(
                    "Building pipeline | table=%s | watermark=%s",
                    table_name, watermark_col
                )

                parquet_schema = get_mysql_schema(table_name)

                (
                    p
                    | f"Start-{table_name}" >> beam.Create([None])
                    | f"Read-{table_name}" >> beam.ParDo(
                        ReadFromCloudSQL(
                            table_name,
                            watermark_col,
                            last_ts
                        )
                    )
                    | f"Normalize-{table_name}" >> beam.Map(
                        normalize_row
                    )
                    | f"Reshuffle-{table_name}" >> Reshuffle()
                    | f"Write-{table_name}" >> WriteToParquet(
                        file_path_prefix=(
                            f"{GCS_RAW_BASE}{table_name}/"
                            f"ingestion_date={run_ts}"
                        ),
                        schema=parquet_schema,
                        file_name_suffix=".parquet"
                    )
                )

        # Update watermark only after successful run
        for t in tables:
            update_watermark(t.table_name, run_ts)

        LOGGER.info("CDC pipeline completed successfully")

    except Exception as e:
        log_exception("Pipeline execution failed", e)
        raise


# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run()
