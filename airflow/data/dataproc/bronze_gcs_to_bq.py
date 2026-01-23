"""
=====================================================
BANKING DOMAIN – BRONZE INGESTION PIPELINE
=====================================================

This job implements a production-grade Bronze layer with:

✔ Metadata-driven ingestion
✔ Explicit schemas (schema-safe)
✔ CDC-aware deduplication
✔ Idempotent-friendly append design
✔ Centralized audit logging
✔ Table-level fault isolation
✔ Dataproc Spark → BigQuery

Bronze Philosophy:
- Capture RAW data safely
- No business transformations
- Only technical enrichment
"""

# =====================================================
# 1. IMPORTS
# =====================================================

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType,
    TimestampType, DateType
)
from pyspark.sql.functions import (
    col,
    row_number,
    current_timestamp,
    lit
)
from pyspark.sql.window import Window
from datetime import datetime
import traceback

# =====================================================
# 2. GLOBAL CONFIGURATION
# =====================================================

# GCP Project
PROJECT_ID = "dev-gcp-100"

# BigQuery datasets
BRONZE_DATASET = "banking_bronze"
METADATA_DATASET = "banking_metadata"

# Temporary GCS bucket for BigQuery connector
BQ_TEMP_BUCKET = "banking-temp-dev"

# Environment details
ENV = "DEV"
RUN_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")

# =====================================================
# 3. EXPLICIT SCHEMAS (NO SCHEMA DRIFT)
# =====================================================
# Each source table must have a predefined schema.
# Any new column in source must be added here explicitly.

SCHEMA_MAP = {

    "customers": StructType([
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("bronze_load_ts", TimestampType(), True),
    ]),

    "accounts": StructType([
        StructField("account_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("opened_date", DateType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("bronze_load_ts", TimestampType(), True),
    ]),

    "transactions": StructType([
        StructField("transaction_id", LongType(), True),
        StructField("account_id", LongType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("transaction_ts", TimestampType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("bronze_load_ts", TimestampType(), True),
    ])
}

# =====================================================
# 4. AUDIT TABLE SCHEMA
# =====================================================
# Stores ingestion status for monitoring and debugging

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("status", StringType(), False),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("error_message", StringType(), True)
])

# =====================================================
# 5. SPARK SESSION INITIALIZATION
# =====================================================

spark = (
    SparkSession.builder
    .appName("banking-bronze-schema-safe")
    .getOrCreate()
)

# =====================================================
# 6. AUDIT LOG WRITER FUNCTION
# =====================================================
# Ensures audit is written even if table processing fails

def write_audit_log(spark, audit_record):

    audit_df = spark.createDataFrame(
        [audit_record],
        schema=AUDIT_SCHEMA
    )

    (
        audit_df.write
        .format("bigquery")
        .option("table", f"{METADATA_DATASET}.ingestion_audit_log")
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
        .mode("append")
        .save()
    )

# =====================================================
# 7. READ INGESTION METADATA
# =====================================================
# Metadata table controls:
# - which tables are active
# - source path
# - primary key
# - watermark column

metadata_df = (
    spark.read
    .format("bigquery")
    .option("table", f"{METADATA_DATASET}.table_ingestion_config")
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
    .load()
    .filter("""
        is_active = true
        AND target_dataset = 'banking_bronze'
    """)
)

# =====================================================
# 8. PROCESS EACH TABLE INDEPENDENTLY
# =====================================================
# toLocalIterator avoids driver OOM for large metadata

for row in metadata_df.toLocalIterator():

    source_table = row.source_table
    target_table = row.target_table
    primary_key = row.primary_key
    watermark_col = row.watermark_column
    source_path = row.source_path

    start_ts = datetime.utcnow()

    # Initialize audit record
    audit_record = {
        "run_id": RUN_ID,
        "source_table": source_table,
        "target_table": target_table,
        "status": "STARTED",
        "records_read": 0,
        "records_written": 0,
        "start_ts": start_ts,
        "end_ts": None,
        "error_message": None
    }

    try:
        print(f"🚀 Starting Bronze ingestion for: {source_table}")

        # -------------------------------------------------
        # VALIDATION CHECKS
        # -------------------------------------------------

        if source_table not in SCHEMA_MAP:
            raise ValueError(f"Schema not defined for table: {source_table}")

        if not primary_key or not watermark_col:
            raise ValueError("Primary key or watermark column missing in metadata")

        schema = SCHEMA_MAP[source_table]

        # -------------------------------------------------
        # READ SOURCE DATA WITH EXPLICIT SCHEMA
        # -------------------------------------------------

        df = spark.read.schema(schema).parquet(source_path)
        print(source_path)
        # print(schema)

        records_read = df.count()
        audit_record["records_read"] = records_read

        if records_read == 0:
            raise Exception("Source dataset is empty")

        # -------------------------------------------------
        # CDC DEDUPLICATION
        # Keep latest record per primary key
        # -------------------------------------------------

        window_spec = (
            Window
            .partitionBy(primary_key)
            .orderBy(col(watermark_col).desc())
        )

        bronze_df = (
            df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
            .withColumn("bronze_load_ts", current_timestamp())
        )

        records_written = bronze_df.count()
        audit_record["records_written"] = records_written

        # -------------------------------------------------
        # WRITE TO BIGQUERY BRONZE TABLE
        # -------------------------------------------------

        (
            bronze_df.write
            .format("bigquery")
            .option("table", f"{BRONZE_DATASET}.{target_table}")
            .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
            .mode("append")
            .save()
        )

        audit_record["status"] = "SUCCESS"
        audit_record["end_ts"] = datetime.utcnow()

        print(f"✅ Completed Bronze ingestion for: {source_table}")

    except Exception as e:
        # -------------------------------------------------
        # ERROR HANDLING
        # -------------------------------------------------

        audit_record["status"] = "FAILED"
        audit_record["end_ts"] = datetime.utcnow()
        audit_record["error_message"] = str(e)

        print(f"❌ Failed Bronze ingestion for: {source_table}")
        print(traceback.format_exc())

    finally:
        # -------------------------------------------------
        # WRITE AUDIT LOG (ALWAYS EXECUTES)
        # -------------------------------------------------

        write_audit_log(spark, audit_record)

# =====================================================
# 9. JOB COMPLETION
# =====================================================

print("🎯 Bronze ingestion job completed successfully")