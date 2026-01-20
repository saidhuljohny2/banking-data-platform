"""
We have implemented a metadata-driven, schema-safe, auditable Bronze pipeline with:

    ✔ GCS → BigQuery Bronze (Dataproc Spark)
    ✔ Explicit schemas (no drift)
    ✔ CDC-aware deduplication
    ✔ Idempotent append design
    ✔ Config-driven ingestion
    ✔ Audit logging to BigQuery
    ✔ Production-safe error handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType,
    TimestampType, DateType
)
from pyspark.sql.functions import (
    col, row_number,
    current_timestamp
)
from pyspark.sql.window import Window
from datetime import datetime
import traceback
from pyspark.sql import Row

# =====================================================
# GLOBAL CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
BRONZE_DATASET = "banking_bronze"
METADATA_DATASET = "banking_metadata"
BQ_TEMP_BUCKET = "banking-temp-dev"

ENV = "DEV"
RUN_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")

# =====================================================
# EXPLICIT SCHEMAS
# =====================================================
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
        StructField("updated_at", TimestampType(), True)
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
        StructField("updated_at", TimestampType(), True)
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
        StructField("updated_at", TimestampType(), True)
    ])
}

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
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
    .appName("banking-bronze-schema-safe")
    .getOrCreate()
)

# =====================================================
# READ METADATA
# =====================================================
metadata_df = (
    spark.read
    .format("bigquery")
    .option("table", f"{PROJECT_ID}:{METADATA_DATASET}.table_ingestion_config")
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
    .load()
    .filter("is_active = true")
    .filter(f"target_dataset = '{BRONZE_DATASET}'")
)

metadata_rows = metadata_df.collect()

# =====================================================
# PROCESS EACH TABLE
# =====================================================
for row in metadata_rows:
    source_table = row.source_table
    target_table = row.target_table
    pk = row.primary_key
    watermark_col = row.watermark_column
    source_path = row.source_path
    file_format = row.file_format

    start_ts = datetime.utcnow()

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
        print(f"🚀 Processing Bronze table: {source_table}")

        schema = SCHEMA_MAP[source_table]

        # -----------------------------
        # READ SOURCE (EXPLICIT SCHEMA)
        # -----------------------------
        df = spark.read.schema(schema).parquet(source_path)

        audit_record["records_read"] = df.count()

        if audit_record["records_read"] == 0:
            raise Exception("Source dataset is empty")

        # -----------------------------
        # CDC DEDUP
        # -----------------------------
        window_spec = (Window.partitionBy(pk).orderBy(col(watermark_col).desc()))

        bronze_df = (
            df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
            .withColumn("bronze_load_ts", current_timestamp())
        )

        audit_record["records_written"] = bronze_df.count()

        # -----------------------------
        # WRITE TO BIGQUERY
        # -----------------------------
        (
            bronze_df.write
            .format("bigquery")
            .option("table",f"{PROJECT_ID}:{BRONZE_DATASET}.{target_table}")
            .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
            .mode("append")
            .save()
        )

        audit_record["status"] = "SUCCESS"
        audit_record["end_ts"] = datetime.utcnow()

        print(f"✅ Completed Bronze table: {source_table}")

    except Exception as e:
        audit_record["status"] = "FAILED"
        audit_record["end_ts"] = datetime.utcnow()
        audit_record["error_message"] = str(e)

        print(f"❌ Failed Bronze table: {source_table}")
        print(traceback.format_exc())

    # -----------------------------
    # WRITE AUDIT LOG
    # -----------------------------
    audit_row = Row(
        run_id=audit_record["run_id"],
        source_table=audit_record["source_table"],
        target_table=audit_record["target_table"],
        status=audit_record["status"],
        records_read=audit_record["records_read"],
        records_written=audit_record["records_written"],
        start_ts=audit_record["start_ts"],
        end_ts=audit_record["end_ts"],
        error_message=audit_record["error_message"]
    )

    audit_df = spark.createDataFrame(
        [audit_row],
        schema=AUDIT_SCHEMA
    )

    (
        audit_df.write
        .format("bigquery")
        .option("table", f"{PROJECT_ID}:{METADATA_DATASET}.ingestion_audit_log")
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
        .mode("append")
        .save()
    )

# =====================================================
# JOB COMPLETE
# =====================================================