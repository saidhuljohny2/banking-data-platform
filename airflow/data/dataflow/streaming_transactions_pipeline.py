import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# =====================================================
# CONFIGURATION
# =====================================================
PROJECT_ID = "dev-gcp-100"
REGION = "us-central1"

# Pub/Sub subscription for streaming transactions
INPUT_SUBSCRIPTION = (
    "projects/dev-gcp-100/subscriptions/"
    "banking-transactions-stream-sub"
)

# BigQuery Bronze table (append-only)
BQ_TABLE = "banking_bronze.bronze_streaming_transactions"

# Dead-letter topic for invalid / corrupt messages
DEADLETTER_TOPIC = (
    "projects/dev-gcp-100/topics/"
    "banking-transactions-deadletter"
)

# =====================================================
# PIPELINE OPTIONS (STREAMING MODE)
# =====================================================
pipeline_options = PipelineOptions(
    streaming=True,               # Required for Pub/Sub streaming
    project=PROJECT_ID,
    region=REGION,
    save_main_session=True        # Needed for DoFn class serialization
)

# =====================================================
# PARSE & VALIDATE EVENTS
# =====================================================
class ParseAndValidateEvent(beam.DoFn):
    """
    Parses JSON message from Pub/Sub and validates required fields.
    Invalid messages are routed to a dead-letter topic.
    """

    def process(self, message):
        try:
            # Decode Pub/Sub message (bytes → dict)
            event = json.loads(message.decode("utf-8"))

            # Mandatory banking transaction fields
            required_fields = [
                "event_id",
                "transaction_id",
                "account_id",
                "customer_id",
                "amount",
                "currency",
                "transaction_type",
                "channel",
                "merchant",
                "event_ts"
            ]

            # Validate required fields
            for field in required_fields:
                if field not in event:
                    raise ValueError(f"Missing field: {field}")

            # Convert event timestamp to datetime (event time)
            event["event_ts"] = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))

            # Add ingestion timestamp (processing time)
            event["ingest_ts"] = datetime.utcnow()

            # Emit valid event
            yield event

        except Exception as e:
            # Log error and send raw message to dead-letter stream
            logging.error(f"Invalid message: {e}")
            yield beam.pvalue.TaggedOutput("deadletter",message.decode("utf-8"))

# =====================================================
# FORMAT RECORD FOR BIGQUERY
# =====================================================
class FormatForBigQuery(beam.DoFn):
    """
    Converts validated event into BigQuery-compatible dictionary.
    """

    def process(self, event):
        yield {
            "event_id": event["event_id"],
            "transaction_id": event["transaction_id"],
            "account_id": event["account_id"],
            "customer_id": event["customer_id"],
            "amount": event["amount"],
            "currency": event["currency"],
            "transaction_type": event["transaction_type"],
            "channel": event["channel"],
            "merchant": event["merchant"],
            "event_ts": event["event_ts"],
            "ingest_ts": event["ingest_ts"]
        }

# =====================================================
# PIPELINE DEFINITION
# =====================================================
def run():
    """
    Streaming pipeline:
    Pub/Sub → Validation → Windowing → Deduplication → BigQuery
    """

    with beam.Pipeline(options=pipeline_options) as p:

        # -------------------------------------------------
        # READ STREAMING DATA FROM PUB/SUB
        # -------------------------------------------------
        events = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidateEvent()).with_outputs("deadletter", main="valid")
        )

        valid_events = events.valid
        deadletter_events = events.deadletter

        # -------------------------------------------------
        # DEAD-LETTER HANDLING
        # -------------------------------------------------
        # Invalid messages are published for replay/debugging
        deadletter_events | "WriteDeadLetter" >> beam.io.WriteToPubSub(DEADLETTER_TOPIC)

        # -------------------------------------------------
        # EVENT TIME ASSIGNMENT & WINDOWING
        # -------------------------------------------------
        windowed_events = (
            valid_events
            # Assign event time for watermarking
            | "AssignEventTime" >> beam.Map(lambda e: beam.window.TimestampedValue(e, e["event_ts"].timestamp()))
            
            # Fixed 1-minute windows with late data support
            | "WindowIntoFixedWindows" >> beam.WindowInto(FixedWindows(60),allowed_lateness=300)   # Accept late events up to 5 minutes
        )

        # -------------------------------------------------
        # DEDUPLICATION (EVENT_ID)
        # -------------------------------------------------
        # Safe deduplication for streaming pipelines
        deduped_events = (windowed_events | "DeduplicateByEventId" >> beam.Distinct())

        # -------------------------------------------------
        # WRITE TO BIGQUERY (BRONZE LAYER)
        # -------------------------------------------------
        (
            deduped_events
            | "FormatForBQ" >> beam.ParDo(FormatForBigQuery())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                custom_gcs_temp_location="gs://banking-temp-dev/temp/"
            )
        )

# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    run()