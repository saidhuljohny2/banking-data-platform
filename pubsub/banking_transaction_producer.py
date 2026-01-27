import json
import random
import time
import uuid
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100" # replace with your GCP project ID
TOPIC_ID = "banking-transactions-topic"

EVENTS_PER_SECOND = 2   # control load (TPS)
RUN_FOREVER = True      # set False for testing

# =====================================================
# PUBSUB CLIENT
# =====================================================
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# =====================================================
# SAMPLE MASTER DATA (SIMULATION)
# =====================================================
CHANNELS = ["ATM", "UPI", "CARD", "NETBANKING"]
MERCHANTS = ["AMAZON", "FLIPKART", "SWIGGY", "ZOMATO", "IRCTC"]
STATUSES = ["SUCCESS", "FAILED"]
CURRENCY = "INR"

# =====================================================
# EVENT GENERATOR
# =====================================================
def generate_transaction_event():
    event = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:10]}",
        "account_id": random.randint(10001, 11000),
        "customer_id": random.randint(1, 500),
        "transaction_type": random.choice(["DEBIT", "CREDIT"]),
        "channel": random.choice(CHANNELS),
        "amount": round(random.uniform(10, 50000), 2),
        "currency": CURRENCY,
        "merchant": random.choice(MERCHANTS),
        "transaction_ts": datetime.now(timezone.utc).isoformat(),
        "status": random.choices(
            STATUSES,
            weights=[90, 10]  # realistic failure rate
        )[0]
    }
    return event

# =====================================================
# PUBLISH LOOP
# =====================================================
def publish_events():
    print("🚀 Starting Banking Transaction Producer...")
    while True:
        event = generate_transaction_event()
        message = json.dumps(event).encode("utf-8")

        future = publisher.publish(topic_path, message)
        print(f"✅ Published: {event['transaction_id']}")

        time.sleep(1 / EVENTS_PER_SECOND)

        if not RUN_FOREVER:
            break

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    publish_events()
