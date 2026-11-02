import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------- Reference data ----------
HOME_CITIES = {
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.6139, 77.2090),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867),
}
FAR_CITIES = {
    "London": (51.5074, -0.1278),
    "New York": (40.7128, -74.0060),
    "Singapore": (1.3521, 103.8198),
    "Dubai": (25.2048, 55.2708),
}
MERCHANTS = [
    ("Big Bazaar", "grocery"), ("Amazon", "ecommerce"), ("Flipkart", "ecommerce"),
    ("Swiggy", "food"), ("Zomato", "food"), ("IndianOil", "fuel"),
    ("Uber", "transport"), ("Apollo Pharmacy", "health"), ("Netflix", "subscription"),
    ("Croma", "electronics"), ("Tanishq", "jewellery"), ("MakeMyTrip", "travel"),
]

TOPIC = "transactions"


# ---------- Card profiles ----------
class Card:
    def __init__(self, card_id):
        self.card_id = card_id
        self.home_city = random.choice(list(HOME_CITIES))
        self.avg_amount = random.choice([300, 600, 1200, 2500, 5000])
        self.devices = [f"dev-{uuid.uuid4().hex[:8]}" for _ in range(random.randint(1, 2))]


def jitter(lat, lon, km=8):
    # ~0.009 degrees is roughly 1 km
    d = km * 0.009
    return round(lat + random.uniform(-d, d), 5), round(lon + random.uniform(-d, d), 5)


def build_event(card, amount=None, city=None, coords=None, device=None,
                fraud_type=None):
    merchant, category = random.choice(MERCHANTS)
    if amount is None:
        amount = max(20, random.lognormvariate(0, 0.5) * card.avg_amount)
    if coords is None:
        city = card.home_city
        coords = jitter(*HOME_CITIES[city])
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card.card_id,
        "amount": round(amount, 2),
        "currency": "INR",
        "merchant": merchant,
        "merchant_category": category,
        "city": city,
        "latitude": coords[0],
        "longitude": coords[1],
        "device_id": device or random.choice(card.devices),
        "event_time": datetime.now(timezone.utc).isoformat(),
        # Ground-truth labels: for evaluation ONLY, never used by detection rules
        "is_injected_fraud": fraud_type is not None,
        "fraud_type": fraud_type,
    }


# ---------- Fraud scenarios (each returns a list of events) ----------
def fraud_burst(card):
    n = random.randint(5, 8)
    rogue_device = f"dev-{uuid.uuid4().hex[:8]}"
    return [build_event(card, amount=random.uniform(200, 1500),
                        device=rogue_device, fraud_type="burst") for _ in range(n)]


def fraud_impossible_travel(card):
    normal = build_event(card)  # legit transaction at home
    city = random.choice(list(FAR_CITIES))
    far = build_event(card, city=city, coords=jitter(*FAR_CITIES[city], km=5),
                      device=f"dev-{uuid.uuid4().hex[:8]}",
                      fraud_type="impossible_travel")
    return [normal, far]


def fraud_large_amount(card):
    amount = card.avg_amount * random.uniform(8, 20)
    return [build_event(card, amount=amount, fraud_type="large_amount")]


FRAUD_SCENARIOS = [fraud_burst, fraud_impossible_travel, fraud_large_amount]


# ---------- Kafka ----------
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--rate", type=float, default=5, help="normal events per second")
    parser.add_argument("--fraud-prob", type=float, default=0.03,
                        help="probability that a tick triggers a fraud scenario")
    parser.add_argument("--cards", type=int, default=200)
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20})
    cards = [Card(f"card-{i:05d}") for i in range(args.cards)]
    sent = 0

    print(f"Producing to '{TOPIC}' at ~{args.rate}/s. Ctrl+C to stop.")
    try:
        while True:
            card = random.choice(cards)
            if random.random() < args.fraud_prob:
                events = random.choice(FRAUD_SCENARIOS)(card)
            else:
                events = [build_event(card)]

            for e in events:
                e["event_time"] = datetime.now(timezone.utc).isoformat()
                producer.produce(
                    TOPIC,
                    key=e["card_id"],          # same card -> same partition -> ordered
                    value=json.dumps(e).encode("utf-8"),
                    callback=delivery_report,
                )
                sent += 1
                if len(events) > 1:
                    time.sleep(random.uniform(0.1, 0.8))  # burst spacing
            producer.poll(0)

            if sent % 100 < len(events):
                print(f"sent={sent}")
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Stopped. Total sent: {sent}")


if __name__ == "__main__":
    main()