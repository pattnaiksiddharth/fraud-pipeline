import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# Reuse the scenario builders from the main generator
from producer import (TOPIC, Card, build_event, fraud_burst,
                      fraud_impossible_travel, fraud_large_amount)


def send(producer, event):
    # Stamp the time at send, so spacing between events is realistic
    event["event_time"] = datetime.now(timezone.utc).isoformat()
    producer.produce(TOPIC, key=event["card_id"],
                     value=json.dumps(event).encode("utf-8"))
    producer.poll(0)


def run(producer, scenario):
    card = Card(f"card-demo-{uuid.uuid4().hex[:4]}")
    print(f"\n>>> {scenario.upper()} attack on {card.card_id} (home: {card.home_city})")

    if scenario == "large":
        print("Warming up with 3 normal purchases (about 15 seconds)...")
        for _ in range(3):
            send(producer, build_event(card))
            time.sleep(5)
        events = fraud_large_amount(card)
    elif scenario == "burst":
        events = fraud_burst(card)
    else:
        events = fraud_impossible_travel(card)

    for e in events:
        send(producer, e)
        print(f"sent {e['amount']:>10,.2f} INR  {e['city']:<10} {e['merchant']}")
        time.sleep(random.uniform(0.1, 0.8) if scenario == "burst" else 1.5)

    producer.flush()
    print(f"Done. Search for {card.card_id} in the dashboard or analyst console.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True,
                        choices=["burst", "travel", "large", "all"])
    parser.add_argument("--bootstrap", default="localhost:29092")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    for s in (["burst", "travel", "large"] if args.scenario == "all" else [args.scenario]):
        run(producer, s)
        if args.scenario == "all":
            time.sleep(10)


if __name__ == "__main__":
    main()