#!/usr/bin/env python3
"""Read MOCK_DATA (*.csv) rows and publish each row as one JSON message to Kafka."""

import csv
import glob
import json
import os
import re
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


def main() -> int:
    data_dir = os.environ.get("DATA_DIR", "/data")
    topic = os.environ.get("KAFKA_TOPIC", "sales-raw")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    delay_ms = float(os.environ.get("MESSAGE_DELAY_MS", "0"))

    paths = sorted(glob.glob(os.path.join(data_dir, "MOCK_DATA (*.csv)")))
    if not paths:
        paths = sorted(
            p
            for p in glob.glob(os.path.join(data_dir, "MOCK_DATA*.csv"))
            if "(" in os.path.basename(p)
        )

    if not paths:
        print("No CSV files found under", data_dir, file=sys.stderr)
        return 1

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: v.encode("utf-8"),
        linger_ms=5,
        acks="all",
        retries=5,
    )

    file_index_re = re.compile(r"\((\d+)\)")

    total = 0
    for path in paths:
        print("Publishing", path, flush=True)
        base = os.path.basename(path)
        m = file_index_re.search(base)
        file_idx = int(m.group(1)) if m else 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    oid = int(row.get("id", "0") or 0)
                except ValueError:
                    oid = 0
                row["stream_file_index"] = file_idx
                row["stream_sale_id"] = file_idx * 1_000_000 + oid
                payload = json.dumps(row, ensure_ascii=False)
                producer.send(topic, value=payload)
                total += 1
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
        producer.flush()

    producer.flush()
    producer.close()
    print("Done, messages:", total, flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KafkaError as e:
        print("Kafka error:", e, file=sys.stderr)
        raise SystemExit(2)
