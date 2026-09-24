"""Publish only application events to Kafka, with resumable micro-batches."""
import argparse
import hashlib
import json
import os
import random
import time
from datetime import timedelta

from .common import config, iso, read_jsonl, read_parquet, resolve, sha256, timestamp, write_json
from .generate_data import PRODUCTS, create
from .kafka import producer as make_producer


def context(cfg):
    root = resolve(cfg["generated_dir"])
    customers = {c["customer_id"]: c for c in read_parquet(root / "customers.parquet")}
    customers.update({c["customer_id"]: c for c in read_parquet(root / "batch_customers.parquet")})
    subscription_by_id = {s["subscription_id"]: s for s in read_parquet(root / "subscriptions.parquet")}
    subscription_by_id.update({s["subscription_id"]: s for s in read_parquet(root / "batch_subscriptions.parquet")})
    subs = [s for s in subscription_by_id.values() if s["status"] == "active"]
    incidents = (json.loads((root / "incidents.json").read_text(encoding="utf-8")) +
                 read_jsonl(root / "batch_incidents.jsonl"))
    fingerprint = hashlib.sha256("".join(sha256(root / name) for name in
                                   ("customers.parquet", "subscriptions.parquet", "incidents.json",
                                    "batch_customers.parquet", "batch_subscriptions.parquet",
                                    "batch_incidents.jsonl")).encode()).hexdigest()
    return customers, subs, incidents, fingerprint


def event_at(index, cfg, customers, subs, incidents):
    rng = random.Random(cfg["seed"] * 1000003 + index)
    burst = index % 10 < min(9, int(cfg["stream"]["incident_burst_multiplier"]))
    incident = incidents[(index // 10) % len(incidents)] if burst else None
    if incident:
        matches = [s for s in subs if next(p["service"] for p in PRODUCTS if p["product_id"] == s["product_id"]) == incident["service"]
                   and (incident["region"] == "all" or customers[s["customer_id"]]["region"] == incident["region"])]
        sub = rng.choice(matches)
        event_time = timestamp(incident["start_time"]) + timedelta(seconds=rng.randint(0, 6*3600-1))
        event_type = ("PAYMENT_ATTEMPT" if incident["service"] == "billing" else
                      rng.choice(["ERROR", "TIMEOUT", "API_REQUEST"] if incident["service"] == "api" else
                                 ["ERROR", "TIMEOUT", "FILE_UPLOAD", "FILE_DOWNLOAD"]))
    else:
        sub = rng.choice(subs)
        base = timestamp(cfg["initial"]["arrival_time"])
        event_time = base + timedelta(seconds=rng.randint(0, cfg["days"] * 86400-1))
        service = next(p["service"] for p in PRODUCTS if p["product_id"] == sub["product_id"])
        allowed = {"api": ["LOGIN", "API_REQUEST", "EXPORT", "ERROR", "TIMEOUT"],
                   "billing": ["LOGIN", "PAYMENT_ATTEMPT", "ERROR", "TIMEOUT"],
                   "storage": ["LOGIN", "FILE_UPLOAD", "FILE_DOWNLOAD", "EXPORT", "ERROR", "TIMEOUT"]}
        event_type = rng.choice(allowed[service])
    service = next(p["service"] for p in PRODUCTS if p["product_id"] == sub["product_id"])
    failed = incident is not None or event_type in ("ERROR", "TIMEOUT")
    error_code = incident["error_code"] if incident else ("REQUEST_TIMEOUT" if event_type == "TIMEOUT" else "APPLICATION_ERROR" if event_type == "ERROR" else None)
    status = 504 if event_type == "TIMEOUT" or (incident and incident["service"] == "billing") else 503 if failed else 200
    latency = rng.randint(4000, 15000) if failed else rng.randint(30, 900)
    delay = rng.randint(1, 30)
    if rng.random() < cfg["stream"]["late_rate"]:
        delay += rng.randint(2, 24) * 3600
    if rng.random() < cfg["stream"]["out_of_order_rate"]:
        delay += rng.randint(1, 90) * 60
    arrival_base = max(event_time, timestamp(cfg["initial"]["arrival_time"]) + timedelta(seconds=index))
    return {"event_id": f"EVT-{index+1:09d}", "event_time": iso(event_time),
            "arrival_time": iso(arrival_base + timedelta(seconds=delay)),
            "customer_id": sub["customer_id"], "product_id": sub["product_id"],
            "subscription_id": sub["subscription_id"], "service": service,
            "event_type": event_type, "error_code": error_code, "http_status": status,
            "latency_ms": latency, "region": customers[sub["customer_id"]]["region"],
            "incident_id": incident["incident_id"] if incident else None}


def publish(cfg, max_events, resume=False, state_file="stream_state.json", pause_file="stream.pause", client=None, sleep=time.sleep):
    customers, subs, incidents, fingerprint = context(cfg)
    state_path = resolve(state_file)
    pause_path = resolve(pause_file)
    if state_path.exists():
        if not resume:
            raise ValueError(f"Checkpoint exists: {state_path}; pass --resume")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state["fingerprint"] != fingerprint or state["seed"] != cfg["seed"]:
            raise ValueError("Streaming inputs changed since checkpoint")
        index = state["next_index"]
    else:
        index = 0
    own_client = client is None
    client = client or make_producer(os.getenv("AIVEN_KAFKA_SERVICE_URI"))
    sent = 0
    emitted = 0
    try:
        while emitted < max_events:
            while pause_path.exists():
                sleep(.25)
            n = min(cfg["stream"]["micro_batch_size"], max_events-emitted)
            records = [event_at(i, cfg, customers, subs, incidents) for i in range(index, index+n)]
            # Arrival order differs from event-time order. Duplicates retain the same event_id.
            rng = random.Random(cfg["seed"] + index)
            records.sort(key=lambda row: row["arrival_time"])
            records += [row.copy() for row in records if rng.random() < cfg["stream"]["duplicate_rate"]]
            futures = []
            for event in records:
                futures.append(client.send(cfg["stream"]["topic"], key=event["customer_id"], value=event))
                sent += 1
            client.flush()
            for future in futures:
                if future is not None:
                    future.get(timeout=30)
            # Checkpoint only after acknowledgement; crash may replay last micro-batch.
            index += n
            emitted += n
            write_json(state_path, {"fingerprint": fingerprint, "seed": cfg["seed"], "next_index": index})
            if emitted < max_events:
                sleep(n / cfg["stream"]["event_rate"])
    finally:
        if own_client:
            client.close()
    return {"logical_events": emitted, "published_messages": sent, "next_index": index}


def run(cfg, max_events, resume=False, state_file="stream_state.json", pause_file="stream.pause",
        prepare=False, kaggle_csv=None, client=None, sleep=time.sleep):
    if prepare:
        create(cfg, kaggle_csv)
    return publish(cfg, max_events, resume, state_file, pause_file, client, sleep)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--max-events", type=int, default=30)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--state-file", default="stream_state.json")
    p.add_argument("--pause-file", default="stream.pause")
    p.add_argument("--event-rate", type=float)
    p.add_argument("--micro-batch-size", type=int)
    p.add_argument("--prepare", action="store_true", help="Generate source data before publishing events")
    p.add_argument("--kaggle-csv", help="Local Kaggle ticket CSV; use with --prepare")
    args = p.parse_args()
    cfg = config(args.config)
    cfg["stream"]["topic"] = os.getenv("AIVEN_KAFKA_TOPIC", cfg["stream"]["topic"])
    if args.event_rate is not None:
        cfg["stream"]["event_rate"] = args.event_rate
    if args.micro_batch_size is not None:
        cfg["stream"]["micro_batch_size"] = args.micro_batch_size
    if args.max_events < 1 or cfg["stream"]["event_rate"] <= 0 or cfg["stream"]["micro_batch_size"] < 1:
        p.error("max-events, event-rate, and micro-batch-size must be positive")
    if args.kaggle_csv and not args.prepare:
        p.error("--kaggle-csv requires --prepare")
    print(run(cfg, args.max_events, args.resume, args.state_file, args.pause_file,
              args.prepare, args.kaggle_csv))


if __name__ == "__main__":
    main()
