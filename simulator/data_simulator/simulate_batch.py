"""Emit immutable local source batches; optionally replay copies to a separate drop."""
import argparse
import copy
import hashlib
import json
import random
import shutil
from datetime import timedelta

from .common import config, iso, read_jsonl, read_parquet, resolve, sha256, timestamp, write_immutable, write_json
from .generate_data import create
from .generate_knowledge import generate as generate_knowledge
from .simulate_initial import INPUTS as INITIAL_INPUTS, fingerprint as initial_fingerprint
from .upload_volume import upload


INPUTS = {"customers": "batch_customers.parquet", "products": "batch_products.parquet",
          "subscriptions": "batch_subscriptions.parquet", "payments": "batch_payments.parquet",
          "tickets": "batch_tickets.jsonl", "incidents": "batch_incidents.jsonl",
          "knowledge_articles": "knowledge_articles_batch.jsonl"}


def inject(rows, source, rates, seed):
    rng = random.Random(seed)
    changed = copy.deepcopy(rows)
    counts = {key: 0 for key in rates}
    id_field = {"customers": "customer_id", "products": "product_id", "subscriptions": "subscription_id",
                "payments": "payment_id", "tickets": "ticket_id", "incidents": "incident_id",
                "knowledge_articles": "article_id"}[source]
    for row in changed:
        if rng.random() < rates.get("null_required_rate", 0):
            row[id_field] = None
            counts["null_required_rate"] += 1
        if rng.random() < rates.get("invalid_value_rate", 0):
            field = "status" if source in ("subscriptions", "payments", "tickets") else "region" if source == "customers" else "service" if source in ("products", "incidents") else "title"
            row[field] = "INVALID_VALUE"
            counts["invalid_value_rate"] += 1
        if source in ("subscriptions", "payments", "tickets") and rng.random() < rates.get("invalid_fk_rate", 0):
            row["customer_id"] = "CUST-DOES-NOT-EXIST"
            counts["invalid_fk_rate"] += 1
        time_field = {"customers": "created_at", "subscriptions": "started_at", "payments": "attempted_at", "tickets": "opened_at", "incidents": "start_time"}.get(source)
        if time_field and rng.random() < rates.get("invalid_timestamp_rate", 0):
            row[time_field] = "not-a-timestamp"
            counts["invalid_timestamp_rate"] += 1
        if source == "payments" and rng.random() < rates.get("negative_payment_rate", 0):
            row["amount"] = -abs(row["amount"])
            counts["negative_payment_rate"] += 1
    for row in list(changed):
        if rng.random() < rates.get("duplicate_rate", 0):
            changed.append(copy.deepcopy(row))
            counts["duplicate_rate"] += 1
    return changed, counts


def _load(path):
    return read_parquet(path) if path.suffix == ".parquet" else read_jsonl(path)


def _check_initial(cfg, generated):
    initial_root = resolve(cfg["source_dir"]) / "initial"
    manifest_path = initial_root / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError("Initial ingestion is missing; run python -m data_simulator.simulate_initial first")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest["fingerprint"] != initial_fingerprint(cfg, generated):
        raise ValueError("Initial ingestion does not match the generated source snapshot")
    expected_sources = {source for source, filename in INITIAL_INPUTS.items()
                        if source != "knowledge_articles" or (generated / filename).exists()}
    if {item["source"] for item in manifest["files"]} != expected_sources:
        raise ValueError("Initial ingestion is incomplete")
    for item in manifest["files"]:
        if sha256(initial_root / item["path"]) != item["sha256"]:
            raise ValueError("Initial ingestion file changed")
    return manifest["fingerprint"]


def _fingerprint(cfg, generated, initial_hash):
    for source, filename in INPUTS.items():
        if source != "knowledge_articles" and not (generated / filename).exists():
            raise FileNotFoundError(generated / filename)
    inputs = {name: sha256(generated / filename) for name, filename in INPUTS.items() if (generated / filename).exists()}
    payload = {"seed": cfg["seed"], "batch": cfg["batch"], "bad_data": cfg["bad_data"],
               "initial_fingerprint": initial_hash, "inputs": inputs}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def emit(cfg, through_arrival=None):
    generated = resolve(cfg["generated_dir"])
    initial_hash = _check_initial(cfg, generated)
    root = resolve(cfg["source_dir"]) / "batch"
    root.mkdir(parents=True, exist_ok=True)
    fingerprint = _fingerprint(cfg, generated, initial_hash)
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["fingerprint"] != fingerprint:
            raise ValueError("Existing batch manifest uses different inputs or settings; choose another source_dir")
    else:
        manifest = {"ingestion_type": "batch", "fingerprint": fingerprint, "files": []}
    known = {entry["path"]: entry for entry in manifest["files"]}
    cutoff = timestamp(through_arrival) if through_arrival else None
    count = 0
    for source, filename in INPUTS.items():
        source_path = generated / filename
        if not source_path.exists():
            if source == "knowledge_articles":
                continue
            raise FileNotFoundError(source_path)
        records = _load(source_path)
        schema = None
        if source_path.suffix == ".parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq
            schema = pq.read_schema(source_path).append(pa.field("source_batch_id", pa.string())).append(pa.field("source_arrival_time", pa.string()))
        batch_count = cfg["batch"]["count"]
        for number in range(1, batch_count + 1):
            arrival = timestamp(cfg["batch"]["arrival_start"]) + timedelta(hours=(number-1)*cfg["batch"]["arrival_step_hours"])
            if cutoff and arrival > cutoff:
                continue
            subset = records[(number-1)*cfg["batch"]["size"]:number*cfg["batch"]["size"]]
            if not subset:
                continue
            extension = ".parquet" if source_path.suffix == ".parquet" else ".json"
            relative = f"{source}/batch_{number:03d}{extension}"
            destination = root / relative
            if relative in known:
                if not destination.exists() or sha256(destination) != known[relative]["sha256"]:
                    raise ValueError(f"Emitted batch changed or missing: {relative}")
                continue
            stable_seed = cfg["seed"] + sum(ord(c) for c in source) * 1000 + number
            stamped = [{**row, "source_batch_id": f"{source}-{number:03d}", "source_arrival_time": iso(arrival)} for row in subset]
            emitted, bad_counts = inject(stamped, source, cfg["bad_data"], stable_seed)
            write_immutable(destination, emitted, schema)
            entry = {"batch_id": f"{source}-{number:03d}", "source": source, "path": relative,
                     "simulated_arrival_time": iso(arrival), "row_count": len(emitted),
                     "bad_counts": bad_counts, "sha256": sha256(destination)}
            manifest["files"].append(entry)
            write_json(manifest_path, manifest)
            count += 1
    return {"new_batches": count, "total_batches": len(manifest["files"]), "manifest": str(manifest_path)}


def replay(cfg, destination, batch_ids=None):
    root = resolve(cfg["source_dir"]) / "batch"
    target = resolve(destination) / "batch"
    if root.resolve() == target.resolve() or root.resolve() in target.resolve().parents:
        raise ValueError("Replay destination must be separate from the batch source directory")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    selected = [e for e in manifest["files"] if not batch_ids or e["batch_id"] in batch_ids]
    if batch_ids and len(selected) != len(batch_ids):
        raise ValueError("Unknown batch ID in replay selection")
    for entry in selected:
        source = root / entry["path"]
        if sha256(source) != entry["sha256"]:
            raise ValueError(f"Source batch changed: {source}")
        output = target / entry["path"]
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            if sha256(output) != entry["sha256"]:
                raise FileExistsError(f"Replay collision: {output}")
        else:
            shutil.copyfile(source, output)
    write_json(target / "manifest.json", {"ingestion_type": "batch", "source_fingerprint": manifest["fingerprint"], "files": selected})
    return {"replayed_batches": len(selected), "destination": str(target)}


def run(cfg, through_arrival=None, prepare=False, upload_files=False, dry_run_upload=False,
        kaggle_csv=None, volume_path=None, volume_client=None):
    result = {}
    if prepare:
        result["generated"] = create(cfg, kaggle_csv)
        if not cfg["knowledge"]["skip"]:
            result["knowledge"] = generate_knowledge(cfg, model=cfg["knowledge"]["model"])
    result["batch"] = emit(cfg, through_arrival)
    if upload_files:
        result["upload"] = upload(cfg, volume_path, client=volume_client,
                                  ingestion_type="batch", dry_run=dry_run_upload)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument("--replay-dir")
    parser.add_argument("--through-arrival", help="Emit batches scheduled through this ISO-8601 time")
    parser.add_argument("--batch-id", action="append", help="Limit replay to selected stable batch IDs")
    parser.add_argument("--prepare", action="store_true", help="Generate source data before emitting files")
    parser.add_argument("--kaggle-csv", help="Local Kaggle ticket CSV; use with --prepare")
    parser.add_argument("--upload", action="store_true", help="Upload emitted batches to the configured Volume")
    parser.add_argument("--dry-run-upload", action="store_true", help="Preview the Volume upload without connecting")
    parser.add_argument("--volume-path", help="Override DATABRICKS_VOLUME_PATH")
    args = parser.parse_args()
    if args.kaggle_csv and not args.prepare:
        parser.error("--kaggle-csv requires --prepare")
    if args.dry_run_upload and not args.upload:
        parser.error("--dry-run-upload requires --upload")
    if args.replay_dir and (args.prepare or args.upload or args.through_arrival):
        parser.error("--replay-dir cannot be combined with --prepare, --upload, or --through-arrival")
    cfg = config(args.config)
    print(replay(cfg, args.replay_dir, args.batch_id) if args.replay_dir else
          run(cfg, args.through_arrival, args.prepare, args.upload, args.dry_run_upload,
              args.kaggle_csv, args.volume_path))


if __name__ == "__main__":
    main()
