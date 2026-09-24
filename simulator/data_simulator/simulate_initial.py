"""Emit immutable historical snapshots for the initial ingestion."""
import argparse
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq

from .common import config, iso, read_jsonl, read_parquet, resolve, sha256, timestamp, write_immutable, write_json
from .generate_data import create
from .generate_knowledge import generate as generate_knowledge
from .upload_volume import upload


INPUTS = {"customers": "customers.parquet", "products": "products.parquet",
          "subscriptions": "subscriptions.parquet", "payments": "payments.parquet",
          "tickets": "tickets.jsonl", "incidents": "incidents.json",
          "knowledge_articles": "knowledge_articles_initial.jsonl"}


def fingerprint(cfg, generated):
    for source, filename in INPUTS.items():
        if source != "knowledge_articles" and not (generated / filename).exists():
            raise FileNotFoundError(generated / filename)
    hashes = {source: sha256(generated / filename) for source, filename in INPUTS.items()
              if (generated / filename).exists()}
    payload = {"seed": cfg["seed"], "initial": cfg["initial"], "inputs": hashes}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def emit(cfg):
    generated = resolve(cfg["generated_dir"])
    root = resolve(cfg["source_dir"]) / "initial"
    root.mkdir(parents=True, exist_ok=True)
    current_fingerprint = fingerprint(cfg, generated)
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["fingerprint"] != current_fingerprint:
            raise ValueError("Initial snapshot inputs changed; choose another source_dir")
    else:
        manifest = {"ingestion_type": "initial", "snapshot_id": "initial-001",
                    "fingerprint": current_fingerprint, "files": []}
    known = {item["path"]: item for item in manifest["files"]}
    arrival = iso(timestamp(cfg["initial"]["arrival_time"]))
    new_files = 0
    for source, filename in INPUTS.items():
        source_path = generated / filename
        if not source_path.exists():
            if source == "knowledge_articles":
                continue
            raise FileNotFoundError(source_path)
        extension = ".parquet" if source_path.suffix == ".parquet" else ".json"
        relative = f"{source}/snapshot_001{extension}"
        destination = root / relative
        if relative in known:
            if not destination.exists() or sha256(destination) != known[relative]["sha256"]:
                raise ValueError(f"Initial file changed or missing: {relative}")
            continue
        if source_path.suffix == ".parquet":
            records = read_parquet(source_path)
            schema = (pq.read_schema(source_path)
                      .append(pa.field("source_snapshot_id", pa.string()))
                      .append(pa.field("source_arrival_time", pa.string())))
        elif source == "incidents":
            records = json.loads(source_path.read_text(encoding="utf-8"))
            schema = None
        else:
            records = read_jsonl(source_path)
            schema = None
        rows = [{**row, "source_snapshot_id": "initial-001", "source_arrival_time": arrival}
                for row in records]
        write_immutable(destination, rows, schema)
        manifest["files"].append({"snapshot_id": "initial-001", "source": source,
                                  "path": relative, "row_count": len(rows),
                                  "simulated_arrival_time": arrival, "sha256": sha256(destination)})
        write_json(manifest_path, manifest)
        new_files += 1
    return {"new_files": new_files, "total_files": len(manifest["files"]), "manifest": str(manifest_path)}


def run(cfg, prepare=False, upload_files=False, dry_run_upload=False, kaggle_csv=None,
        volume_path=None, volume_client=None):
    result = {}
    if prepare:
        result["generated"] = create(cfg, kaggle_csv)
        if not cfg["knowledge"]["skip"]:
            result["knowledge"] = generate_knowledge(cfg, model=cfg["knowledge"]["model"])
    result["initial"] = emit(cfg)
    if upload_files:
        result["upload"] = upload(cfg, volume_path, client=volume_client,
                                  ingestion_type="initial", dry_run=dry_run_upload)
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--prepare", action="store_true", help="Generate source data before emitting files")
    p.add_argument("--kaggle-csv", help="Local Kaggle ticket CSV; use with --prepare")
    p.add_argument("--upload", action="store_true", help="Upload emitted files to the configured Volume")
    p.add_argument("--dry-run-upload", action="store_true", help="Preview the Volume upload without connecting")
    p.add_argument("--volume-path", help="Override DATABRICKS_VOLUME_PATH")
    args = p.parse_args()
    if args.kaggle_csv and not args.prepare:
        p.error("--kaggle-csv requires --prepare")
    if args.dry_run_upload and not args.upload:
        p.error("--dry-run-upload requires --upload")
    print(run(config(args.config), args.prepare, args.upload, args.dry_run_upload,
              args.kaggle_csv, args.volume_path))


if __name__ == "__main__":
    main()
