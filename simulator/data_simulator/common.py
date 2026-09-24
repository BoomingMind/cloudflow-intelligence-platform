import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]


def load_environment():
    """Load a local .env if present; exported environment variables take precedence."""
    load_dotenv(ROOT / ".env", override=False)


def config(path=None):
    load_environment()
    with open(path or ROOT / "data_simulator" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve(path):
    path = Path(path)
    return path if path.is_absolute() else ROOT / path


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat()


def read_jsonl(path):
    if not Path(path).exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def read_parquet(path):
    import pyarrow.parquet as pq
    return pq.read_table(path).to_pylist()


def write_parquet(path, rows, schema=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")


def write_immutable(path, rows, schema=None):
    path = Path(path)
    temp = path.with_name(path.name + f".{os.getpid()}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if path.suffix == ".parquet":
            write_parquet(temp, rows, schema)
        else:
            write_jsonl(temp, rows)
        try:
            os.link(temp, path)
        except FileExistsError:
            # Recover from a crash after file creation but before manifest update.
            if sha256(temp) != sha256(path):
                raise ValueError(f"Existing emitted file differs: {path}")
    finally:
        temp.unlink(missing_ok=True)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    temp = Path(path).with_name(Path(path).name + f".{os.getpid()}.tmp")
    try:
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(value, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
