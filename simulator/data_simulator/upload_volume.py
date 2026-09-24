"""Copy emitted initial and batch source files to an existing Unity Catalog Volume."""
import argparse
import hashlib
import json
import os
from pathlib import PurePosixPath

from .common import config, load_environment, resolve, sha256


def _volume_path(value):
    if not value:
        raise ValueError("Set DATABRICKS_VOLUME_PATH to /Volumes/<catalog>/<schema>/<volume>/<folder>")
    path = value.replace("\\", "/").rstrip("/")
    parts = path.split("/")
    if len(parts) < 5 or parts[:2] != ["", "Volumes"] or any(
        part in ("", ".", "..") for part in parts[2:]
    ):
        raise ValueError("Destination must be /Volumes/<catalog>/<schema>/<volume>[/folder]")
    return path


def _plan(cfg, volume_path, ingestion_type):
    if ingestion_type not in ("initial", "batch", "all"):
        raise ValueError("ingestion_type must be initial, batch, or all")
    root = resolve(cfg["source_dir"])
    kinds = ("initial", "batch") if ingestion_type == "all" else (ingestion_type,)
    files = []
    for kind in kinds:
        base = root / kind
        manifest_path = base / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Run {kind} ingestion before uploading: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("ingestion_type") != kind:
            raise ValueError(f"Unexpected ingestion type in {manifest_path}")
        for entry in manifest["files"]:
            relative = PurePosixPath(entry["path"].replace("\\", "/"))
            if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
                raise ValueError(f"Unsafe path in {manifest_path}: {entry['path']}")
            local = base.joinpath(*relative.parts)
            if not local.is_file() or sha256(local) != entry["sha256"]:
                raise ValueError(f"Local file missing or changed: {local}")
            files.append((kind, local, f"{volume_path}/{kind}/{relative.as_posix()}", entry["sha256"]))
    return kinds, files


def _remote_matches(client, remote, local, expected_hash):
    metadata = client.files.get_metadata(remote)
    if metadata.content_length is not None and metadata.content_length != local.stat().st_size:
        return False
    response = client.files.download(remote)
    if response.contents is None:
        raise RuntimeError(f"Databricks returned no file content for {remote}")
    digest = hashlib.sha256()
    with response.contents as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest() == expected_hash


def upload(cfg, volume_path=None, client=None, ingestion_type="all", dry_run=False):
    """Upload immutable drops; existing identical files are skipped, never overwritten."""
    load_environment()
    normalized = _volume_path(volume_path or os.getenv("DATABRICKS_VOLUME_PATH"))
    kinds, files = _plan(cfg, normalized, ingestion_type)
    if dry_run:
        return {"volume_path": normalized, "planned": {kind: sum(item[0] == kind for item in files)
                                                     for kind in kinds},
                "files": [item[2] for item in files]}
    if any(part.startswith("YOUR_") for part in normalized.split("/")[2:5]):
        raise ValueError("Replace the DATABRICKS_VOLUME_PATH placeholders before uploading")
    if client is None:
        host = os.getenv("DATABRICKS_HOST", "").strip()
        token = os.getenv("DATABRICKS_TOKEN", "").strip()
        if (not host or not token or host == "https://YOUR-WORKSPACE-URL"
                or token == "YOUR-PERSONAL-ACCESS-TOKEN"):
            raise ValueError("Replace DATABRICKS_HOST and DATABRICKS_TOKEN placeholders in .env before uploading")
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient(host=host, token=token)
    from databricks.sdk.errors import NotFound, ResourceAlreadyExists
    uploaded = {kind: 0 for kind in kinds}
    skipped = {kind: 0 for kind in kinds}
    for kind, local, remote, expected_hash in files:
        try:
            exists = client.files.get_metadata(remote)
        except NotFound:
            exists = None
        if exists is not None:
            if not _remote_matches(client, remote, local, expected_hash):
                raise FileExistsError(f"Remote file differs; refusing to overwrite: {remote}")
            skipped[kind] += 1
            continue
        client.files.create_directory(remote.rsplit("/", 1)[0])
        try:
            client.files.upload_from(remote, str(local), overwrite=False, use_parallel=False)
            uploaded[kind] += 1
        except ResourceAlreadyExists:
            # Another process may have created the file after our metadata check.
            if not _remote_matches(client, remote, local, expected_hash):
                raise FileExistsError(f"Remote file differs; refusing to overwrite: {remote}")
            skipped[kind] += 1
    return {"volume_path": normalized, "uploaded": uploaded, "skipped": skipped}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("volume_path", nargs="?", help="Defaults to DATABRICKS_VOLUME_PATH in .env")
    p.add_argument("--config")
    p.add_argument("--ingestion-type", choices=["initial", "batch", "all"], default="all")
    p.add_argument("--dry-run", action="store_true", help="Check local files and show remote paths without connecting")
    args = p.parse_args()
    print(json.dumps(upload(config(args.config), args.volume_path,
                            ingestion_type=args.ingestion_type, dry_run=args.dry_run), indent=2))


if __name__ == "__main__":
    main()
