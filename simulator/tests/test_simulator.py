import csv
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_simulator.common import config, read_jsonl, read_parquet, sha256, timestamp
from data_simulator.generate_data import create
from data_simulator.generate_knowledge import generate, generate_one
from data_simulator.kafka import connection_options
from data_simulator.simulate_batch import emit, inject, replay
from data_simulator.simulate_batch import run as run_batch
from data_simulator.simulate_initial import emit as emit_initial
from data_simulator.simulate_initial import run as run_initial
from data_simulator.simulate_stream import context, event_at, publish
from data_simulator.simulate_stream import run as run_stream
from data_simulator.upload_volume import upload


@pytest.fixture
def sample(tmp_path):
    cfg = config()
    cfg["generated_dir"] = str(tmp_path / "generated")
    cfg["source_dir"] = str(tmp_path / "source")
    cfg["customers"] = 12
    cfg["tickets"] = 24
    cfg["batch"]["size"] = 8
    cfg["batch"]["count"] = 3
    cfg["bad_data"] = {key: 0 for key in cfg["bad_data"]}
    return cfg


def test_relationships_incidents_and_seed(sample, tmp_path):
    seed_file = tmp_path / "tickets.csv"
    with seed_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Ticket ID", "Ticket Subject", "Ticket Type"])
        writer.writeheader()
        writer.writerow({"Ticket ID": "123", "Ticket Subject": "Sign-in issue", "Ticket Type": "Technical issue"})
    result = create(sample, str(seed_file))
    assert result["kaggle_seed_rows"] == 1
    root = tmp_path / "generated"
    customers = {r["customer_id"]: r for r in read_parquet(root / "customers.parquet")}
    products = {r["product_id"] for r in read_parquet(root / "products.parquet")}
    subscriptions = {r["subscription_id"]: r for r in read_parquet(root / "subscriptions.parquet")}
    payments = read_parquet(root / "payments.parquet")
    tickets = read_jsonl(root / "tickets.jsonl")
    batch_customers = read_parquet(root / "batch_customers.parquet")
    batch_products = read_parquet(root / "batch_products.parquet")
    batch_subscriptions = read_parquet(root / "batch_subscriptions.parquet")
    batch_payments = read_parquet(root / "batch_payments.parquet")
    batch_tickets = read_jsonl(root / "batch_tickets.jsonl")
    batch_incidents = read_jsonl(root / "batch_incidents.jsonl")
    incidents = {r["incident_id"]: r for r in json.loads((root / "incidents.json").read_text())}
    assert len(incidents) == 3
    assert all(s["customer_id"] in customers and s["product_id"] in products for s in subscriptions.values())
    assert all(p["subscription_id"] in subscriptions and p["customer_id"] == subscriptions[p["subscription_id"]]["customer_id"] for p in payments)
    assert all(t["subscription_id"] in subscriptions and t["customer_id"] == subscriptions[t["subscription_id"]]["customer_id"] for t in tickets)
    assert {t["incident_id"] for t in tickets if t["incident_id"]} == set(incidents)
    assert all(t["source_seed"] == "kaggle" for t in tickets)
    assert any(p["status"] == "failed" and p["incident_id"] == "INC-001" for p in payments)
    payment_by_id = {p["payment_id"]: p for p in payments}
    assert all(p["retry_of_payment_id"] in payment_by_id and payment_by_id[p["retry_of_payment_id"]]["status"] == "failed"
               for p in payments if p["retry_of_payment_id"])
    all_customers = customers | {r["customer_id"]: r for r in batch_customers}
    all_products = products | {r["product_id"] for r in batch_products}
    all_subscriptions = subscriptions | {r["subscription_id"]: r for r in batch_subscriptions}
    all_incidents = incidents | {r["incident_id"]: r for r in batch_incidents}
    all_ticket_ids = {t["ticket_id"] for t in tickets + batch_tickets}
    assert len(batch_customers) == 2 and len(batch_products) == 1 and len(batch_subscriptions) == 4
    assert len(batch_payments) == 60 and len(batch_tickets) == 60 and len(batch_incidents) == 1
    assert all(s["customer_id"] in all_customers and s["product_id"] in all_products for s in batch_subscriptions)
    assert all(p["subscription_id"] in all_subscriptions and p["customer_id"] == all_subscriptions[p["subscription_id"]]["customer_id"]
               and (p["incident_id"] is None or p["incident_id"] in all_incidents) for p in batch_payments)
    assert all(t["subscription_id"] in all_subscriptions and t["product_id"] == all_subscriptions[t["subscription_id"]]["product_id"]
               and (t["incident_id"] is None or t["incident_id"] in all_incidents) for t in batch_tickets)
    assert any(p["incident_id"] == "INC-004" and p["status"] == "failed" for p in batch_payments)
    assert any(t["incident_id"] == "INC-004" for t in batch_tickets)
    initial_articles = read_jsonl(root / "knowledge_metadata_initial.jsonl")
    batch_articles = read_jsonl(root / "knowledge_metadata_batch.jsonl")
    assert len(initial_articles) == 3 and len(batch_articles) == 5
    assert any(a["article_id"] == initial_articles[1]["article_id"] and a["article_version"] == 2 for a in batch_articles)
    assert all(a["product_id"] in all_products and
               (a["incident_id"] is None or a["incident_id"] in all_incidents) and
               set(a["related_ticket_ids"]) <= all_ticket_ids for a in initial_articles + batch_articles)


def test_batches_resume_replay_immutable(sample, tmp_path):
    create(sample)
    with pytest.raises(FileNotFoundError, match="Initial ingestion is missing"):
        emit(sample)
    initial = emit_initial(sample)
    assert initial["new_files"] == 6
    initial_root = tmp_path / "source" / "initial"
    initial_manifest = json.loads((initial_root / "manifest.json").read_text())
    assert {e["source"] for e in initial_manifest["files"]} == {
        "customers", "products", "subscriptions", "payments", "tickets", "incidents"}
    initial_hashes = {e["path"]: sha256(initial_root / e["path"]) for e in initial_manifest["files"]}
    assert emit_initial(sample)["new_files"] == 0
    assert initial_hashes == {e["path"]: sha256(initial_root / e["path"]) for e in initial_manifest["files"]}
    staged = emit(sample, sample["batch"]["arrival_start"])
    assert staged["new_batches"] == 6
    first = emit(sample)
    assert first["new_batches"] >= 4
    root = tmp_path / "source" / "batch"
    manifest = json.loads((root / "manifest.json").read_text())
    assert len({e["batch_id"] for e in manifest["files"]}) == len(manifest["files"])
    assert len([e for e in manifest["files"] if e["source"] == "tickets"]) == 3
    assert len([e for e in manifest["files"] if e["source"] == "incidents"]) == 1
    assert {e["source"] for e in manifest["files"]} == {
        "customers", "products", "subscriptions", "payments", "tickets", "incidents"}
    for entry in manifest["files"]:
        path = root / entry["path"]
        rows = read_parquet(path) if path.suffix == ".parquet" else read_jsonl(path)
        assert all(timestamp(row["source_change_time"]) <= timestamp(entry["simulated_arrival_time"])
                   for row in rows)
    hashes = {e["path"]: sha256(root / e["path"]) for e in manifest["files"]}
    assert emit(sample)["new_batches"] == 0
    assert hashes == {e["path"]: sha256(root / e["path"]) for e in manifest["files"]}
    replay_result = replay(sample, str(tmp_path / "replay"), ["tickets-001", "payments-001"])
    assert replay_result["replayed_batches"] == 2
    for name in ("tickets/batch_001.json", "payments/batch_001.parquet"):
        assert sha256(root / name) == sha256(tmp_path / "replay" / "batch" / name)
    with pytest.raises(ValueError, match="different inputs"):
        changed = {**sample, "batch": {**sample["batch"], "size": 7}}
        emit(changed)


def test_bad_data_is_configurable(sample, tmp_path):
    create(sample)
    sample["bad_data"]["negative_payment_rate"] = 1
    sample["bad_data"]["duplicate_rate"] = 1
    emit_initial(sample)
    historical_payments = read_parquet(tmp_path / "source" / "initial" / "payments" / "snapshot_001.parquet")
    assert all(row["amount"] > 0 for row in historical_payments)
    assert len({row["payment_id"] for row in historical_payments}) == len(historical_payments)
    emit(sample)
    payments = read_parquet(tmp_path / "source" / "batch" / "payments" / "batch_001.parquet")
    assert len(payments) == 16
    assert all(row["amount"] < 0 for row in payments)
    assert len({row["payment_id"] for row in payments}) == 8
    broken, counts = inject([{"payment_id": "PAY-1", "customer_id": "CUST-1", "amount": 10,
                               "attempted_at": "2026-09-01T00:00:00+00:00", "status": "succeeded"}],
                             "payments", {key: 1 for key in sample["bad_data"]}, 1)
    assert len(broken) == 2
    assert broken[0]["payment_id"] is None
    assert broken[0]["customer_id"] == "CUST-DOES-NOT-EXIST"
    assert broken[0]["attempted_at"] == "not-a-timestamp"
    assert broken[0]["status"] == "INVALID_VALUE"
    assert broken[0]["amount"] == -10
    assert all(value == 1 for value in counts.values())


class FakeProducer:
    def __init__(self):
        self.sent = []
        self.flushed = 0

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed += 1


def test_single_command_ingestion_workflows_without_services(sample, tmp_path):
    volume = "/Volumes/catalog/schema/volume/cloudflow"
    initial = run_initial(sample, prepare=True, upload_files=True,
                          dry_run_upload=True, volume_path=volume)
    assert initial["initial"]["new_files"] == 6
    assert initial["upload"]["planned"] == {"initial": 6}
    batch = run_batch(sample, through_arrival=sample["batch"]["arrival_start"],
                      prepare=True, upload_files=True, dry_run_upload=True, volume_path=volume)
    assert batch["batch"]["new_batches"] == 6
    assert batch["upload"]["planned"] == {"batch": 6}
    fake = FakeProducer()
    streamed = run_stream(sample, 4, prepare=True, client=fake, sleep=lambda _: None,
                          state_file=str(tmp_path / "stream_state.json"),
                          pause_file=str(tmp_path / "stream.pause"))
    assert streamed["logical_events"] == 4
    assert fake.sent and all(topic == sample["stream"]["topic"] and key == event["customer_id"]
                             for topic, key, event in fake.sent)


def test_stream_fields_incidents_and_resume(sample, tmp_path):
    create(sample)
    sample["stream"]["duplicate_rate"] = 1
    fake = FakeProducer()
    state = str(tmp_path / "checkpoint.json")
    pause = str(tmp_path / "pause")
    first = publish(sample, 12, state_file=state, pause_file=pause, client=fake, sleep=lambda _: None)
    assert first["logical_events"] == 12
    assert first["published_messages"] == 24
    assert fake.flushed == 2
    assert all(key == value["customer_id"] for _, key, value in fake.sent)
    assert all({"event_id", "event_time", "arrival_time", "customer_id", "product_id", "service", "event_type", "error_code", "http_status", "latency_ms", "region"} <= value.keys() for _, _, value in fake.sent)
    assert all(value["event_type"] in {
        "api": {"LOGIN", "API_REQUEST", "EXPORT", "ERROR", "TIMEOUT"},
        "billing": {"LOGIN", "PAYMENT_ATTEMPT", "ERROR", "TIMEOUT"},
        "storage": {"LOGIN", "FILE_UPLOAD", "FILE_DOWNLOAD", "EXPORT", "ERROR", "TIMEOUT"},
    }[value["service"]] for _, _, value in fake.sent)
    assert any(value["incident_id"] for _, _, value in fake.sent)
    assert {value["incident_id"] for _, _, value in fake.sent if value["incident_id"]} >= {"INC-001", "INC-002"}
    with pytest.raises(ValueError, match="Checkpoint exists"):
        publish(sample, 1, state_file=state, pause_file=pause, client=fake, sleep=lambda _: None)
    resumed = publish(sample, 3, resume=True, state_file=state, pause_file=pause, client=fake, sleep=lambda _: None)
    assert resumed["next_index"] == 15
    customers, subs, incidents, _ = context(sample)
    assert event_at(0, sample, customers, subs, incidents) == event_at(0, sample, customers, subs, incidents)
    delayed_cfg = {**sample, "stream": {**sample["stream"], "late_rate": 1, "out_of_order_rate": 1}}
    delayed = event_at(20, delayed_cfg, customers, subs, incidents)
    assert (timestamp(delayed["arrival_time"]) - timestamp(delayed["event_time"])).total_seconds() > 7200
    pause_path = tmp_path / "pause"
    pause_path.write_text("paused")
    waits = []
    def release_pause(seconds):
        waits.append(seconds)
        pause_path.unlink()
    publish(sample, 1, state_file=str(tmp_path / "new-checkpoint.json"), pause_file=str(pause_path),
            client=fake, sleep=release_pause)
    assert waits == [.25]


def test_knowledge_validation_retry_cache_and_volume_mock(sample, tmp_path):
    create(sample)
    metadata = read_jsonl(tmp_path / "generated" / "knowledge_metadata_initial.jsonl")[0]
    valid = {"explanation_summary": "A service disruption can prevent successful payment completion. Agents should confirm the service status, scope, and timing before recommending any account change.",
             "symptoms": ["Payment fails", "Checkout is delayed"], "possible_causes": ["Gateway latency", "Upstream availability"],
             "troubleshooting_steps": ["Check service status", "Confirm the affected time window", "Review recent attempts"],
             "workaround": "Wait for the affected service to recover before retrying the payment.",
             "resolution": "Confirm recovery and reconcile the payment attempt before closing the case.",
             "prevention": ["Monitor upstream latency", "Review alert thresholds"],
             "agent_hints": ["Check the incident window", "Avoid duplicate charges"],
             "related_questions": ["Was my payment captured?", "When should I retry?"],
             "search_terms": ["payment", "gateway", "timeout"]}
    class Responses:
        calls = 0
        def parse(self, **kwargs):
            self.calls += 1
            body = dict(valid)
            if self.calls == 1:
                body["resolution"] = "Use PRD-999 to fix the issue immediately."
            return SimpleNamespace(output_parsed=body)
    client = SimpleNamespace(responses=Responses())
    first = generate_one(metadata, "gpt-5.6-luna", tmp_path / "generated" / "knowledge_cache", client, sleep=lambda _: None)
    assert first["product_id"] == metadata["product_id"]
    assert client.responses.calls == 2
    again = generate_one(metadata, "gpt-5.6-luna", tmp_path / "generated" / "knowledge_cache", client)
    assert again == first and client.responses.calls == 2
    generated = generate(sample, client=client)
    assert generated["outputs"]["initial"]["articles"] == 3
    assert generated["outputs"]["batch"]["articles"] == 5
    emit_initial(sample)
    emit(sample)
    class Files:
        def __init__(self):
            self.uploaded = []
            self.remote = {}
        def create_directory(self, path):
            pass
        def get_metadata(self, remote):
            from databricks.sdk.errors import NotFound
            if remote not in self.remote:
                raise NotFound("missing")
            return SimpleNamespace(content_length=len(self.remote[remote]))
        def download(self, remote):
            return SimpleNamespace(contents=io.BytesIO(self.remote[remote]))
        def upload_from(self, remote, local, overwrite, use_parallel):
            assert overwrite is False
            assert use_parallel is False
            assert remote not in self.remote
            self.uploaded.append(remote)
            self.remote[remote] = Path(local).read_bytes()
    files = Files()
    destination = "/Volumes/catalog/schema/volume/drop"
    planned = upload(sample, destination, ingestion_type="all", dry_run=True)
    assert len(planned["files"]) >= 14
    result = upload(sample, destination, SimpleNamespace(files=files))
    assert sum(result["uploaded"].values()) == len(files.uploaded)
    assert any("initial/knowledge_articles/snapshot_001.json" in path for path in files.uploaded)
    assert any("batch/knowledge_articles/batch_001.json" in path for path in files.uploaded)
    assert any("tickets/batch_001.json" in path for path in files.uploaded)
    resumed = upload(sample, destination, SimpleNamespace(files=files))
    assert sum(resumed["uploaded"].values()) == 0
    assert sum(resumed["skipped"].values()) == len(files.uploaded)
    files.remote[files.uploaded[0]] = b"different file"
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        upload(sample, destination, SimpleNamespace(files=files))


def test_aiven_sasl_settings_without_connection(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("test CA")
    monkeypatch.setenv("AIVEN_KAFKA_SERVICE_URI", "broker.example.test:9093")
    monkeypatch.setenv("AIVEN_KAFKA_AUTH_METHOD", "sasl")
    monkeypatch.setenv("AIVEN_KAFKA_CA_CERT_PATH", str(ca))
    monkeypatch.setenv("AIVEN_KAFKA_SASL_USERNAME", "sample-user")
    monkeypatch.setenv("AIVEN_KAFKA_SASL_PASSWORD", "sample-password")
    options = connection_options()
    assert options == {
        "bootstrap_servers": "broker.example.test:9093",
        "security_protocol": "SASL_SSL",
        "ssl_cafile": str(ca),
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": "sample-user",
        "sasl_plain_password": "sample-password",
    }
    # An empty exported value prevents the project's .env from refilling it.
    monkeypatch.setenv("AIVEN_KAFKA_SASL_PASSWORD", "")
    with pytest.raises(ValueError, match="AIVEN_KAFKA_SASL_PASSWORD"):
        connection_options()


def test_aiven_certificate_settings_without_connection(monkeypatch, tmp_path):
    paths = {}
    for variable, filename in (
        ("AIVEN_KAFKA_CA_CERT_PATH", "ca.pem"),
        ("AIVEN_KAFKA_SERVICE_CERT_PATH", "service.cert"),
        ("AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH", "service.key"),
    ):
        path = tmp_path / filename
        path.write_text("test certificate")
        monkeypatch.setenv(variable, str(path))
        paths[variable] = str(path)
    monkeypatch.setenv("AIVEN_KAFKA_AUTH_METHOD", "client_certificate")
    options = connection_options("broker.example.test:17072")
    assert options == {
        "bootstrap_servers": "broker.example.test:17072",
        "security_protocol": "SSL",
        "ssl_cafile": paths["AIVEN_KAFKA_CA_CERT_PATH"],
        "ssl_certfile": paths["AIVEN_KAFKA_SERVICE_CERT_PATH"],
        "ssl_keyfile": paths["AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH"],
    }
    monkeypatch.setenv("AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH", str(tmp_path / "missing.key"))
    with pytest.raises(FileNotFoundError, match="AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH"):
        connection_options("broker.example.test:17072")
