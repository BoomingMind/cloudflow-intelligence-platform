# CloudFlow Data Ingestion Simulator

A local source simulator for a future customer-support lakehouse. It prepares three distinct ingestion paths. It does not build Auto Loader, Structured Streaming, tables, or a Databricks lakehouse.

| Data | Initial ingestion | Batch ingestion | Streaming ingestion |
| --- | --- | --- | --- |
| Customers | Historical snapshot | New accounts and tier changes | — |
| Products | Historical catalog | Price changes | — |
| Subscriptions | Historical snapshot | New, changed, and cancelled subscriptions | — |
| Payments | Historical ledger | New attempts and retries | — |
| Support tickets | Historical tickets | New tickets and status changes | — |
| Incidents | Historical incidents | New incidents | — |
| Knowledge articles | Published product guides | New incident guides and revised articles | — |
| Application events | — | — | Kafka only |

The exact ingestion entry points are:

- `data_simulator/simulate_initial.py` — writes one immutable historical snapshot per source.
- `data_simulator/simulate_batch.py` — releases immutable incremental files by simulated arrival time; supports resume and replay.
- `data_simulator/simulate_stream.py` — publishes application events to Kafka with `customer_id` as the key.

`generate_data.py` prepares deterministic, related records for those entry points. `generate_knowledge.py` optionally adds OpenAI-written article content. Kafka is used only for application events.

## Quick start

Python 3.10+ is required. From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
python -m data_simulator.generate_data
python -m data_simulator.generate_knowledge --skip
python -m data_simulator.simulate_initial
python -m data_simulator.simulate_batch --through-arrival 2026-10-04T09:00:00+00:00
python -m data_simulator.simulate_batch --through-arrival 2026-10-05T09:00:00+00:00
python -m data_simulator.simulate_batch
python -m pytest -q
```

The default development seed creates 40 historical customers, three products, 103 subscriptions, 266 payments, 90 tickets, and three historical incidents. Later records include a new customer, product price change, subscription changes, 60 payments, 60 ticket changes, and another payment incident. Three baseline article definitions and five later article definitions are prepared. Knowledge content files are present only after successful OpenAI generation.

## Windows one-command ingestion

After filling the Databricks settings in `.env` and creating the target Volume, run the launchers from this folder:

```powershell
.\simulate_initial.bat
.\simulate_batch.bat --through-arrival 2026-10-04T09:00:00+00:00
.\simulate_batch.bat --through-arrival 2026-10-05T09:00:00+00:00
.\simulate_stream.bat --max-events 30
```

`simulate_inital.bat` is an alias for `simulate_initial.bat` to match the alternate spelling. The initial launcher generates deterministic source data, emits the initial snapshots, then uploads them. The batch launcher regenerates the same deterministic inputs, emits batches due by the chosen arrival time, then uploads the local batch files. Run initial before batch. Without `--through-arrival`, batch emits all remaining configured batches. Repeated uploads skip identical files and refuse changed remote files.

The stream launcher regenerates the same inputs and publishes **application events only** to Aiven Kafka; it does not upload files. Pass `--resume` to continue a prior stream checkpoint. All launchers forward additional command-line options to their matching `simulate_*.py` entry point. If you use a Kaggle CSV, set `kaggle_csv` in `data_simulator/config.yaml` so every launcher uses the same seed.

Preview file generation and upload paths without Databricks credentials or a connection:

```powershell
.\simulate_initial.bat --dry-run-upload
.\simulate_batch.bat --dry-run-upload
```

These dry runs still create local files. There is no dry-run mode for Kafka publishing; use the mocked test suite for offline stream checks.

## Output layout

```text
generated_data_v2/                 # clean deterministic inputs
  customers.parquet               # initial input
  payments.parquet
  tickets.jsonl
  knowledge_metadata_initial.jsonl
  batch_customers.parquet         # later source changes
  batch_payments.parquet
  batch_tickets.jsonl
  knowledge_metadata_batch.jsonl
  ...
ingestion_data/
  initial/
    manifest.json
    customers/snapshot_001.parquet
    products/snapshot_001.parquet
    subscriptions/snapshot_001.parquet
    payments/snapshot_001.parquet
    tickets/snapshot_001.json
    incidents/snapshot_001.json
    knowledge_articles/snapshot_001.json  # when generated
  batch/
    manifest.json
    customers/batch_001.parquet
    products/batch_001.parquet
    subscriptions/batch_001.parquet
    payments/batch_001.parquet
    payments/batch_002.parquet
    tickets/batch_001.json
    incidents/batch_001.json
    knowledge_articles/batch_001.json     # when generated
    ...
```

JSON batch and snapshot files contain one JSON object per line. The initial manifest records snapshot IDs, row counts, arrival time, and hashes. The batch manifest records stable batch IDs, arrival times, bad-record counts, row counts, and hashes. Batch changes carry `change_type`, `record_version`, and `source_change_time`. A repeated entity ID with a higher version is an update, while payments remain append-only attempts. Initial snapshots are clean; configurable bad records are injected only into batch drops.

Run `simulate_initial.py` before `simulate_batch.py`. The batch command checks that the complete initial snapshot matches the generated source data. `--through-arrival` releases only batches scheduled through that UTC time; running again with a later cutoff releases more. Running without a cutoff emits all remaining files. Existing emitted files are verified and never rewritten.

Replay selected batch files to a separate drop:

```powershell
python -m data_simulator.simulate_batch --replay-dir replay_data --batch-id tickets-001 --batch-id payments-001
```

Replayed files appear under `replay_data/batch/` with identical hashes. Change `source_dir` in a copied config file for a genuinely new run with different seeds or settings.

## Kaggle support-ticket seed

The [Customer Support Ticket Dataset](https://www.kaggle.com/datasets/suraj520/customer-support-ticket-dataset) is the preferred historical ticket seed. Download the CSV locally and run:

```powershell
python -m data_simulator.generate_data --kaggle-csv .\customer_support_tickets.csv
```

Source ticket IDs, subjects, types, and descriptions are retained as seed fields. CloudFlow customer, subscription, product, and incident relationships are generated by Python. Source customer names and emails are not imported. Without the CSV, historical and later tickets are synthetic.

## Knowledge articles in both paths

`generate_data.py` creates three baseline product-guide definitions and five later article definitions, including an updated guide version. Python owns article IDs, versions, products, error codes, and ticket and incident links. OpenAI `gpt-5.6-luna` generates the summary, symptoms, causes, troubleshooting, workaround, resolution, prevention, agent hints, questions, and search terms. Structured output is validated, retried, and cached; prose containing invented IDs or error codes is rejected.

To include detailed article content in the one-command launchers, set `knowledge.skip: false` in `data_simulator/config.yaml` and provide a valid `OPENAI_API_KEY` **before the first initial ingestion**. The initial and batch launchers then generate both initial and batch articles automatically and use the cache on repeated runs. You can also run article generation directly:

```powershell
python -m data_simulator.generate_knowledge --ingestion-type all
```

It writes `generated_data_v2/knowledge_articles_initial.jsonl` and `generated_data_v2/knowledge_articles_batch.jsonl`. For local development without OpenAI, use `--skip`; the ingestion scripts then omit article files while keeping the metadata definitions in `generated_data_v2/`. A focused model check can use `--ingestion-type initial --limit 1 --model MODEL --output generated_data_v2/knowledge_model_check.jsonl`.

Generate article content before the initial snapshot. If you have already emitted files without articles, set a **new** `source_dir` in a copied config and run both ingestion commands there. Emitted snapshots and batches are immutable.

## Streaming with Aiven Kafka

In the [Aiven Console Quick connect for Python](https://aiven.io/docs/products/kafka/howto/connect-with-python), select or create the `application-events` topic and choose **SASL** (recommended) or **Client certificate**. Copy `.env.example` to `.env`; the scripts load `.env` automatically. Exported environment variables take precedence.

| Aiven Quick connect field or download | `.env` key | Required for |
| --- | --- | --- |
| Service URI for the selected auth method, `host:port` | `AIVEN_KAFKA_SERVICE_URI` | Both |
| Selected topic name | `AIVEN_KAFKA_TOPIC` | Both |
| Selected authentication method | `AIVEN_KAFKA_AUTH_METHOD=sasl` or `client_certificate` | Both |
| Download CA certificate (`ca.pem`) | `AIVEN_KAFKA_CA_CERT_PATH` | Both |
| Service user name | `AIVEN_KAFKA_SASL_USERNAME` | SASL |
| Service user password | `AIVEN_KAFKA_SASL_PASSWORD` | SASL |
| Download service certificate (`service.cert`) | `AIVEN_KAFKA_SERVICE_CERT_PATH` | Client certificate |
| Download service access key (`service.key`) | `AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH` | Client certificate |

Put the downloaded certificates in `certs/` under this project or use absolute paths. The `certs/` directory and `.env` are ignored by Git. The simulator uses Aiven's SASL/SCRAM-SHA-256 over TLS, or mTLS for client certificates. Use the Service URI shown for the **same authentication method** you selected in Quick connect. The URI must contain `host:port` without a URL scheme. The [Aiven connection examples](https://aiven.io/docs/products/kafka/howto/kcat) show these TLS and SASL settings.

Only run these commands when you are ready to connect to Aiven:

```powershell
python -m data_simulator.simulate_stream --max-events 30
python -m data_simulator.kafka --limit 5
```

The stream includes login, API, payment, file, export, error, and timeout events. Simulated event and arrival timestamps support delayed, out-of-order, and duplicate messages plus incident bursts. Historical incident events can arrive after the initial cutoff as a replay. The producer checkpoints only after Kafka acknowledgements. Create `stream.pause` to pause between micro-batches and remove it to continue. After stopping, pass `--resume` to continue from `stream_state.json`. Consumers should deduplicate by `event_id` because a crash after publish but before checkpoint can repeat a micro-batch.

No broker connection is required by the normal tests. Initial and batch ingestion write local files; streaming sends application events to Kafka and creates no event batch files.

## Upload file drops to a Databricks Volume

The uploader sends the emitted files under `ingestion_data/initial/` and `ingestion_data/batch/` to an **existing** Unity Catalog Volume. These are the source files prepared for future Auto Loader testing. The intermediate files in `generated_data_v2/` and the changing local manifests stay local.

Fill these placeholders in the existing `.env` file (or copy `.env.example` if you do not have one):

```dotenv
DATABRICKS_HOST=https://YOUR-WORKSPACE-URL
DATABRICKS_TOKEN=YOUR-PERSONAL-ACCESS-TOKEN
DATABRICKS_VOLUME_PATH=/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/cloudflow
```

`DATABRICKS_HOST` is your **workspace** URL. The token must belong to a user or service principal with access to that workspace. The catalog, schema, and Volume must already exist. Databricks requires `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, and `WRITE VOLUME` for file uploads and verification. See the [Databricks authentication fields](https://docs.databricks.com/aws/en/dev-tools/auth/env-vars) and [Volume privileges](https://docs.databricks.com/aws/en/volumes/privileges).

Preview the exact remote paths without credentials or a connection, then upload when the values are filled in. These commands are also available separately from the Windows launchers:

```powershell
python -m data_simulator.upload_volume --dry-run
python -m data_simulator.upload_volume --ingestion-type initial
python -m data_simulator.upload_volume --ingestion-type batch
```

You can also pass a Volume path as the command's positional argument to override `DATABRICKS_VOLUME_PATH`. The uploader checks every local file against its manifest before connecting. Existing remote files are downloaded and compared by SHA-256; identical files are skipped on rerun, and differing files stop the upload. Files are never overwritten. Remote paths retain the `initial/` and `batch/` prefixes. The script does not create Databricks resources or implement Auto Loader.

## Tests

`python -m pytest -q` runs without Databricks, credentials, Kafka, OpenAI, or Kaggle. It tests parent-child relationships across the two file paths, the four incidents, immutable initial and batch emission, arrival timing, resume and replay, bad data, the combined launcher workflows, mocked streaming and knowledge generation, and mocked Volume upload.
