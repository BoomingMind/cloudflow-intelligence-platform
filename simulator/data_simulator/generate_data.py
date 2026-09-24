"""Create deterministic initial history and later batch-source changes."""
import argparse
import csv
import random
from datetime import timedelta

from faker import Faker

from .common import config, iso, resolve, timestamp, write_json, write_jsonl, write_parquet


PRODUCTS = [
    {"product_id": "PRD-001", "product_name": "CloudFlow API", "service": "api", "monthly_price": 99.0},
    {"product_id": "PRD-002", "product_name": "CloudFlow Billing", "service": "billing", "monthly_price": 49.0},
    {"product_id": "PRD-003", "product_name": "CloudFlow Storage", "service": "storage", "monthly_price": 79.0},
]
INCIDENT_SPECS = [
    ("INC-001", 7, "payment_degradation", "billing", "all", "PAYMENT_GATEWAY_TIMEOUT"),
    ("INC-002", 14, "api_incident", "api", "all", "API_UPSTREAM_503"),
    ("INC-003", 21, "regional_outage", "storage", "us-west", "STORAGE_REGION_UNAVAILABLE"),
]


def kaggle_rows(path):
    if not path:
        return []
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows or not any("Ticket Subject" in row for row in rows):
        raise ValueError("Kaggle CSV needs the 'Ticket Subject' column")
    return rows


def create(cfg, kaggle_csv=None):
    rng = random.Random(cfg["seed"])
    fake = Faker("en_US")
    fake.seed_instance(cfg["seed"])
    start = timestamp(cfg["start_date"] + "T00:00:00+00:00")
    out = resolve(cfg["generated_dir"])
    out.mkdir(parents=True, exist_ok=True)
    source = kaggle_rows(kaggle_csv or cfg.get("kaggle_csv"))
    regions = ["us-east", "us-west", "eu-west"]
    customers = []
    for i in range(cfg["customers"]):
        customers.append({
            "customer_id": f"CUST-{i+1:05d}", "name": fake.name(),
            "email": f"customer{i+1:05d}@example.com", "region": regions[i % len(regions)],
            "tier": rng.choices(["standard", "business", "enterprise"], [65, 27, 8])[0],
            "created_at": iso(start - timedelta(days=rng.randint(30, 600))),
        })
    incidents = []
    for incident_id, day, kind, service, region, code in INCIDENT_SPECS:
        begin = start + timedelta(days=day, hours=10)
        incidents.append({"incident_id": incident_id, "kind": kind, "service": service,
                          "region": region, "error_code": code, "start_time": iso(begin),
                          "end_time": iso(begin + timedelta(hours=6))})
    subscriptions = []
    for customer in customers:
        # Every account has API and billing; storage is common in business and enterprise.
        products = PRODUCTS if customer["customer_id"] == "CUST-00002" or customer["tier"] != "standard" or rng.random() < .35 else PRODUCTS[:2]
        for product in products:
            subscriptions.append({"subscription_id": f"SUB-{len(subscriptions)+1:06d}",
                                  "customer_id": customer["customer_id"],
                                  "product_id": product["product_id"], "status": "active",
                                  "started_at": iso(start - timedelta(days=rng.randint(30, 360))),
                                  "monthly_amount": product["monthly_price"] *
                                  (3 if customer["tier"] == "enterprise" else 1.5 if customer["tier"] == "business" else 1)})
    by_customer = {c["customer_id"]: c for c in customers}
    payments = []
    for sub in subscriptions:
        for n in range(cfg["base_payments_per_subscription"]):
            paid_at = start + timedelta(days=2 + 17 * n, hours=rng.randint(0, 20))
            payments.append({"payment_id": f"PAY-{len(payments)+1:07d}",
                             "subscription_id": sub["subscription_id"], "customer_id": sub["customer_id"],
                             "product_id": sub["product_id"], "amount": sub["monthly_amount"],
                             "currency": "USD", "status": "succeeded", "attempted_at": iso(paid_at),
                             "incident_id": None, "error_code": None, "retry_of_payment_id": None})
    for sub in subscriptions:
        if sub["product_id"] != "PRD-002" or rng.random() > .7:
            continue
        incident = incidents[0]
        failed_id = f"PAY-{len(payments)+1:07d}"
        payments.append({"payment_id": failed_id,
                         "subscription_id": sub["subscription_id"], "customer_id": sub["customer_id"],
                         "product_id": sub["product_id"], "amount": sub["monthly_amount"],
                         "currency": "USD", "status": "failed",
                         "attempted_at": iso(timestamp(incident["start_time"]) + timedelta(minutes=rng.randint(1, 330))),
                         "incident_id": incident["incident_id"], "error_code": incident["error_code"],
                         "retry_of_payment_id": None})
        payments.append({"payment_id": f"PAY-{len(payments)+1:07d}",
                         "subscription_id": sub["subscription_id"], "customer_id": sub["customer_id"],
                         "product_id": sub["product_id"], "amount": sub["monthly_amount"],
                         "currency": "USD", "status": "succeeded",
                         "attempted_at": iso(timestamp(incident["end_time"]) + timedelta(hours=rng.randint(1, 6))),
                         "incident_id": incident["incident_id"], "error_code": None,
                         "retry_of_payment_id": failed_id})
    payments.sort(key=lambda payment: (payment["attempted_at"], payment["payment_id"]))
    tickets = []
    ticket_templates = {
        "billing": [
            ("Payment attempt failed", "Billing page reports a gateway timeout during renewal."),
            ("Invoice total question", "The customer asks the agent to explain a recent invoice total."),
            ("Payment method update", "The customer needs help updating the card used for renewal."),
            ("Renewal receipt missing", "The customer cannot find the receipt for a completed renewal."),
        ],
        "api": [
            ("API requests returning errors", "Requests to the API intermittently return 503."),
            ("Token authentication question", "The customer is checking why a valid API token was rejected."),
            ("Webhook delivery delay", "A webhook delivery arrived later than the customer expected."),
            ("Rate limit guidance", "The customer asks how to handle a burst of API requests."),
        ],
        "storage": [
            ("File access unavailable", "Uploads and downloads are unavailable in this region."),
            ("Upload interrupted", "A large file upload stopped before completion."),
            ("Export duration question", "The customer asks why an export took longer than usual."),
            ("Download permission question", "The customer needs help checking file download permissions."),
        ],
    }
    for i in range(cfg["tickets"]):
        incident = incidents[i % 3] if i < max(3, cfg["tickets"] // 2) else None
        seed_row = source[i % len(source)] if source else None
        if incident:
            service = incident["service"]
        elif seed_row:
            text = (str(seed_row.get("Ticket Subject", "")) + " " + str(seed_row.get("Ticket Type", ""))).lower()
            service = "billing" if any(word in text for word in ("bill", "pay", "invoice", "refund")) else "storage" if any(word in text for word in ("file", "data", "storage", "upload", "download")) else "api"
        else:
            service = rng.choices(["api", "billing", "storage"], [50, 25, 25])[0]
        eligible = [s for s in subscriptions if next(p["service"] for p in PRODUCTS if p["product_id"] == s["product_id"]) == service
                    and (not incident or incident["region"] == "all" or by_customer[s["customer_id"]]["region"] == incident["region"])]
        sub = rng.choice(eligible)
        title, description = ticket_templates[service][0] if incident else rng.choice(ticket_templates[service])
        opened = (timestamp(incident["start_time"]) + timedelta(minutes=rng.randint(5, 350))) if incident else (start + timedelta(days=rng.randint(0, cfg["days"]-1), hours=rng.randint(0, 23)))
        priority_seed = str(seed_row.get("Ticket Priority", "")).lower() if seed_row else ""
        priority = "high" if incident else priority_seed if priority_seed in ("low", "medium", "high", "critical") else rng.choices(["low", "medium", "high"], [20, 65, 15])[0]
        status = rng.choices(["open", "pending", "resolved"], [25, 15, 60])[0]
        response = opened + timedelta(minutes=rng.randint(60, 240) if incident else rng.randint(5, 120))
        resolved = (max(response, timestamp(incident["end_time"])) if incident else response) + timedelta(hours=rng.randint(1, 24)) if status == "resolved" else None
        channel_seed = str(seed_row.get("Ticket Channel", "")).lower() if seed_row else ""
        channel = channel_seed if channel_seed in ("email", "phone", "chat", "web", "social media") else rng.choice(["email", "chat", "web"])
        tickets.append({"ticket_id": f"TKT-{i+1:07d}", "customer_id": sub["customer_id"],
                        "product_id": sub["product_id"], "subscription_id": sub["subscription_id"],
                        "incident_id": incident["incident_id"] if incident else None,
                        "title": title,
                        "description": description, "source_seed": "kaggle" if seed_row else "synthetic",
                        "seed_ticket_id": str(seed_row.get("Ticket ID", "")) if seed_row else None,
                        "seed_ticket_type": str(seed_row.get("Ticket Type", "")) if seed_row else None,
                        "seed_subject": str(seed_row.get("Ticket Subject", "")) if seed_row else None,
                        "seed_description": str(seed_row.get("Ticket Description", "")) if seed_row else None,
                        "priority": priority, "channel": channel, "status": status,
                        "opened_at": iso(opened), "first_response_at": iso(response),
                        "resolved_at": iso(resolved) if resolved else None,
                        "region": by_customer[sub["customer_id"]]["region"]})
    # Initial product guidance is independent of later operational incidents.
    initial_articles = []
    for index, product in enumerate(PRODUCTS, start=1):
        initial_articles.append({"article_id": f"KA-GUIDE-{index:03d}",
                                 "article_version": 1, "change_type": "SNAPSHOT",
                                 "published_at": cfg["initial"]["arrival_time"],
                                 "title": f"{product['product_name']} support guide",
                                 "product_id": product["product_id"],
                                 "product_name": product["product_name"],
                                 "service": product["service"], "incident_id": None,
                                 "region": "all", "error_codes": [],
                                 "related_ticket_ids": [t["ticket_id"] for t in tickets
                                                        if t["product_id"] == product["product_id"]][:5],
                                 "facts": {"supported_scope": f"Customer support for the {product['service']} service",
                                           "observed_effect": "Routine usage questions and troubleshooting requests"}})

    # Batch data begins after the initial snapshot. Existing IDs can reappear only
    # as explicit higher-version changes; new records get fresh IDs.
    cutover = timestamp(cfg["initial"]["arrival_time"])
    batch_incident = {"incident_id": "INC-004", "kind": "payment_degradation", "service": "billing",
                      "region": "all", "error_code": "BILLING_RETRY_STORM",
                      "start_time": iso(cutover + timedelta(hours=10)),
                      "end_time": iso(cutover + timedelta(hours=16)),
                      "status": "resolved", "record_version": 1, "change_type": "INSERT",
                      "source_change_time": iso(cutover + timedelta(hours=10))}
    batch_incidents = [batch_incident]
    new_customer = {"customer_id": f"CUST-{len(customers)+1:05d}", "name": fake.name(),
                    "email": f"customer{len(customers)+1:05d}@example.com", "region": "us-east",
                    "tier": "business", "created_at": iso(cutover + timedelta(hours=6)),
                    "record_version": 1, "change_type": "INSERT",
                    "source_change_time": iso(cutover + timedelta(hours=6))}
    upgraded_customer = {**customers[0], "tier": "business", "record_version": 2,
                         "change_type": "UPSERT", "source_change_time": iso(cutover + timedelta(hours=7))}
    batch_customers = [new_customer, upgraded_customer]
    batch_products = [{**PRODUCTS[0], "monthly_price": 109.0, "record_version": 2,
                       "change_type": "UPSERT", "source_change_time": iso(cutover + timedelta(hours=8))}]
    batch_subscriptions = [
        {**subscriptions[0], "monthly_amount": 109.0, "record_version": 2,
         "change_type": "UPSERT", "source_change_time": iso(cutover + timedelta(hours=8))},
        {**subscriptions[1], "status": "cancelled", "record_version": 2,
         "change_type": "UPSERT", "source_change_time": iso(cutover + timedelta(hours=8))},
    ]
    for product in PRODUCTS[:2]:
        batch_subscriptions.append({"subscription_id": f"SUB-{len(subscriptions)+len(batch_subscriptions)-1:06d}",
                                    "customer_id": new_customer["customer_id"],
                                    "product_id": product["product_id"], "status": "active",
                                    "started_at": iso(cutover + timedelta(hours=9)),
                                    "monthly_amount": 109.0 if product["service"] == "api" else product["monthly_price"] * 1.5,
                                    "record_version": 1, "change_type": "INSERT",
                                    "source_change_time": iso(cutover + timedelta(hours=9))})
    latest_subscriptions = {sub["subscription_id"]: sub for sub in subscriptions}
    latest_subscriptions.update({sub["subscription_id"]: sub for sub in batch_subscriptions})
    active_subscriptions = [sub for sub in latest_subscriptions.values() if sub["status"] == "active"]
    billing_subscriptions = [sub for sub in active_subscriptions if sub["product_id"] == "PRD-002"]
    latest_customers = {**by_customer, new_customer["customer_id"]: new_customer}
    batch_payments = []
    for i in range(60):
        if i < 8:
            sub = billing_subscriptions[i % len(billing_subscriptions)]
            attempted = cutover + timedelta(hours=10, minutes=10 + i * 30)
            status, code, retry_id = "failed", batch_incident["error_code"], None
            incident_id = batch_incident["incident_id"]
        elif i < 16:
            failed = batch_payments[i - 8]
            sub = latest_subscriptions[failed["subscription_id"]]
            attempted = cutover + timedelta(hours=17, minutes=(i - 8) * 20)
            status, code, retry_id = "succeeded", None, failed["payment_id"]
            incident_id = batch_incident["incident_id"]
        else:
            sub = rng.choice(active_subscriptions)
            attempted = cutover + timedelta(days=i // 25, hours=rng.randint(10, 20), minutes=rng.randint(0, 59))
            status, code, retry_id, incident_id = "succeeded", None, None, None
        batch_payments.append({"payment_id": f"PAY-{len(payments)+i+1:07d}",
                               "subscription_id": sub["subscription_id"],
                               "customer_id": sub["customer_id"], "product_id": sub["product_id"],
                               "amount": sub["monthly_amount"], "currency": "USD", "status": status,
                               "attempted_at": iso(attempted), "incident_id": incident_id,
                               "error_code": code, "retry_of_payment_id": retry_id,
                               "record_version": 1, "change_type": "INSERT",
                               "source_change_time": iso(attempted)})
    batch_payments.sort(key=lambda row: (row["source_change_time"], row["payment_id"]))
    batch_tickets = []
    open_tickets = [ticket for ticket in tickets if ticket["status"] != "resolved"]
    for i in range(60):
        if i < 3:
            original = open_tickets[i]
            changed = cutover + timedelta(hours=8, minutes=i * 10)
            batch_tickets.append({**original, "status": "resolved", "resolved_at": iso(changed),
                                  "record_version": 2, "change_type": "UPSERT",
                                  "source_change_time": iso(changed)})
            continue
        incident = batch_incident if i < 15 else None
        sub = rng.choice(billing_subscriptions if incident else active_subscriptions)
        service = next(p["service"] for p in PRODUCTS if p["product_id"] == sub["product_id"])
        opened = (cutover + timedelta(hours=10, minutes=rng.randint(5, 340)) if incident else
                  cutover + timedelta(days=i // 25, hours=rng.randint(8, 20), minutes=rng.randint(0, 59)))
        responded = opened + timedelta(minutes=rng.randint(10, 90))
        status = "open" if incident else rng.choice(["open", "pending", "resolved"])
        batch_tickets.append({"ticket_id": f"TKT-{len(tickets)+i-2:07d}",
                              "customer_id": sub["customer_id"], "product_id": sub["product_id"],
                              "subscription_id": sub["subscription_id"],
                              "incident_id": incident["incident_id"] if incident else None,
                              "title": ticket_templates[service][0][0] if incident else rng.choice(ticket_templates[service])[0],
                              "description": ticket_templates[service][0][1] if incident else "Customer requests assistance with this service.",
                              "source_seed": "synthetic", "seed_ticket_id": None,
                              "seed_ticket_type": None, "seed_subject": None, "seed_description": None,
                              "priority": "high" if incident else "medium", "channel": rng.choice(["email", "chat", "web"]),
                              "status": status, "opened_at": iso(opened),
                              "first_response_at": iso(responded),
                              "resolved_at": iso(responded + timedelta(hours=4)) if status == "resolved" else None,
                              "region": latest_customers[sub["customer_id"]]["region"],
                              "record_version": 1, "change_type": "INSERT",
                              "source_change_time": iso(opened)})
    batch_tickets.sort(key=lambda row: (row["source_change_time"], row["ticket_id"]))

    # Curated incident facts and all identifiers come from Python, never from the LLM.
    batch_articles = []
    for incident in incidents + batch_incidents:
        product = next(p for p in PRODUCTS if p["service"] == incident["service"])
        related = tickets + batch_tickets
        batch_articles.append({"article_id": "KA-" + incident["incident_id"].split("-")[1],
                         "article_version": 1, "change_type": "INSERT",
                         "published_at": iso(cutover + timedelta(hours=18)),
                         "title": incident["kind"].replace("_", " ").title(),
                         "product_id": product["product_id"], "product_name": product["product_name"],
                         "service": incident["service"], "incident_id": incident["incident_id"],
                         "region": incident["region"], "error_codes": [incident["error_code"]],
                         "related_ticket_ids": [t["ticket_id"] for t in related if t["incident_id"] == incident["incident_id"]][:8],
                         "facts": {"start_time": incident["start_time"], "end_time": incident["end_time"],
                                   "observed_effect": ticket_templates[incident["service"]][0][1]}})
    batch_articles.append({**initial_articles[1], "article_version": 2,
                           "change_type": "UPSERT", "published_at": iso(cutover + timedelta(hours=19)),
                           "facts": {"supported_scope": "Billing and renewal support",
                                     "observed_effect": "Include safe retry and duplicate-charge checks after gateway disruption"}})
    write_parquet(out / "customers.parquet", customers)
    write_parquet(out / "products.parquet", PRODUCTS)
    write_parquet(out / "subscriptions.parquet", subscriptions)
    write_parquet(out / "payments.parquet", payments)
    write_jsonl(out / "tickets.jsonl", tickets)
    write_json(out / "incidents.json", incidents)
    write_jsonl(out / "knowledge_metadata_initial.jsonl", initial_articles)
    write_jsonl(out / "knowledge_metadata_batch.jsonl", batch_articles)
    write_parquet(out / "batch_customers.parquet", batch_customers)
    write_parquet(out / "batch_products.parquet", batch_products)
    write_parquet(out / "batch_subscriptions.parquet", batch_subscriptions)
    write_parquet(out / "batch_payments.parquet", batch_payments)
    write_jsonl(out / "batch_tickets.jsonl", batch_tickets)
    write_jsonl(out / "batch_incidents.jsonl", batch_incidents)
    return {"customers": len(customers), "products": len(PRODUCTS), "subscriptions": len(subscriptions),
            "payments": len(payments), "tickets": len(tickets), "incidents": len(incidents),
            "batch_customers": len(batch_customers), "batch_products": len(batch_products),
            "batch_subscriptions": len(batch_subscriptions), "batch_payments": len(batch_payments),
            "batch_tickets": len(batch_tickets), "batch_incidents": len(batch_incidents),
            "knowledge_initial": len(initial_articles), "knowledge_batch": len(batch_articles),
            "kaggle_seed_rows": len(source)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--kaggle-csv", help="Locally downloaded Kaggle CSV")
    args = p.parse_args()
    print(create(config(args.config), args.kaggle_csv))


if __name__ == "__main__":
    main()
