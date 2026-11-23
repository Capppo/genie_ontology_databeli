# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,# 03 - Genie Agent setup
# MAGIC %md
# MAGIC # 03 - Genie Agent setup
# MAGIC
# MAGIC This notebook creates and governs the Genie Agent end-to-end via API. Run the
# MAGIC cells top-to-bottom: configuration (00_config + warehouse id) -> UC grants for
# MAGIC the service principal -> create the agent -> certify it -> create the domains.
# MAGIC
# MAGIC The reference blocks below are kept for the manual UI alternative (Genie
# MAGIC Agents list -> Create Agent): use the same Name/Description, data sources,
# MAGIC instructions and sample questions.
# MAGIC
# MAGIC ## Name
# MAGIC ```
# MAGIC Retail Analytics Genie
# MAGIC ```
# MAGIC
# MAGIC ## Description
# MAGIC ```
# MAGIC Answers natural-language questions about Northwind Retail sales performance,
# MAGIC customer returns, and inventory health using certified metric views. Covers
# MAGIC revenue, units sold, orders, average order value, returns and return volume,
# MAGIC and stock-on-hand, sliced by date, product category/brand, customer
# MAGIC segment/region, and store region/channel.
# MAGIC ```
# MAGIC
# MAGIC ## Data sources to attach
# MAGIC Add these three metric views as the agent's tables/data sources (created in
# MAGIC `02_create_metric_views`):
# MAGIC - `mv_sales_performance`
# MAGIC - `mv_customer_returns`
# MAGIC - `mv_inventory_health`
# MAGIC
# MAGIC Do **not** attach the raw fact/dimension tables directly - the point of the demo
# MAGIC is to show Genie preferring the governed metric views over ad hoc joins.
# MAGIC
# MAGIC ## Custom instructions (agent-level, not workspace-level)
# MAGIC ```
# MAGIC You are a retail analytics assistant for Northwind Retail.
# MAGIC
# MAGIC - Always prefer the metric views (mv_sales_performance, mv_customer_returns,
# MAGIC   mv_inventory_health) over querying raw tables directly.
# MAGIC - "Revenue" or "sales" means the total_revenue measure in mv_sales_performance,
# MAGIC   not list price or unit cost.
# MAGIC - "Return rate" is not a stored measure - compute it as
# MAGIC   mv_customer_returns.return_amount / mv_sales_performance.total_revenue for the
# MAGIC   same time period and dimension slice, and say so when you do.
# MAGIC - All monetary figures are in USD.
# MAGIC - When a question is ambiguous about time range, default to the trailing 12
# MAGIC   months and state the range you used.
# MAGIC ```
# MAGIC
# MAGIC ## Sample / benchmark questions to add in the agent's setup screen
# MAGIC - What was total revenue last quarter by product category?
# MAGIC - What's our customer return rate this year vs last year?
# MAGIC - Which region has the highest revenue but also high returns?
# MAGIC - What is the average order value by customer segment?
# MAGIC - Which products have the lowest average stock on hand?
# MAGIC
# MAGIC ## After creating the agent
# MAGIC 1. Certification and Sales-domain assignment are handled by the notebook cells
# MAGIC    above (governed tags applied via API).
# MAGIC 2. Run `04_apply_certification_and_domains` for everything else (tables +
# MAGIC    metric views) - it's safe to also certify/tag the metric view you did by
# MAGIC    hand again; the script is idempotent.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Warehouse id
# Cell — Get only the first warehouse_id
WAREHOUSE_ID = spark.sql("""
SELECT warehouse_id
FROM system.compute.warehouses
WHERE delete_time IS NULL
  AND warehouse_type IN ('PRO', 'SERVERLESS')
ORDER BY change_time DESC
LIMIT 1
""").first()["warehouse_id"]

print(f"WAREHOUSE_ID = '{WAREHOUSE_ID}'")

# COMMAND ----------

# DBTITLE 1,Genie Agent configuration
# Cell 1 — Genie Agent configuration with OAuth M2M service principal
import json
import uuid
from databricks.sdk import WorkspaceClient

# --- Workspace configuration ---
WORKSPACE_HOST = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

# --- Metric views and warehouse ---
# CATALOG/SCHEMA come from 00_config (via %run); WAREHOUSE_ID from the cell above.

# --- Unity Catalog Secrets (three-level namespace) ---
CLIENT_ID_SECRET = "api-token-service-principal-client-id"       # <-- secret name
CLIENT_SECRET_SECRET = "api-token-service-principal-secret"  # <-- secret name

client_id = dbutils.secrets.get(
    catalog=CATALOG,
    schema=SCHEMA,
    key=CLIENT_ID_SECRET
)

client_secret = dbutils.secrets.get(
    catalog=CATALOG,
    schema=SCHEMA,
    key=CLIENT_SECRET_SECRET
)

# --- OAuth M2M authenticated client ---
# The create-agent API (POST /api/2.0/genie/spaces) requires the "genie" scope,
# so the OAuth secret must be generated with that scope assigned and the
# client must request it (the SDK defaults to all-apis).
w = WorkspaceClient(
    host=WORKSPACE_HOST,
    client_id=client_id,
    client_secret=client_secret,
    auth_type="oauth-m2m",
    scopes=["genie"]
)

HOST = WORKSPACE_HOST.rstrip("/")

# --- Genie Agent definition ---
AGENT_TITLE = "Retail Analytics Genie"

AGENT_DESCRIPTION = """Answers natural-language questions about Northwind Retail sales performance,
customer returns, and inventory health using certified metric views. Covers
revenue, units sold, orders, average order value, returns and return volume,
and stock-on-hand, sliced by date, product category/brand, customer
segment/region, and store region/channel."""

AGENT_INSTRUCTIONS = """You are a retail analytics assistant for Northwind Retail.

- Always prefer the metric views (mv_sales_performance, mv_customer_returns,
  mv_inventory_health) over querying raw tables directly.
- "Revenue" or "sales" means the total_revenue measure in mv_sales_performance,
  not list price or unit cost.
- "Return rate" is not a stored measure - compute it as
  mv_customer_returns.return_amount / mv_sales_performance.total_revenue for the
  same time period and dimension slice, and say so when you do.
- All monetary figures are in USD.
- When a question is ambiguous about time range, default to the trailing 12
  months and state the range you used."""

SAMPLE_QUESTIONS = [
    "What was total revenue last quarter by product category?",
    "What's our customer return rate this year vs last year?",
    "Which region has the highest revenue but also high returns?",
    "What is the average order value by customer segment?",
    "Which products have the lowest average stock on hand?",
]

METRIC_VIEWS = [
    f"{CATALOG}.{SCHEMA}.mv_customer_returns",
    f"{CATALOG}.{SCHEMA}.mv_inventory_health",
    f"{CATALOG}.{SCHEMA}.mv_sales_performance",
]

# IDs must be unique, lowercase, 32-character hexadecimal strings. They are
# displayed in ascending-ID order, so sort them first to keep the questions
# in the order of SAMPLE_QUESTIONS in the agent UI.
_ids = sorted(uuid.uuid4().hex for _ in SAMPLE_QUESTIONS)
sample_questions = [
    {"id": id_, "question": [q]}
    for id_, q in zip(_ids, SAMPLE_QUESTIONS)
]

serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": sample_questions
    },
    "data_sources": {
        "metric_views": [
            {"identifier": identifier}
            for identifier in sorted(METRIC_VIEWS)
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": uuid.uuid4().hex,
                "content": [AGENT_INSTRUCTIONS]
            }
        ]
    }
}

print("OAuth M2M credentials loaded from Databricks Secrets.")
print(json.dumps(serialized_space, indent=2))

# COMMAND ----------

# DBTITLE 1,Grant SP access to metric views
# Cell — Grant the service principal read access to the metric views
# The Genie API validates that the agent's creator can read every attached
# data asset (USE CATALOG + USE SCHEMA + SELECT). Run this once as the
# notebook user, who owns the catalog and schema; the grants go to the
# OAuth service principal (its application ID is the OAuth client_id).
GRANTS = [
    ("USE CATALOG on the catalog", f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{client_id}`"),
    ("USE SCHEMA on the schema", f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{client_id}`"),
    ("SELECT on mv_sales_performance", f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.mv_sales_performance TO `{client_id}`"),
    ("SELECT on mv_customer_returns", f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.mv_customer_returns TO `{client_id}`"),
    ("SELECT on mv_inventory_health", f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.mv_inventory_health TO `{client_id}`"),
]

for description, statement in GRANTS:
    spark.sql(statement)
    print(f"Granted {description} to the service principal.")

# COMMAND ----------

# DBTITLE 1,Create the Genie Agent
# Cell 2 — Create the Genie Agent using an OAuth M2M access token
import json
import requests

# Obtain an OAuth access token for the service principal.
# The SDK manages the OAuth token lifecycle.
token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
url = f"{HOST}/api/2.0/genie/spaces"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

payload = {
    "title": AGENT_TITLE,
    "description": AGENT_DESCRIPTION,
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space)
}

# Idempotent: if an agent with this title already exists, update it instead
# of creating a duplicate.
existing = requests.get(url, headers=headers).json()
match = next(
    (s for s in existing.get("spaces", []) if s.get("title") == AGENT_TITLE),
    None,
)

if match is not None:
    GENIE_SPACE_ID = match["space_id"]
    update_body = {**payload}
    if match.get("etag"):
        update_body["etag"] = match["etag"]
    response = requests.patch(f"{url}/{GENIE_SPACE_ID}", headers=headers, json=update_body)
    response.raise_for_status()
    print(f"Updated existing Genie Agent: {AGENT_TITLE}")
else:
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    GENIE_SPACE_ID = response.json()["space_id"]
    print(f"Created Genie Agent: {AGENT_TITLE}")

print(f"Genie space_id: {GENIE_SPACE_ID}")

# COMMAND ----------

# DBTITLE 1,Certify the Genie Agent
# Cell 3 — Certify the Genie Agent (governed tag, user context)
# Agent certification is the Unity Catalog governed tag
# system.certification_status = certified applied to the space entity.
# It needs the notebook user's permissions (ASSIGN on the governed tag),
# so this cell uses default notebook authentication - not the genie-scoped
# service principal token from Cell 1, which cannot call the tag API.
from databricks.sdk import WorkspaceClient

w_user = WorkspaceClient()

def fetch_tags():
    result = w_user.api_client.do(
        "GET", f"/api/2.0/entity-tag-assignments/geniespaces/{GENIE_SPACE_ID}/tags"
    )
    return {a["tag_key"]: a["tag_value"] for a in result.get("tag_assignments", [])}


assignments = fetch_tags()
if assignments.get("system.certification_status") != "certified":
    w_user.api_client.do(
        "POST",
        "/api/2.0/entity-tag-assignments",
        body={
            "entity_type": "geniespaces",
            "entity_id": GENIE_SPACE_ID,
            "tag_key": "system.certification_status",
            "tag_value": "certified",
        },
    )
    assignments = fetch_tags()

# Verify the assignment took effect (idempotent: safe to re-run)
assert assignments.get("system.certification_status") == "certified", assignments
print(f"Certified Genie Agent {GENIE_SPACE_ID}: system.certification_status = certified")

# COMMAND ----------

# DBTITLE 1,Create demo domains
# Cell 4 — Create the 4 demo domains (Discover)
# A domain is backed by a governed tag whose key IS the domain name - the
# same keys notebook 04 uses in its SET TAG statements. Creating a domain
# via the API is two steps per name:
#   1. create the governed tag policy (POST /api/2.1/tag-policies)
#   2. create the domain referencing it (POST /api/2.0/domains)
# Idempotent: existing tag policies and domains are skipped, safe to re-run.
from databricks.sdk import WorkspaceClient

w_user = WorkspaceClient()

DOMAINS = {
    "Sales":        ("sales",        "Retail sales performance and revenue analytics"),
    "Customer":     ("customer",     "Customer behavior, returns and segmentation"),
    "Supply Chain": ("supply-chain", "Inventory health and stock performance"),
    "Finance":      ("finance",      "Financial reporting and revenue accounting"),
}

existing_policies = {
    p["tag_key"]
    for p in w_user.api_client.do("GET", "/api/2.1/tag-policies")["tag_policies"]
}
existing_domains = {
    d["tag_key"]
    for d in w_user.api_client.do("GET", "/api/2.0/domains")["domains"]
}

for tag_key, (domain_id, subtitle) in DOMAINS.items():
    if tag_key not in existing_policies:
        w_user.api_client.do("POST", "/api/2.1/tag-policies", body={"tag_key": tag_key})
        print(f"Created governed tag: {tag_key}")
    if tag_key not in existing_domains:
        w_user.api_client.do(
            "POST",
            "/api/2.0/domains",
            body={"domain_id": domain_id, "tag_key": tag_key, "subtitle": subtitle},
        )
        print(f"Created domain: {tag_key}")
    else:
        print(f"Domain already exists: {tag_key}")

# The create endpoint does not set display names (it also ignores a custom
# domain_id and generates its own UUID) - PATCH the display name onto any
# domain still missing one.
current_domains = {
    d["tag_key"]: d
    for d in w_user.api_client.do("GET", "/api/2.0/domains")["domains"]
}
for tag_key in DOMAINS:
    domain = current_domains.get(tag_key)
    if domain is None or domain.get("display_name"):
        continue
    w_user.api_client.do(
        "PATCH",
        f"/api/2.0/domains/{domain['domain_id']}",
        body={"display_name": tag_key},
        query={"update_mask": "display_name"},
    )
    print(f"Set display name for domain: {tag_key}")

# Assign the Genie Agent to the Sales domain (key-only governed tag) —
# idempotent, mirroring the certify cell.
agent_tag_keys = {
    t["tag_key"]
    for t in w_user.api_client.do(
        "GET", f"/api/2.0/entity-tag-assignments/geniespaces/{GENIE_SPACE_ID}/tags"
    ).get("tag_assignments", [])
}
if "Sales" not in agent_tag_keys:
    w_user.api_client.do(
        "POST",
        "/api/2.0/entity-tag-assignments",
        body={
            "entity_type": "geniespaces",
            "entity_id": GENIE_SPACE_ID,
            "tag_key": "Sales",
        },
    )
    print("Assigned the Genie Agent to the Sales domain.")

print("\nCurrent domains:")
for d in w_user.api_client.do("GET", "/api/2.0/domains")["domains"]:
    print(f"  - {d.get('display_name')} (tag: {d['tag_key']})")