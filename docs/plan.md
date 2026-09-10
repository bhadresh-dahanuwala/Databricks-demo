# Migration Plan: Supabase and Confluent Cloud Integration

## 1. Current State

- **Infrastructure (Terraform):** Currently provisions an Azure Databricks workspace, Unity Catalog with an external location (`ecomm-ext`), schemas (`raw`, `staging`, `intermediate`, `analytics`), and associated ADLS storage.
- **Pipelines:** Delta Live Tables (DLT) pipelines are orchestrated via Databricks Asset Bundles (`databricks.yml`).
- **Data Ingestion:** `trigger_raw_ingestion.yml` runs on a `file_arrival` trigger for `/Volumes/ecomm/raw/raw_vol/`. The raw layer script (`src/raw/generic_ingestion.py`) uses Spark Auto Loader (`cloudFiles`) to read JSON dumps for `customer`, `product`, `order`, `order_item`, `return`, and `return_item`.
- All six entities currently have **historical data already landed** in `raw` and propagated through `staging` → `intermediate` → `analytics` via the existing JSON-based pipeline.

## 2. Proposed Architectural Strategy

Rather than defaulting to custom hand-written Spark JDBC/Structured Streaming, the architecture prioritizes modern, managed Databricks-native services, while explicitly accounting for the fact that the source systems are Free-Tier and that this is a **live cutover** from an already-running pipeline, not a greenfield build.

### A. Postgres Ingestion (Supabase Free-Tier)

Two options are evaluated based on downstream consumption needs — **not** a single default:

1. **Option 1: Lakehouse Federation (Zero-ETL)**
   - **How it works:** Query the Postgres database directly through Unity Catalog without physically copying the data.
   - **Why consider this:** For modest data volumes (expected on a free-tier project), live federated queries avoid building and maintaining an ETL pipeline entirely.
   - **When appropriate:** Downstream consumers do not need Delta-level query performance, do not need point-in-time historical snapshots of the Postgres-sourced entities, and can tolerate querying an external system's live state.
   - **Architectural implication:** If chosen, these entities **do not get a physical `raw` copy**. See Section 2C for how this reconciles with the existing medallion schema.

2. **Option 2: Lakeflow Connect Managed PostgreSQL Connector (CDC)**
   - **How it works:** If a physical copy into Delta is required (analytics performance, historical tracking, or full lineage/DQ enforcement), use Lakeflow Connect's managed PostgreSQL connector (Public Preview). This relies on logical replication to capture true inserts, updates, and deletes — no periodic JDBC sweeps, no missed deletes.
   - **Free-Tier Constraints to Verify (blocking, must confirm before committing to this option):**
     - **Logical Replication:** Requires Postgres 13+, `wal_level=logical`, a publication, and a replication user. Must verify Supabase's free tier permits modifying `wal_level` and exposes sufficient privileges to configure logical replication.
     - **Connection limits & Pausing:** Supabase free tier has connection pooling limits and auto-pauses projects after inactivity. Replication slots consume connections and will be disrupted by a pause — need a mitigation (e.g., a keep-alive mechanism) if this tier is used beyond a POC.

**Decision rule:** Start with Federation unless a validated downstream requirement (historical trend analysis, join-heavy performance needs, or SLA on freshness/query latency) justifies the added operational complexity of the CDC connector.

### B. Kafka Ingestion (Confluent Cloud Free-Tier)

1. **Option 1: Lakeflow Connect Managed Kafka Connector (preferred default)**
   - **How it works:** Ingests data from Kafka topics into streaming tables via serverless compute, with Unity Catalog governance built in, authenticated through a UC Connection object (see Section 2D).
   - **Beta Limitations to Verify against requirements:** Does not currently support orchestration via Databricks Workflows, SCD Type 2, column selection/deselection, or row filtering.
2. **Option 2: Custom Structured Streaming (fallback only)**
   - Used only if a Beta limitation above is a hard, validated requirement (e.g., SCD Type 2 is genuinely needed on a Kafka-sourced entity, or Workflows orchestration cannot be worked around).

**Note on orchestration:** The Workflows limitation applies specifically to *how the ingestion pipeline is triggered/monitored at runtime*, not to deployment. Lakeflow Connect ingestion pipelines, including the Kafka connector, support deployment via Databricks Asset Bundles — meaning the existing `databricks.yml`-based deployment pattern likely does not need to be abandoned even if the managed Kafka connector is used. This should still be validated in a spike before finalizing the approach.

### C. Reconciling with the Existing Medallion Schema (`raw` / `staging` / `intermediate` / `analytics`)

This is a first-class design decision, not an implementation detail:

- **If Federation is used for Postgres entities:** There is no physical `raw` table for those entities. `staging`/`intermediate` layers for these entities become **views or lightly-transformed pass-throughs over the federated tables**, not materialized Delta tables. `analytics`-layer consumers get the same interface either way, but lineage tools and data-quality checks must be configured to treat these differently (federated source vs. physically ingested).
- **If the CDC connector is used for Postgres entities, or the managed Kafka connector for Kafka entities:** These land as physical streaming tables in `raw`, consistent with the existing pattern for `customer`/`product`.
- **Governance impact:** Document explicitly, per entity, whether its `raw` layer is "physical" or "federated (virtual)" — this affects retention/compliance handling, DQ tooling, and any future auditing requirements.

### D. Secrets & Connections (Terraform)

Rather than generic Databricks Secret Scopes for connection strings/API keys, provision **Unity Catalog `Connection` objects** (`databricks_connection` in Terraform) for both the Supabase Postgres source and the Confluent Kafka source. This is the pattern the managed connectors expect:

- Credentials are stored once in the UC Connection, not distributed via secret scopes referenced ad hoc in pipeline code.
- Access is governed via `USE CONNECTION` grants — auditable and revocable through Unity Catalog, consistent with the rest of the governance model already in place for this workspace.
- If Lakehouse Federation is chosen for Postgres, it also uses a UC Connection (of type `postgresql`) rather than a secret scope.

### E. Free-Tier System Constraints & Risk Mitigation

- **Confluent Cloud Free Tier:** Connection/throughput limits and retention caps apply. A continuous streaming pipeline may be overkill (and waste serverless compute) if actual throughput is low — evaluate triggered/scheduled micro-batching as an alternative to continuous ingestion once real volume is known.
- **Confluent Schema Registry:** If Kafka topics use Avro/Protobuf, Schema Registry integration affects connector configuration and may require custom deserialization logic if not natively supported by the managed connector for the given format.
- **Supabase Free Tier:** As above — verify `wal_level`, replication user privileges, and auto-pause behavior before committing to the CDC path.

## 3. Historical Data & Cutover Strategy

Since `order`, `order_item`, `return`, and `return_item` already have history flowing through the existing JSON-based pipeline, switching sources requires an explicit transition plan — not a silent swap:

1. **Baseline reconciliation:** Before cutover, confirm the last successfully ingested record (per entity) from the JSON/Auto Loader path, to establish a clean handoff point.
2. **Parallel run / validation window:** Run the new Postgres/Kafka ingestion path alongside the existing JSON path for a defined validation period. Compare row counts and key aggregates between the two to confirm the new path is capturing data correctly before decommissioning the old one.
3. **No re-backfill of history via the new path (default assumption):** Existing historical data in `raw`/`analytics` from the JSON pipeline is retained as-is; the new ingestion path is only responsible for data from the cutover point forward. If a full historical backfill from Supabase/Confluent is instead required (e.g., because the JSON dumps are known to be incomplete or inconsistent), this must be called out as a separate, explicit workstream with its own reconciliation logic — it is not assumed by default.
4. **Decommissioning:** Only disable the `file_arrival`-triggered JSON pipeline for the migrated entities once the validation window confirms parity, and after confirming no other consumers depend on the JSON dump process.

## 4. Implementation Steps

1. **Investigate constraints:** Validate Supabase's ability to support logical replication on the free tier; assess Confluent Cloud throughput/retention limits.
2. **Baseline reconciliation:** Capture last-ingested state per entity from the current JSON pipeline (see Section 3).
3. **Infrastructure (Terraform) updates:** Provision UC Connection objects for Postgres (Federation and/or CDC) and Kafka; update schema/grant definitions as needed per the `raw` reconciliation in Section 2C.
4. **Pipeline refactoring:**
   - Keep Auto Loader logic unchanged for `customer` and `product`.
   - Implement Lakehouse Federation or Lakeflow Connect CDC for Supabase entities, per the decision rule in Section 2A.
   - Implement Lakeflow Connect Managed Kafka or Structured Streaming for Confluent entities, per Section 2B.
5. **Parallel run:** Execute the validation window described in Section 3.
6. **Job/orchestration updates:** Adjust `trigger_raw_ingestion.yml` and related Asset Bundle definitions based on the final connector choices and their orchestration constraints (Section 2B note).
7. **Cutover:** Decommission the JSON path for migrated entities once validated.

## 5. Architectural Decisions Status

1. **Entity mapping (Resolved):**
   - **Supabase (PostgreSQL):** `orders`, `order_items`
   - **ADLS (Storage):** `orders`, `order_items`, `customers`, `products`, `returns`, `return_items`
   - **Confluent Cloud (Kafka Event Stream):** `returns`, `return_items`
2. **Postgres Ingestion Strategy (Resolved):**
   - **Lakehouse Federation (Zero-ETL / Managed Foreign Catalog)** configured with IPv4 session pooler (`aws-0-us-east-2.pooler.supabase.com:5432`) via Unity Catalog foreign catalog `supabase`.
   - Data is ingested into physical bronze tables in raw layer: `ecomm.raw.order__postgres` and `ecomm.raw.order_item__postgres`.
3. **Multi-Source Raw & Staging Architecture (Resolved):**
   - **Raw Layer Architecture:**
     - ADLS Auto Loader lands as-is in `ecomm.raw.order__adls` and `ecomm.raw.order_item__adls` via `ecomm_raw_pipeline`.
     - Supabase Postgres lands as-is in managed Delta tables `ecomm.raw.order__postgres` and `ecomm.raw.order_item__postgres` via the `sync_supabase_to_raw` task in `trigger_raw_ingestion`. This ensures they are standard managed Delta tables, enabling downstream Spark Structured Streaming.
   - **Staging Layer Harmonization & Deduplication:**
     - `ecomm.staging.order` and `ecomm.staging.order_item` union the cleaned, typed streams from both sources (`unionByName(..., allowMissingColumns=True)`).
     - Each record is tagged with `source_system` (`ADLS` vs `POSTGRES`).
     - Deduplication: When duplicate order IDs occur across sources, the most recent record (by `order_timestamp` / `source_date`) is stored using `dlt.apply_changes(stored_as_scd_type=1)`.
     - Invalid records missing mandatory fields or failing schema types are routed to `quarantine.order` and `quarantine.order_item`.
     - Change Data Feed (`delta.enableChangeDataFeed = true`) enabled on staging tables.
4. **Secret & Credential Management (Resolved):**
   - Public repository safety: No credentials or sensitive parameters committed to git.
   - Credentials (Supabase DB password, Confluent API key/secret) stored in **Azure Key Vault** (`kv-dbw-ecommerce`).
   - Terraform dynamically fetches secrets from Key Vault via data source `azurerm_key_vault_secret`.
5. **Data format & Schema Registry (Kafka):** Standard JSON payloads for `returns` and `return_items` events.