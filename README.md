# Azure Cosmos DB RU-aware CSV Export (C#)

Production-focused **C# scripts** to export large volumes of data from **Azure Cosmos DB** to **CSV** while respecting **RU limits**.

This repository is intentionally lightweight and transparent:
- no secrets in code (env/args only)
- query logic loaded from files (no business logic hardcoded)
- controlled batching and CSV splitting
- RU-aware throttling (sleep when RU consumption spikes)

---

## Why this repo exists

Exporting large datasets from Cosmos DB is often painful:
- naive queries can quickly exhaust RU/s
- portal exports are limited or unstable for large volumes
- most examples do not address real-world operational constraints

These scripts aim to be a pragmatic toolbox for **DevOps / SRE / platform** and **data migration** scenarios.

---

## Typical use cases

- Large-scale data extraction for migrations or decommissioning
- Operational audits (inventory, mapping, reconciliation)
- Controlled exports in production environments
- Building input lists (IDs) for other pipelines

---

## Scripts

### 1) Export by ID list (RU-aware)
**`src/ExportByIdListRuAware.cs`**

- Reads IDs from a file (one per line)
- Runs a query in batches using `ARRAY_CONTAINS(@ids, UPPER(c.<idField>))`
- Outputs a CSV with one row per input ID (even if missing in DB)
- Splits CSV files by `--max-rows`
- Throttles when `RequestCharge` exceeds a threshold

### 2) Export scalar list from a query (RU-aware)
**`src/ExportScalarListRuAware.cs`**

- Loads a query from a `.sql` file
- The query should return a scalar `VALUE` (e.g., `SELECT VALUE c.vin FROM c WHERE ...`)
- Outputs results to CSV
- Splits files and throttles based on RU

---

## Requirements

- .NET SDK 6+ (or 7/8)
- NuGet package:
  - `Microsoft.Azure.Cosmos`

> Tip: use a minimal console project and drop the scripts into `src/`.

---

## Configuration

No secrets in code. Provide config via **environment variables** or **CLI args**.

### Environment variables

Required:
- `COSMOS_CONNECTION_STRING`
- `COSMOS_DATABASE_ID`
- `COSMOS_CONTAINER_ID`

Additional (depending on script):
- `COSMOS_IDS_FILE` (for ExportByIdListRuAware)
- `COSMOS_QUERY_FILE` (for ExportScalarListRuAware)
- `COSMOS_OUTPUT_DIR` (optional)
- `COSMOS_OUTPUT_PREFIX` (optional)

---

## Quick start

### Create a console project

```bash
dotnet new console -n cosmos-export
cd cosmos-export
dotnet add package Microsoft.Azure.Cosmos
