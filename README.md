# Azure Cosmos DB RU-aware CSV Export (C#)

Production-focused **C# scripts** to export large volumes of data from **Azure Cosmos DB**
to **CSV** while respecting **RU limits**.

This repository provides lightweight, transparent tooling designed for **real production environments**:
- no secrets in code (env vars / CLI only)
- RU-aware throttling
- controlled batching and CSV splitting
- predictable and debuggable behavior

---

## Why this repository exists

Exporting large datasets from Cosmos DB is often painful:
- naive queries can quickly exhaust RU/s
- portal exports are limited or unstable for large volumes
- many examples do not address real-world operational constraints

These scripts aim to provide a **pragmatic toolbox** for DevOps, platform, and cloud engineers
who need **safe and controlled exports** from Cosmos DB.

---

## Typical use cases

- Large-scale data extraction
- Migrations or decommissioning
- Operational audits and reconciliations
- Building ID lists for downstream pipelines
- Controlled exports from production environments

---

## Scripts

### 1) Export by ID list (RU-aware)
**`ExportByIdListRuAware.cs`**

- Reads a list of IDs from a text file (one per line)
- Queries Cosmos DB in batches using `ARRAY_CONTAINS(@ids, UPPER(c.<idField>))`
- Outputs one CSV row per input ID (even if missing in DB)
- Splits CSV files by maximum row count
- Throttles automatically when RU consumption exceeds a threshold

---

### 2) Export scalar list from a query (RU-aware)
**`ExportScalarListRuAware.cs`**

- Loads a Cosmos SQL query from a `.sql` file
- The query must return a scalar value (e.g. `SELECT VALUE c.id FROM c WHERE ...`)
- Writes results to CSV
- Splits output files by size
- Applies RU-aware throttling

---

## Requirements

- .NET SDK 6+ (6, 7 or 8)
- NuGet package:
  - `Microsoft.Azure.Cosmos`

> Tip: use a minimal console project and drop the scripts into it.

---

## Configuration

No secrets are stored in code.

Configuration is provided via **environment variables** or **CLI arguments**.

### Required environment variables

- `COSMOS_CONNECTION_STRING`
- `COSMOS_DATABASE_ID`
- `COSMOS_CONTAINER_ID`

### Script-specific variables

- `COSMOS_IDS_FILE` (Export by ID list)
- `COSMOS_QUERY_FILE` (Export scalar list)
- `COSMOS_OUTPUT_DIR` (optional, default: current directory)
- `COSMOS_OUTPUT_PREFIX` (optional)

---

## Examples

### Example: Export scalar list of identifiers

This example exports a scalar list of identifiers using a Cosmos SQL query.

#### 1) Write a query file

Create `queries/example_ids.sql`:

```sql
SELECT VALUE c.id
FROM c
WHERE c.type = "example"
```
#### 2) Configure environment
```bash
export COSMOS_CONNECTION_STRING="AccountEndpoint=...;AccountKey=...;"
export COSMOS_DATABASE_ID="MyDatabase"
export COSMOS_CONTAINER_ID="MyContainer"
export COSMOS_QUERY_FILE="queries/example_ids.sql"
export COSMOS_OUTPUT_DIR="./out"
export COSMOS_OUTPUT_PREFIX="ids"
```
#### 3) Run
```bash
dotnet run -- --ru-threshold 8000 --max-rows 50000
```
### Example: Export by ID list
This example exports documents corresponding to a predefined list of IDs while respecting RU limits.

#### 1) Prepare an IDs file
Create ids.txt
ID_1
ID_2
ID_3

#### 2) Configure your environment
```bash
export COSMOS_CONNECTION_STRING="AccountEndpoint=...;AccountKey=...;"
export COSMOS_DATABASE_ID="MyDatabase"
export COSMOS_CONTAINER_ID="MyContainer"
export COSMOS_IDS_FILE="ids.txt"
export COSMOS_OUTPUT_DIR="./out"
export COSMOS_OUTPUT_PREFIX="items"
```

#### 3) Run
```bash
dotnet run -- --id-batch 500 --ru-threshold 8000 --max-rows 50000
```

## RU throttling notes
Cosmos DB reports RU consumption (RequestCharge) per query page.
These scripts apply a simple and production-friendly policy:
- RU consumption is logged for each page
- if the RU cost exceeds --ru-threshold, the script pauses for --ru-sleep-ms
- this prevents sustained RU spikes while keeping throughput predictable
This behavior is intentionally explicit and easy to adapt.

## Security
- No secrets are stored in code.
- All credentials must be provided via environment variables or CLI arguments.
- Never commit real connection strings, keys, or .env files.
- If a secret ever appears in Git history, revoke it immediately.
For production usage, prefer secure secret stores such as:
- Azure Key Vault
- GitHub Actions secrets
-   CI/CD secret managers
