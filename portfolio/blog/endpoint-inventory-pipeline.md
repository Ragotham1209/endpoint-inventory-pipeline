# Building a 1M+ Endpoint Inventory Pipeline with Azure Databricks & Delta Lake

*By Ragotham Kanchi | Data Engineer*

In large enterprise environments, tracking the exact hardware specifications, installed software, and peripheral drivers across a fleet of laptops and desktops is notoriously difficult. Native tools often suffer from stale data, rigid reporting schemas, and an inability to track historical changes when a device changes hands.

This article details how I engineered an event-driven, serverless data pipeline on Microsoft Azure to ingest, process, and historize system telemetry from over **1,000,000 Windows endpoints** using a Medallion Architecture on Databricks Delta Lake.

---

## 🏗️ The Architecture

At a high level, the pipeline consists of three distinct phases:

1.  **Edge Collection:** A lightweight PowerShell agent running on the client machines.
2.  **Serverless Ingestion:** An auto-scaling Azure Function acting as the API Gateway.
3.  **Distributed Processing:** A PySpark job on Azure Databricks performing SCD Type 2 merges into Gold Delta tables.

<p align="center">
  <img src="../docs/architecture.png" alt="Architecture Diagram" width="800"/>
</p>

### Phase 1: The Edge Hash Gate

When managing 1M+ endpoints, bandwidth and API costs are major concerns. If every laptop uploads its full 5MB inventory JSON every day, we're needlessly processing terabytes of redundant identical data.

**The Solution:** Rather than blindly transmitting, the PowerShell agent computes a local `SHA-256` hash of the generated JSON payload. It compares this against a locally cached hash from the previous successful run.

*   If the hash matches (no new software installed, no hardware changes), the agent silently exits.
*   If the hash differs, the script transmits *only* the modified payload.

```powershell
# Compute Hash
$Bytes = [System.Text.Encoding]::UTF8.GetBytes($JsonPayload)
$Hasher = [System.Security.Cryptography.SHA256]::Create()
$HashBytes = $Hasher.ComputeHash($Bytes)
$CurrentHash = [BitConverter]::ToString($HashBytes) -replace '-'

if ($CurrentHash -ne $PreviousHash) {
    # Transmit to Azure API
}
```

This single engineering decision reduced our daily ingestion volume by **over 92%**.

### Phase 2: Absorbing the "Boot Storm"

In corporate environments, hundreds of thousands of users turn their computers on at 8:00 AM on Monday morning. This creates a massive, sudden spike in concurrent HTTP POST requests—a "boot storm."

To handle this, the API Gateway is built on **Azure Functions Elastic Premium (EP1)**. 

Unlike traditional App Service Plans which have a fixed number of VMs, the EP1 tier keeps a minimum number of instances eternally "warm" (preventing cold-start latency) and dynamically scales out to thousands of isolated micro-containers in seconds to absorb the traffic spike. Once the morning rush subsides, it mathematically scales back down, optimizing compute costs.

The Function App authenticates the payload via Function Keys and securely streams the raw JSON into our Azure Data Lake Storage Gen2 (ZRS) Landing Zone using its **System-Assigned Managed Identity**. No credentials, SAS tokens, or connection strings exist in the code.

### Phase 3: The Databricks Medallion Architecture

The core of the ETL process runs on a scheduled Azure Databricks Premium cluster with Unity Catalog enabled. The PySpark script (`scd2_processor.py`) navigates the data through the Medallion Architecture:

#### Bronze: Validation & Quarantine
The raw JSON is read from the ADLS Gen2 landing zone. Corrupt JSON payloads (flagged by PySpark's native `_corrupt_record` handler) are dynamically routed to a dedicated quarantine table. This prevents bad agent data from paralyzing the entire batch job.

#### Silver: Deduplication
Even with the edge hash gate, a highly mobile laptop might send 3 identical payloads in one day due to network connection retries. 

We use a PySpark Window function partitioned by `EndpointId` to deterministically select only the most recent payload for that execution cycle.

```python
windowSpec = Window.partitionBy("EndpointId").orderBy(col("IngestionTimestamp").desc())
silver_df = clean_df.withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") == 1).drop("rank")
```

Next, because endpoints transmit software and drivers as heavily nested JSON arrays, explicitly flattening them in the data lake (1NF) would mathematically multiply a single endpoint's physical storage by the number of applications installed (e.g., 73 apps = 73 redundant rows), causing massive storage bloat and exponentially increasing the `MERGE` execution duration.

To eliminate this 1-to-many data lake bloat, we natively preserve the nested JSON arrays as optimized Delta Lake `ARRAY<STRUCT<...>>` columns. This maintains an ultra-efficient footprint of strictly **1 physical row per endpoint**. The PowerBI developer simply clicks "Expand to New Rows" natively inside Power Query to dynamically flatten the arrays purely within the BI memory engine!

#### Gold: SCD Type 2 Delta Merge
To provide Power BI with accurate point-in-time reporting (e.g., "What software was installed on laptop XYZ *last month*?"), we implement Slowly Changing Dimension (SCD) Type 2 logic.

Using Delta Lake's native `MERGE` capabilities, we cross-reference the incoming payload hash against the active Gold table record. 

*   If the data is identical, we skip.
*   If the data changed, we expire the old record (`IsActive = False`, stamping `ValidTo = current_timestamp()`).
*   We insert the incoming data as a new active row.

```python
gold_table.alias("target").merge(
    source=staged_updates_with_expirations.alias("source"),
    condition="target.EndpointId = source.mergeKey and target.IsActive = true"
).whenMatchedUpdate(
    condition="source.PayloadHash != target.PayloadHash", 
    set={"IsActive": lit(False), "ValidTo": current_timestamp()}
).whenNotMatchedInsert(
    values=insert_dict
).execute()
```

## 🔐 Security & Governance First

The entire platform is deployed via zero-click Infrastructure as Code (Azure Bicep). Key security pillars include:

1.  **VNet Injection:** The Databricks cluster runs entirely within delegated public/private subnets inside the corporate Virtual Network. There is no public IP.
2.  **Unity Catalog:** All Delta tables are governed by UC, allowing us to implement granular row and column-level access controls for sensitive custom inventory data.
3.  **Zero Hardcoded Secrets:** All storage keys and API secrets are dynamically fetched from Azure Key Vault.

## 🏁 Conclusion

By combining the scalable ingestion of Azure Functions with the distributed processing power of PySpark and the transactional integrity of Delta Lake, this architecture easily handles the daily churn of 1M+ endpoints. 

It guarantees zero data loss during traffic spikes, maintains perfect historical lineage for compliance, and provides sub-second query performance for Power BI semantic models.

*Check out the source code on GitHub: [Ragotham1209/endpoint-inventory-pipeline](https://github.com/Ragotham1209/endpoint-inventory-pipeline)*
