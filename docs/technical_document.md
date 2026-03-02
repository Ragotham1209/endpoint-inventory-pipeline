# Enterprise Endpoint Inventory Pipeline — Technical Document
**Author:** Ragotham Kanchi

## 1. Cloud Architecture & Scalability
This pipeline is engineered using Microsoft Azure's PaaS and Serverless offerings to provide infinite scalability while mathematically scaling idle costs down to $0.00/hour.

### The Ingestion Layer: Azure Functions
- **Technology:** `Python 3.11` Linux Function App.
- **MSDN (Cost-Optimized):** Deployed on a `Y1 Dynamic` (Consumption) Linux plan. It optimizes for zero-dollar idle cost but suffers from cold-starts and limits execution time to 5 minutes.
- **Production (SLA-Backed):** Deployed on an `Elastic Premium (EP1)` plan. This tier keeps minimum instances eternally "warm" (zero cold start delay) and allows for complex VNet integration (IP Firewalls) while still dynamically auto-scaling to thousands of containers to handle 8:00 AM boot-storm traffic.

### The Storage Layer: Azure Data Lake Storage Gen2
- **Technology:** StorageV2 with Hierarchical Namespace Enabled.
- **MSDN (Cost-Optimized):** `Standard_LRS` (Locally Redundant Storage). Data is written to a single localized datacenter rack.
- **Production (SLA-Backed):** `Standard_ZRS` (Zone Redundant Storage) or `GRS` (Geo-Redundant). This protects the multi-petabyte Gold Parquet analytics from a complete datacenter blackout.
- **Why ADLS Gen2?** The Databricks Delta Lake framework relies on atomic rename capabilities at the filesystem level. Standard Blob Storage cannot support Delta transaction logs (`_delta_log`), resulting in data corruption. ADLS Gen2 provides true POSIX directory architecture.

### The Processing Engine: Azure Databricks (Job Compute)
- **Technology:** Apache Spark `14.3 LTS (Scala 2.12)`.
- **MSDN (Cost-Optimized):** A severely constrained Single-Node cluster utilizing `Standard_D4s_v3` (4 vCPUs) instances to respect the strict 10-core Datacenter family quotas. Unity Catalog is purposefully disabled (`data_security_mode="NONE"`) to skirt strict namespace writes.
- **Production (SLA-Backed):** A massively parallel Multi-Node cluster utilizing `Standard_D8ds_v4` nodes. Unity Catalog is strictly enabled (`USER_ISOLATION`) to enforce granular row/column access control policies, and the nodes dynamically Autoscale between 2 to 8 workers based on payload ingestion volume.

## 2. Telemetry and Enterprise Logging
A critical requirement for enterprise SLA monitoring is deeply integrated failure logging.
- **Azure Functions:** The `function_app.py` script routes standard Python `logging` directly to **Azure Application Insights**. All `202 Accepted` and `500 Server Error` results are natively indexed. 
- **Azure Databricks:** The `scd2_processor.py` script utilizes standard Python `logging` formatting (`%(asctime)s - %(levelname)s - %(message)s`). Databricks inherently hooks into the stdout pipeline of this module, capturing and indexing these timestamps directly into the **Driver Logs**.

## 3. Deployment Security Posture
The entire architecture is deployed using a zero-click Infrastructure as Code (Bicep) configuration. Security configurations included:
1. **System-Assigned Managed Identity:** The Azure Function App relies explicitly on a Microsoft Entra ID Managed Identity. There are no connection strings, SAS tokens, or static passwords hardcoded anywhere in the codebase.
2. **Role-Based Access Control (RBAC):** The Bicep script dynamically issues the `Storage Blob Data Contributor` role to the Function App's Service Principal ID.
3. **Private Subnets:** The Databricks workspace is securely injected into a dedicated Virtual Network with `public` and `private` delegation subnets.

## 4. PySpark Data Processing Engine (ETL)
The core `scd2_processor.py` Databricks notebook executes highly specialized algorithms to parse, flatten, and historize the raw JSON telemetry:

1. **Two-Tier Hardware Deduplication:** To prevent analytic tables from bloating with identical payload submissions, the engine employs a two-layer defense. 
   - *Intra-Batch Filtering:* A PySpark Window function identifies and selects only the single most recent payload for a given machine within that execution cycle.
   ```python
   windowSpec = Window.partitionBy("EndpointId").orderBy(col("IngestionTimestamp").desc())
   silver_df = clean_df.withColumn("rank", rank().over(windowSpec)) \
       .filter(col("rank") == 1).drop("rank")
   ```
   - *Cross-Batch Hashing:* PySpark computes a deterministic `SHA-256` hash of the fully parsed payload. During the final Delta Merge, it silently discards the incoming row if the hardware, software, or drivers identically match the active Gold table record (`source.PayloadHash != target.PayloadHash`).
   ```python
   exclude_cols = {"EndpointId", "IngestionTimestamp", "MessageId", "QuarantineReason", "IsActive", "ValidFrom", "ValidTo"}
   hash_cols = [c for c in sorted(silver_df.columns) if c not in exclude_cols]
   silver_df = silver_df.withColumn("PayloadHash", sha2(to_json(struct(*[col(c) for c in hash_cols])), 256))
   ```

2. **Native Delta Array Structures (`ARRAY<STRUCT>`):** Endpoints transmit software and driver lists as heavily nested JSON arrays. Explicitly flattening them in PySpark (First Normal Form) would mathematically multiply a single endpoint's physical storage by the number of applications installed (e.g., 73 apps = 73 redundant rows). To permanently eliminate this data lake bloat, the processor preserves the arrays natively as Gold Delta Lake `ARRAY<STRUCT>` columns, maintaining an ultra-efficient 1-to-1 footprint. Power BI natively expands these lists at read-time.

3. **Graceful Error Interception:** Databricks standard behavior throws hard crash exceptions when instructed to read an empty directory or when encountering a rotating access key (`403 Access Denied`). The PySpark logic wraps the Spark engine in custom exception handlers—allowing the cluster to silently skip missing data.
   ```python
    try:
        raw_df = spark.read.json(raw_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logger.info("Landing zone is empty (no recent files). Skipping.")
            return # Gracefully aborts this payload iteration without crashing the job
   ```
