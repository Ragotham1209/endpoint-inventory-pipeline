🚀 **How I saved $2.1M/year and cut Databricks ETL time by 85% tracking 1M+ Endpoints**

Data Engineering isn't just about moving data from A to B; it's about doing it efficiently at scale. Recently, I completely overhauled the Enterprise Endpoint Inventory Pipeline that tracks hardware, software, and driver telemetry for over 1,000,000 Windows devices.

If we routed 2TB of daily JSON state directly into Azure Log Analytics, the ingestion bill alone would hit **~$179,000/month**. Instead, I built a serverless Azure Functions gateway to land the data in ADLS Gen2, and used Azure Databricks (PySpark) to maintain SCD Type 2 history in Delta Lake.

But PySpark can get expensive too if not tuned correctly. To drastically compress our Databricks compute costs, I implemented three core Data Structures & Algorithms (DSA) optimizations:

1️⃣ **O(N) HashSets at the Edge**
Rather than having 1M+ laptops run `Sort-Object -Unique` (an O(N log N) sorting process causing memory reallocation thrashing), I rewrote the PowerShell deduplication using pre-allocated `.NET List` and `HashSet` classes. This O(1) string matching saved **27+ hours of fleet CPU execution time daily**, entirely eliminating background lag.

2️⃣ **O(Log F) Z-Order Curves & O(1) Bloom Filters**
Instead of the PySpark engine executing an O(F) scan of every Parquet file in the data lake, I added an `OPTIMIZE ... ZORDER BY (EndpointId)` command and Bloom Filters on the `PayloadHash`. This mathematically clusters identical devices into space-filling curves and allows Databricks to deterministically skip 99% of file reads without parsing metadata.

3️⃣ **O(1) Broadcast Hash Joins**
Our daily SCD2 `MERGE` was previously executing an O(N log N) Sort-Merge-Join network shuffle of 50M+ rows. I wrapped the incoming daily payload stream in a `broadcast()` hint, forcing PySpark to build an O(1) HashMap in the RAM of all worker nodes. This completely severed the network latency.

💡 **The Result?**
ETL execution times plummeted from ~60 minutes to under 8 minutes per day. 
By heavily applying computer science fundamentals to cloud architecture, our monthly Databricks compute bill dropped by over 80%.

Check out the interactive Medallion Architecture data flow and the full open-source codebase in my portfolio! 👇

🔗 **Live Portfolio:** ragotham1209.github.io/endpoint-inventory-pipeline
🔗 **Source Code:** github.com/Ragotham1209/endpoint-inventory-pipeline

#DataEngineering #Azure #Databricks #PySpark #DeltaLake #BigData #Optimization
