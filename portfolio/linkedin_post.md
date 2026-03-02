Early on in my data engineering journey, I thought the job was just about getting data from Point A to Point B. Build the pipeline, make sure it doesn't break, and call it a day. 

Then I tried tracking telemetry for 1 million endpoints. 

I quickly realized that if I just brute-forced it and streamed everything directly into Azure Log Analytics, the ingestion bill would be completely insane—we're talking over $170k a month just for logs. That was a huge wake-up call for me. It taught me that it’s not just about building something that works; it’s about understanding the resources you’re consuming and engineering a way to do it efficiently.

I ended up scrapping the native logging approach entirely. Instead, I planned a custom architecture using serverless Azure Functions to land the JSON payloads in ADLS Gen2, and used Databricks (PySpark) to process the history using a Medallion architecture on Delta Lake. 

But even then, running a daily PySpark `MERGE` on 50 million historical records was taking almost an hour and chewing through expensive Databricks compute. 

So, I went back to the drawing board and dug into foundational Data Structures and Algorithms to optimize the actual code:
1. I swapped out the slow PowerShell array sorting on the laptops for O(1) .NET HashSets, stopping the edge scripts from draining user CPU.
2. I implemented Broadcast Hash Joins in PySpark to build in-memory hash maps, completely cutting out the massive network shuffle latency.
3. I used Z-Order Curves to mathematically group the Parquet files and added Bloom Filters, which let Databricks instantly skip reading 99% of the data lake.

The result? The daily Databricks ETL time dropped from an hour down to under 8 minutes, and the compute costs dropped by over 80%. 

It was an awesome learning experience. It showed me that writing code is really only half the job. Planning the architecture, forecasting the scale, and relentlessly optimizing your resources is what actually makes the whole system work in the real world.

I put together an animation of the data flow below, and the full code is on my portfolio: ragotham1209.github.io/endpoint-inventory-pipeline

![Architecture Data Flow Animation](dataflow_animation.gif)
