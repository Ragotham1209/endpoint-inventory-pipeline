import os
import sys
from pyspark.sql import SparkSession

# Explicitly bind PySpark workers to the correct python executable to prevent "worker failed to connect" IPC errors on Windows
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

def main():
    adls_account = "invmsdn26v2lake"
    storage_key = os.environ.get("AZURE_STORAGE_KEY")
    
    if not storage_key:
        print("Error: AZURE_STORAGE_KEY environment variable not set.")
        return

    print("Initializing local JVM PySpark Session with Delta Lake & Azure Hadoop libraries...")
    
    # Initialize Spark session with Delta and Azure Active Directory / Hadoop packages
    spark = SparkSession.builder \
        .appName("Local_Gold_Reader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Inject the Data Lake Access Key directly into the Hadoop FileSystem configuration natively
    spark.conf.set(f"fs.azure.account.key.{adls_account}.dfs.core.windows.net", storage_key)

    payloads = ["hardware", "software", "drivers", "custom", "audit"]
    
    from pyspark.sql.functions import col, explode_outer
    from pyspark.sql.types import ArrayType, StructType

    for payload in payloads:
        print(f"\n=======================================================")
        print(f" GOLD TABLE: {payload.upper()}")
        print(f"=======================================================")
        path = f"abfss://gold@{adls_account}.dfs.core.windows.net/dim_{payload}/"
        try:
            df = spark.read.format("delta").load(path)
            
            # Print total physical row count before flattening (True Storage Cost)
            total_records = df.count()
            print(f"[*] True Storage Cost (Records in Data Lake): {total_records}")
            
            # -------------------------------------------------------------------
            # DYNAMIC READ-TIME FLATTENING (PowerBI Simulation)
            # -------------------------------------------------------------------
            for field in df.schema.fields:
                if isinstance(field.dataType, ArrayType):
                    print(f"[*] -> Dynamically exploding array column: {field.name}")
                    df = df.withColumn(field.name, explode_outer(col(field.name)))
                    
                    if hasattr(field.dataType.elementType, 'names'):
                        for sub_field in field.dataType.elementType.names:
                            df = df.withColumn(
                                f"{field.name}_{sub_field}", 
                                col(f"{field.name}.{sub_field}")
                            )
                        df = df.drop(field.name)

            if payload in ["software", "drivers"]:
                print(f"[*] Post-Flattening View (What PowerBI Gets): {df.count()} rows")
                df.show(5, truncate=100)
            else:
                df.orderBy("EndpointId").show(5, truncate=100)

        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"[-] No data found in Gold tier for '{payload}'. Databricks may have skipped it if Bronze was empty.")
            else:
                print(f"[!] Error reading {payload}: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
