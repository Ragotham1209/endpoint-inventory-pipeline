import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, rank
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SCD2_Processor")

def create_spark_session(adls_account, storage_account_key=None):
    """Initializes Spark Session with Delta Lake extensions and ADLS Gen2 auth."""
    builder = SparkSession.builder.appName("EndpointInventorySCD2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4")
        
    spark = builder.getOrCreate()
    
    if storage_account_key:
        # Databricks Interactive Notebooks ignore SparkSession.builder configs 
        # since the session already exists, so we explicitly set them on the live session.
        spark.conf.set(f"fs.azure.account.key.{adls_account}.dfs.core.windows.net", storage_account_key)
        spark.conf.set(f"fs.azure.account.key.{adls_account}.blob.core.windows.net", storage_account_key)
        
    return spark

def process_audit_batch(spark, raw_path, gold_path, quarantine_path=None):
    """
    Reads pure execution audit logs and strictly appends them. No Slow Changing Dimension logic used.
    """
    logger.info(f"[Audit] Reading raw data from: {raw_path}")
    
    from pyspark.sql.utils import AnalysisException
    try:
        raw_df = spark.read.json(raw_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logger.info(f"[Audit] Landing zone is empty (no recent files). Skipping.")
            return
        logger.error(f"[Audit] Analysis Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"[Audit] Critical Error reading landing zone: {e}")
        raise e

    if raw_df.count() == 0:
        logger.info(f"[Audit] Landing zone is empty. Nothing to process.")
        return

    # Intercept totally malformed JSON payloads that PySpark isolates into '_corrupt_record'
    if "_corrupt_record" in raw_df.columns:
        corrupt_count = raw_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            logger.warning(f"[Audit] Found {corrupt_count} malformed JSON records. Routing to quarantine.")
            if quarantine_path:
                raw_df.filter(col("_corrupt_record").isNotNull()) \
                    .withColumn("QuarantineReason", lit("Malformed JSON - PySpark _corrupt_record")) \
                    .write.format("delta").mode("append").save(quarantine_path)
            # Keep only the valid records for downstream processing
            raw_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            if raw_df.count() == 0:
                logger.info(f"[Audit] All records were malformed. Nothing left to process.")
                return

    # Check for mandatory fields
    if "EndpointId" not in raw_df.columns or "IngestionTimestamp" not in raw_df.columns:
        logger.error(f"[Audit] CRITICAL: Landing zone data is missing core schema components.")
        return

    clean_df = raw_df.filter(col("EndpointId").isNotNull() & (col("EndpointId") != ""))

    # Flatten the nested 'Data' JSON struct if it exists
    if "Data" in clean_df.columns:
        from pyspark.sql.types import StructType
        data_type = clean_df.schema["Data"].dataType
        if isinstance(data_type, StructType):
            for field in data_type.fields:
                clean_df = clean_df.withColumn(field.name, col(f"Data.{field.name}"))
            clean_df = clean_df.drop("Data")

    # Deduplicate multiple submissions of the exact same message if pipeline re-runs
    windowSpec = Window.partitionBy("MessageId").orderBy(col("IngestionTimestamp").desc())
    silver_df = clean_df.withColumn("rank", rank().over(windowSpec)) \
        .filter(col("rank") == 1) \
        .drop("rank")

    logger.info(f"[Audit] Executing APPEND INTO Gold Delta Table...")
    
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if not DeltaTable.isDeltaTable(spark, gold_path):
        logger.info(f"[Audit] Gold table does not exist. Initializing...")
        silver_df.write.format("delta").save(gold_path)
        logger.info(f"[Audit] Gold table initialized successfully.")
    else:
        # Merge by MessageId purely to prevent duplicate appends of the exact same API execution
        gold_table = DeltaTable.forPath(spark, gold_path)
        gold_table.alias("target").merge(
            source=silver_df.alias("source"),
            condition="target.MessageId = source.MessageId"
        ).whenNotMatchedInsertAll().execute()

    logger.info(f"[Audit] Processing complete.")

def process_inventory_batch(spark, payload_type, raw_path, silver_path, gold_path, quarantine_path):
    """
    Reads raw JSON from landing zone for a specific PayloadType, cleans it, and runs SCD2 merge to Gold.
    """
    logger.info(f"[{payload_type}] Reading raw data from: {raw_path}")
    
    from pyspark.sql.utils import AnalysisException
    try:
        raw_df = spark.read.json(raw_path)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logger.info(f"[{payload_type}] Landing zone is empty (no recent files). Skipping.")
            return
        logger.error(f"[{payload_type}] Analysis Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"[{payload_type}] Critical Error reading landing zone: {e}")
        raise e

    if raw_df.count() == 0:
        logger.info(f"[{payload_type}] Landing zone is empty. Nothing to process.")
        return

    logger.info(f"[{payload_type}] Performing data quality checks...")

    # Intercept totally malformed JSON payloads that PySpark isolates into '_corrupt_record'
    if "_corrupt_record" in raw_df.columns:
        corrupt_count = raw_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            logger.warning(f"[{payload_type}] Found {corrupt_count} malformed JSON records. Routing to quarantine.")
            raw_df.filter(col("_corrupt_record").isNotNull()) \
                .withColumn("QuarantineReason", lit("Malformed JSON - PySpark _corrupt_record")) \
                .write.format("delta").mode("append").save(quarantine_path)
            # Keep only the valid records for downstream processing
            raw_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            if raw_df.count() == 0:
                logger.info(f"[{payload_type}] All records were malformed. Nothing left to process.")
                return

    # Check for mandatory fields
    if "EndpointId" not in raw_df.columns or "IngestionTimestamp" not in raw_df.columns:
        logger.error(f"[{payload_type}] CRITICAL: Landing zone data is missing core schema components.")
        return

    # Split into clean vs. corrupted
    clean_df = raw_df.filter(col("EndpointId").isNotNull() & (col("EndpointId") != ""))
    corrupted_df = raw_df.filter(col("EndpointId").isNull() | (col("EndpointId") == ""))



    if corrupted_df.count() > 0:
        logger.warning(f"[{payload_type}] Found {corrupted_df.count()} corrupted records. Routing to quarantine.")
        corrupted_df.withColumn("QuarantineReason", lit("Missing EndpointId")) \
            .write.format("delta").mode("append").save(quarantine_path)

    logger.info(f"[{payload_type}] Deduplicating multiple submissions per endpoint...")
    
    windowSpec = Window.partitionBy("EndpointId").orderBy(col("IngestionTimestamp").desc())
    silver_df = clean_df.withColumn("rank", rank().over(windowSpec)) \
        .filter(col("rank") == 1) \
        .drop("rank")

    # Write to Silver staging table (keeps 'Data' struct nested)
    silver_df.write.format("delta").mode("overwrite").save(silver_path)

    # Flatten the nested 'Data' JSON struct specifically for the Gold layer
    if "Data" in silver_df.columns:
        from pyspark.sql.types import StructType, ArrayType
        from pyspark.sql.functions import explode_outer
        
        # Phase 1: Unpack the root Struct (e.g., pulls 'InstalledSoftware' out of 'Data')
        data_type = silver_df.schema["Data"].dataType
        if isinstance(data_type, StructType):
            for field in data_type.fields:
                silver_df = silver_df.withColumn(field.name, col(f"Data.{field.name}"))
            silver_df = silver_df.drop("Data")

    # Deterministically hash the actual payload data to prevent redundant SCD2 rows if identical data is sent
    from pyspark.sql.functions import sha2, to_json, struct
    exclude_cols = {"EndpointId", "IngestionTimestamp", "MessageId", "QuarantineReason", "IsActive", "ValidFrom", "ValidTo"}
    hash_cols = [c for c in sorted(silver_df.columns) if c not in exclude_cols]
    silver_df = silver_df.withColumn("PayloadHash", sha2(to_json(struct(*[col(c) for c in hash_cols])), 256))

    logger.info(f"[{payload_type}] Executing MERGE INTO Gold Delta Table (SCD Type 2)...")
    
    if not DeltaTable.isDeltaTable(spark, gold_path):
        logger.info(f"[{payload_type}] Gold table does not exist. Initializing...")
        silver_df.withColumn("IsActive", lit(True)) \
                 .withColumn("ValidFrom", current_timestamp()) \
                 .withColumn("ValidTo", to_timestamp(lit("9999-12-31 23:59:59"))) \
                 .write.format("delta").save(gold_path)
                 
        logger.info(f"[{payload_type}] Gold table initialized successfully.")
        return

    gold_table = DeltaTable.forPath(spark, gold_path)
    
    from pyspark.sql.functions import broadcast
    
    # Identify which records need to be expired because the actual payload data has changed
    # DSA Optimization: Use Broadcast Hash Join. Moves small 'stg' to node RAM for O(1) hash map lookups, eliminating O(N log N) network shuffle
    staged_updates = broadcast(silver_df.alias("stg")) \
        .join(
            spark.read.format("delta").load(gold_path).alias("gld"),
            (col("stg.EndpointId") == col("gld.EndpointId")) & (col("gld.IsActive") == True),
            "left"
        ) \
        .where(
            col("gld.EndpointId").isNull() | (col("stg.PayloadHash") != col("gld.PayloadHash"))
        ) \
        .selectExpr("NULL as mergeKey", "stg.*")

    staged_updates_with_expirations = silver_df.selectExpr("EndpointId as mergeKey", "*") \
        .unionByName(staged_updates)

    # Dynamically build the insert dictionary using Spark Column objects to avoid type inference issues
    insert_dict = {}
    for col_name in silver_df.columns:
        insert_dict[col_name] = col(f"source.{col_name}")
    insert_dict["IsActive"] = lit(True)
    insert_dict["ValidFrom"] = current_timestamp()
    insert_dict["ValidTo"] = to_timestamp(lit("9999-12-31 23:59:59"))

    # Enable native Delta Lake Schema Evolution to allow new JSON columns to automatically alter the table schema.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    gold_table.alias("target").merge(
        source=staged_updates_with_expirations.alias("source"),
        condition="target.EndpointId = source.mergeKey and target.IsActive = true"
    ).whenMatchedUpdate(
        condition="source.PayloadHash != target.PayloadHash", 
        set={"IsActive": lit(False), "ValidTo": current_timestamp()}
    ).whenNotMatchedInsert(
        values=insert_dict
    ).execute()

    logger.info(f"[{payload_type}] Running Storage Maintenance (OPTIMIZE and VACUUM)...")
    try:
        # DSA Optimization: Z-Order Curves. Physically clusters Parquet files mathematically to drop file scanning complexity from O(F) to O(log F)
        gold_table.optimize().executeZOrderBy("EndpointId")
        # Delete old, unreferenced parquet files from the data lake older than 7 days (168 hours)
        gold_table.vacuum(168)
        logger.info(f"[{payload_type}] Storage Maintenance complete.")
    except Exception as e:
        logger.warning(f"[{payload_type}] Storage Maintenance skipped or failed: {e}")

    logger.info(f"[{payload_type}] SCD Type 2 processing complete.")

# -----------------------------------------------------------------------------------------
# DATABRICKS PRODUCTION JOB EXECUTION
# -----------------------------------------------------------------------------------------
import sys

TARGET_PAYLOAD = None
ADLS_ACCOUNT = None
STORAGE_KEY = None

# If running as a Scheduled Job, Databricks passes these as command-line arguments
if "--type" in sys.argv:
    import argparse
    parser = argparse.ArgumentParser(description="Process SCD2 Data")
    parser.add_argument("--type", required=True, help="Payload Type (e.g. hardware)")
    parser.add_argument("--account", required=True, help="ADLS Gen2 Account Name")
    parser.add_argument("--key", required=False, help="ADLS Gen2 Storage Key", default=None)
    args, unknown = parser.parse_known_args()
    
    TARGET_PAYLOAD = args.type.strip().lower()
    ADLS_ACCOUNT = args.account.strip()
    STORAGE_KEY = args.key.strip() if args.key else None
else:
    # If running interactively, Databricks uses Widgets for UI input
    try:
        dbutils.widgets.text("payload_input", "hardware", "Payload Type to Process")
        dbutils.widgets.text("adls_account", "", "ADLS Gen2 Account Name")
        dbutils.widgets.text("storage_key", "", "ADLS Gen2 Storage Key")

        TARGET_PAYLOAD = dbutils.widgets.get("payload_input").strip().lower()
        ADLS_ACCOUNT = dbutils.widgets.get("adls_account").strip()
        STORAGE_KEY = dbutils.widgets.get("storage_key").strip()
    except Exception as e:
        logger.error("Failed to parse both args and widgets. Check your Databricks cluster configuration.")
        raise ValueError("Missing parameters: Please provide either command-line args or Databricks Widgets.")

if not ADLS_ACCOUNT:
    raise ValueError("ADLS_ACCOUNT cannot be empty. Please provide your storage account name.")

if STORAGE_KEY == "" or str(STORAGE_KEY).lower() == "none":
    STORAGE_KEY = None
else:
    import urllib.parse
    import re
    # 1. Strip accidental visible quotes that the Databricks UI might silently inject
    clean_key = str(STORAGE_KEY).strip().strip('"').strip("'")
    
    # 2. If the UI URL-encoded the payload (e.g. converting '==' to '%3D%3D'), decode it back to native Base64
    clean_key = urllib.parse.unquote(clean_key)
    
    # 3. If you accidentally pasted the entire Connection String, automatically extract just the AccountKey
    if "AccountKey=" in clean_key:
        match = re.search(r"AccountKey=([^;]+)", clean_key)
        if match:
            clean_key = match.group(1)
            
    STORAGE_KEY = clean_key.strip()
    logger.info(f"Storage Key parsed. Length: {len(STORAGE_KEY)} characters (should be 88 for ADLS Gen2).")

logger.info(f"=======================================================")
logger.info(f"STARTING PIPELINE RUN FOR PAYLOAD: {TARGET_PAYLOAD.upper()}")
logger.info(f"=======================================================")
        
base_abfss = f"abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net"
raw_path = f"{base_abfss}/{TARGET_PAYLOAD}/*/*/*/*.json"
silver_path = f"abfss://silver@{ADLS_ACCOUNT}.dfs.core.windows.net/{TARGET_PAYLOAD}_staging/"
gold_path = f"abfss://gold@{ADLS_ACCOUNT}.dfs.core.windows.net/dim_{TARGET_PAYLOAD}/"
quarantine_path = f"abfss://quarantine@{ADLS_ACCOUNT}.dfs.core.windows.net/{TARGET_PAYLOAD}_bad/"

# For testing, we pass the parsed STORAGE_KEY to securely authenticate against the datalake.
# In a true Azure Production environment (Premium Databricks tier + Unity Catalog), 
# this key would be omitted entirely since it relies on Managed Identity Azure AD Passthrough.
spark = create_spark_session(ADLS_ACCOUNT, STORAGE_KEY)

if TARGET_PAYLOAD.lower() == "audit":
    process_audit_batch(spark, raw_path, gold_path, quarantine_path)
else:
    process_inventory_batch(spark, TARGET_PAYLOAD.capitalize(), raw_path, silver_path, gold_path, quarantine_path)
