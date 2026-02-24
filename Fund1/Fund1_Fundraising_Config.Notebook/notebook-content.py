# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Default configuration

# PARAMETERS CELL ********************

silver_lakehouse_name = "Fund1_Fundraising_SL"
gold_lakehouse_name = "Fund1_Fundraising_GD"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime, timezone
from itertools import product
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import lit, max
import logging
from typing import Optional
import pandas as pd
import uuid

# Set Spark configuration
spark.conf.set("spark.sql.caseSensitive", True)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Global functions

# CELL ********************

def get_full_table_name(lakehouse: str, table_name: str):
    return f"{lakehouse}.{table_name}"

def get_table_from_lakehouse(lakehouse: str, table_name: str):
    return spark.read.table(get_full_table_name(lakehouse, table_name))

def get_silver_table(table_name: str):
    return get_table_from_lakehouse(silver_lakehouse_name, table_name)

def get_gold_table(table_name: str):
    return get_table_from_lakehouse(gold_lakehouse_name, table_name)

def get_lakehouse_table_counts(lakehouse_name: str, tables: List[str]):
    """
    Returns a Spark DataFrame with table names, record counts, and any errors
    from a Microsoft Fabric Lakehouse, based on a provided list of table names.

    :param lakehouse_name: Name of the Lakehouse (used as the Spark catalog)
    :param tables: List of table names to check
    :return: Spark DataFrame with columns: Table, RecordCount, Error
    """

    results: List[Dict[str, Optional[str]]] = []

    for table in tables:
        try:
            full_table_name = get_full_table_name(lakehouse_name, table)
            count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]["count"]
            
            results.append({
                "Table": table,
                "RecordCount": count,
                "Error": None
            })
        except Exception as e:
            results.append({
                "Table": table,
                "RecordCount": None,
                "Error": str(e)
            })

    # Convert to pandas then to Spark DataFrame with explicit schema
    df_pd = pd.DataFrame(results)

    schema = StructType([
        StructField("Table", StringType(), True),
        StructField("RecordCount", LongType(), True),
        StructField("Error", StringType(), True),
    ])

    return spark.createDataFrame(df_pd, schema=schema)

def table_exists(full_table_name: str) -> bool:
    """
    Checks whether a table exists in Spark catalog.

    Args:
        full_table_name (str): Fully qualified table name, e.g. "lakehouse.table_name"

    Returns:
        bool: True if table exists, False otherwise.
    """
    return spark.catalog.tableExists(full_table_name)

def have_same_columns(df1: DataFrame, df2: DataFrame) -> bool:
    """
    Checks if two DataFrames have the same set of columns (names only).
    Order does not matter.
    """
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    return cols1 == cols2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Change Data Feed functions

# MARKDOWN ********************

# The transformation notebook must be attached to the source lakehouse for correct working of `DESRIBE HISTORY`.

# CELL ********************

from dataclasses import dataclass
from typing import Callable, List, Optional
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

CDF_STATE_TABLE_NAME = "ChangeDataFeedState"

@dataclass
class CdfTable:
    source_lakehouse: str
    target_lakehouse: str
    
    columns: List[str]
    merge_sql_template: str
    enrich_func: Optional[Callable[[DataFrame], DataFrame]] = None

    source_table_name: Optional[str] = None
    source_primary_key: Optional[str] = None
    target_table_name: Optional[str] = None

    name: Optional[str] = None   # DELETE
    primary_key: Optional[str] = None # DELETE

    hard_delete: bool = False
    delete_on: Optional[str] = None

    # optional hook to refine/validate delete keys (Id, SourceId)
    delete_keys_func: Optional[Callable[[DataFrame, "CdfTable"], DataFrame]] = None

    # technical mapping hook (required if source PK != target PK)
    delete_key_mapper: Optional[Callable[[DataFrame, "CdfTable"], DataFrame]] = None

def EnsureChangeDataFeedStateTable(lakehouse_name: str):
    full_table_name = f"{lakehouse_name}.{CDF_STATE_TABLE_NAME}"

    if not table_exists(full_table_name):
        logging.info(f"✅ Creating table: {full_table_name}")
        
        schema = StructType([
            StructField("source_table_name", StringType(), True),
            StructField("target_table_name", StringType(), True),
            StructField("last_processed_commit_version", LongType(), True),
            StructField("last_processed_timestamp", TimestampType(), True)
        ])

        empty_df = spark.createDataFrame([], schema)

        empty_df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(full_table_name)
    else:
        logging.info(f"ℹ️ Table already exists: {full_table_name}")

def GetLastProcessedDataVersion(target_lakehouse_name: str, source_table_name: str, target_table_name: str):
    fullHistoryDf = spark.sql(f"DESCRIBE HISTORY {source_table_name}")

    cdf_state_df = spark.read.format("delta") \
        .table(f"{target_lakehouse_name}.{CDF_STATE_TABLE_NAME}") \
        .where((col("source_table_name") == source_table_name) & 
               (col("target_table_name") == target_table_name))

    lastProcessedEntityVersion = (
        cdf_state_df
        .select(max("last_processed_commit_version"))
        .collect()[0][0]
    )

    nextVersionToProcess = lastProcessedEntityVersion + 1 if lastProcessedEntityVersion is not None else 0
    latestMaxCommitVersion = fullHistoryDf.select(max("version")).collect()[0][0]

    return (nextVersionToProcess, latestMaxCommitVersion)

def UpdateChangeDataFeedTable(target_lakehouse_name: str, source_table_name: str, target_table_name: str, latest_max_commit_version: int):
    logging.info(f"✅ Updating {CDF_STATE_TABLE_NAME} for: {source_table_name} → {target_table_name} in {target_lakehouse_name}")
    current_timestamp_val = datetime.now(timezone.utc)

    spark.createDataFrame(
        [(source_table_name, target_table_name, latest_max_commit_version, current_timestamp_val)],
        ["source_table_name", "target_table_name", "last_processed_commit_version", "last_processed_timestamp"]
    ).write.mode("append").format("delta").saveAsTable(f"{target_lakehouse_name}.{CDF_STATE_TABLE_NAME}")

def get_source_id_by_name(source_name: str, lakehouse: str = silver_lakehouse_name) -> str:
    """
    Look up the SourceId (GUID) from Silver.Source by source name.
    """
    df = spark.read.table(f"{lakehouse}.Source").filter(col("Name") == source_name)

    if df.isEmpty():
        raise ValueError(f"❌ Source with name '{source_name}' not found in {lakehouse}.Source")

    return df.select("SourceId").first()["SourceId"]

from pyspark.sql import functions as F
import logging 


def log_merge_metrics(table_fullname: str, label: str):
    """
    Logs inserted/updated/deleted counts from the most recent MERGE on `table_fullname`.
    Works with runtimes that report either:
      - numTargetRowsNotMatchedBySourceDeleted, or
      - numTargetRowsDeleted
    """
    row = (spark.sql(f"DESCRIBE HISTORY {table_fullname}")
             .orderBy(F.col("version").desc())
             .limit(1)
             .select("operation", "operationMetrics")
             .collect()[0])

    op = row["operation"]
    metrics = row["operationMetrics"] or {}

    inserted = int(metrics.get("numTargetRowsInserted", "0"))
    updated  = int(metrics.get("numTargetRowsUpdated",
                               metrics.get("numTargetRowsMatchedUpdated", "0")))
    deleted  = int(metrics.get("numTargetRowsNotMatchedBySourceDeleted",
                               metrics.get("numTargetRowsDeleted", "0")))

    logging.info(f"📊 {label} MERGE: inserted={inserted}, updated={updated}, deleted={deleted}.")

def _is_silver(lakehouse_name: str) -> bool:
    try:
        return lakehouse_name.lower() == silver_lakehouse_name.lower()
    except NameError:
        return "silver" in lakehouse_name.lower()

def _is_gold(lakehouse_name: str) -> bool:
    try:
        return lakehouse_name.lower() == gold_lakehouse_name.lower()
    except NameError:
        return "gold" in lakehouse_name.lower()

def _default_delete_predicate_for_target(target_lakehouse: str, source_primary_key: str) -> str:
    """
    Silver ⇒ identity by (SourceId, SourceSystemId) with *matching names* on k.
    Gold   ⇒ identity by table PK name (e.g., t.CampaignId = k.CampaignId).
    """
    if _is_silver(target_lakehouse):
        return "t.SourceId = k.SourceId AND t.SourceSystemId = k.SourceSystemId"
    if not source_primary_key:
        raise ValueError("Gold default delete needs source_primary_key (e.g., CampaignId).")
    return f"t.{source_primary_key} = k.{source_primary_key}"

def _predicate_needs(colname: str, predicate_sql: str) -> bool:
    return f"k.{colname}" in (predicate_sql or "")

from datetime import datetime, timezone
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, max

def ProcessCdfTable(table: CdfTable, source_name: Optional[str] = None):
    # 0) Resolve names/paths
    source_table_name = table.source_table_name or getattr(table, "name", None)
    source_primary_key = table.source_primary_key or getattr(table, "primary_key", None)
    target_table_name  = table.target_table_name  or getattr(table, "name", None)

    if not source_table_name or not target_table_name or not source_primary_key:
        raise ValueError("CdfTable must define source_table_name, target_table_name, and source_primary_key.")

    entity_path = f"{table.source_lakehouse}.{source_table_name}"
    temp_view   = f"latestSnapshot_{source_table_name}"

    # 1) CDF window
    next_version, latest_commit = GetLastProcessedDataVersion(
        table.target_lakehouse, source_table_name, target_table_name
    )
    if next_version > latest_commit:
        logging.info(f"✅ {target_table_name} is up to date.")
        return

    logging.info(f"🔄 Processing {target_table_name} from {table.source_lakehouse} → {table.target_lakehouse} ({next_version} to {latest_commit})")

    # 2) Read CDF for the window (+ PK)
    df = (
        spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", next_version)
            .option("endingVersion",   latest_commit)
            .table(entity_path)
            .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
            .select(*table.columns, source_primary_key, "_commit_version", "_change_type")
    )

    # 3) Split changes
    active_df = df.filter(col("_change_type").isin("insert", "update_postimage"))
    delete_df = (
        df.filter(col("_change_type") == "delete")
          .select(
              col(source_primary_key).alias(source_primary_key),
              col("_commit_version").alias("del_ver")
          )
          .dropDuplicates([source_primary_key, "del_ver"])
    )

    # 4) ----------------- DELETE FIRST -----------------
    if getattr(table, "hard_delete", False) and not delete_df.rdd.isEmpty():
        # 4a) Resolve predicate (explicit > default)
        source_pk = source_primary_key
        predicate = table.delete_on or _default_delete_predicate_for_target(
            table.target_lakehouse, source_pk
        )

        # 4b) Build keys matching the predicate (Silver vs Gold vs fallback)
        if _predicate_needs("SourceSystemId", predicate) and _predicate_needs("SourceId", predicate):
            # Silver default: need k.SourceSystemId + k.SourceId
            if not source_name:
                raise ValueError(
                    "source_name is required because the delete predicate uses k.SourceId "
                    f"(predicate: {predicate})"
                )
            src_id = get_source_id_by_name(source_name)
            keys = (
                delete_df.select(col(source_pk).alias("SourceSystemId"))
                         .withColumn("SourceId", lit(src_id))
                         .dropDuplicates()
            )

        elif _predicate_needs(source_pk, predicate):
            # Gold default: need k.<PK> (e.g., k.CampaignId)
            keys = delete_df.select(col(source_pk).alias(source_pk)).dropDuplicates()

        else:
            # Fallback:
            # - if predicate needs k.Id, alias PK → "Id"
            # - else, keep the original PK name (so a mapper can remap it later)
            base_col_name = "Id" if _predicate_needs("Id", predicate) else source_pk
            keys = delete_df.select(col(source_pk).alias(base_col_name)).dropDuplicates()

            # attach k.SourceId if the predicate asks for it
            if _predicate_needs("SourceId", predicate):
                if not source_name:
                    raise ValueError(
                    "source_name is required because the delete predicate uses k.SourceId "
                    f"(predicate: {predicate})"
                    )
                src_id = get_source_id_by_name(source_name)
                keys = keys.withColumn("SourceId", lit(src_id))


        base_cnt = keys.count()

        # 4c) Technical key mapping (e.g., ContactId → ConstituentId)
        if table.delete_key_mapper is not None and _is_gold(table.target_lakehouse):
            keys = table.delete_key_mapper(keys, table)
             

        # 4d) Customer policy filter (only in Silver, optional)
        if table.delete_keys_func is not None and _is_silver(table.target_lakehouse):
            keys_before = base_cnt
            filtered_keys = table.delete_keys_func(keys, table)

            if filtered_keys is None:
                logging.info(f"ℹ️ Delete policy returned None for {table.target_lakehouse}.{target_table_name} → skipping policy")
            else:
                keys = filtered_keys
                keys_after = keys.count()
                excluded = keys_before - keys_after
                if excluded > 0:
                    logging.info(f"🛡️ Delete policy excluded {excluded} key(s) for {table.target_lakehouse}.{target_table_name}")

             

        # 4e) Execute MERGE-DELETE
        if keys.rdd.isEmpty():
            logging.info(f"ℹ️ No keys to delete for {table.target_lakehouse}.{target_table_name} after mapping/policy")
        else:
            keys.createOrReplaceTempView("deleted_keys")
            target_fullname = f"{table.target_lakehouse}.{target_table_name}"

            delete_count = spark.sql(f"""
                SELECT COUNT(*) AS cnt
                FROM {target_fullname} AS t
                JOIN deleted_keys AS k
                  ON {predicate}
            """).collect()[0]["cnt"]

            if delete_count > 0:
                spark.sql(f"""
                    MERGE INTO {target_fullname} AS t
                    USING deleted_keys AS k
                    ON {predicate}
                    WHEN MATCHED THEN DELETE
                """)
                logging.info(f"🗑️ {target_table_name}: Deleted {delete_count} row(s).")
            else:
                logging.info(f"ℹ️ {target_table_name}: no matching rows to delete.")

            spark.catalog.dropTempView("deleted_keys")
    # --------------- end delete -----------------------

    # 5) Build latest snapshot ONLY from active rows (last image per PK)
    # Suppress any active image that is not newer than the delete for that PK
    w = Window.partitionBy(source_primary_key)

    # Latest active image per PK with its commit version
    active_latest = (
        active_df
        .withColumn("act_ver", F.max("_commit_version").over(w))
        .filter(col("_commit_version") == col("act_ver"))
        # keep act_ver for the comparison with delete version
    )

    # Latest delete version per PK (if any)
    del_latest = (
        delete_df
        .groupBy(source_primary_key)
        .agg(F.max("del_ver").alias("del_ver"))
    )

    # Keep active if there is no delete for the PK, or the active is strictly newer than the delete
    df_snapshot = (
        active_latest.alias("a")
        .join(del_latest.alias("d"), on=source_primary_key, how="left")
        .filter( (col("d.del_ver").isNull()) | (col("a.act_ver") > col("d.del_ver")) )
        .drop("act_ver", "del_ver", "_change_type", "_commit_version")
    )


    #df_snapshot = (
    #    active_df.withColumn("maxCommit", max("_commit_version").over(w))
    #             .filter(col("_commit_version") == col("maxCommit"))
    #             .drop("maxCommit", "_change_type", "_commit_version")
    #)

    # 6) Enrich 
    src_cnt = df_snapshot.count()
    if table.enrich_func:
        logging.info(f"➡️ Enriching {source_table_name} → {target_table_name} ({src_cnt} source rows).")
        df_snapshot = table.enrich_func(df_snapshot)
        tgt_cnt = df_snapshot.count()
        msg = "same" if src_cnt == tgt_cnt else f"changed ({src_cnt} → {tgt_cnt})"
        logging.info(f"ℹ️ Enrichment row count {msg}.")

    # 7) Merge snapshot into target
    df_snapshot.createOrReplaceTempView(temp_view)
    if table.merge_sql_template and table.merge_sql_template.strip():
        spark.sql(table.merge_sql_template)
        logging.info(f"✅ Updated {target_table_name}.")
    else:
        logging.info(f"⚠️ Skipping merge for {target_table_name} – merge_sql_template is empty or None.")

    # 8) Advance watermark
    UpdateChangeDataFeedTable(table.target_lakehouse, source_table_name, target_table_name, latest_commit)
    spark.catalog.dropTempView(temp_view)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data model functions

# CELL ********************

from abc import ABC, abstractmethod
import concurrent.futures
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, LongType
from pyspark.sql.functions import expr, monotonically_increasing_id
from typing import Tuple, Optional

class BaseDataModel(ABC):
    def __init__(self, spark: SparkSession, lakehouse: str):
        self.spark = spark
        self.lakehouse = lakehouse

    @abstractmethod
    def get_entities(self) -> list[Tuple[StructType, str, Optional[bool]]]:
        """
        Must return a list of (schema, table_name) tuples
        """
        pass
    
    def create_entities(self):
        """
        Create all tables concurrently.
        """
        entities = self.get_entities()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.create_table, schema, table_name, enable_change_data_feed)
                for schema, table_name, enable_change_data_feed in entities
            ]

            concurrent.futures.wait(futures)

            errors = []
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    errors.append(e)

            if errors:
                error_msgs = "\n".join(str(e) for e in errors)
                raise Exception(f"One or more table creations failed:\n{error_msgs}")

    def add_primary_key(self, df: DataFrame, schema: StructType) -> (DataFrame, str | None):
        pk_field = next(
            (f for f in schema.fields if f.metadata.get("primaryKey", False)),
            None
        )
        if pk_field is None:
            return df, None

        name, dtype, kind = pk_field.name, pk_field.dataType, pk_field.metadata.get("pkType")

        # Generate or validate values
        if kind == "guid":
            df = df.withColumn(name, expr("uuid()"))
        elif isinstance(dtype, LongType):
            df = df.withColumn(name, monotonically_increasing_id())
        # Other types: leave unchanged

        return df, name

    def create_table(self, schema: StructType, table_name: str, enable_change_data_feed: bool = True):
        full_table_name = f"{self.lakehouse}.{table_name}"

        try:
            df: DataFrame = self.spark.createDataFrame(data=[], schema=schema)

            if table_exists(full_table_name):
                df.write.format("delta")\
                        .mode("append")\
                        .option("mergeSchema", "true")\
                        .saveAsTable(full_table_name)
                logging.info(f"✅ Table {full_table_name} updated successfully.")
            else:
                df, pk_name = self.add_primary_key(df, schema)
                df.write.option("delta.enableChangeDataFeed", ("false" if enable_change_data_feed is False else "true"))\
                        .format("delta")\
                        .mode("overwrite")\
                        .saveAsTable(full_table_name)
                logging.info(f"✅ Table {full_table_name} created successfully (EnableChangeDataFeed={enable_change_data_feed}).")

        except Exception as e:
            logging.error(f"⛔ Failed to create or append data to table {full_table_name}: {e}")
            raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
