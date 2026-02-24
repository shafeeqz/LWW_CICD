# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6b9edcf9-3110-4a55-a20f-41e46a56a503",
# META       "default_lakehouse_name": "Fund1_Fundraising_SL",
# META       "default_lakehouse_workspace_id": "14d00f5c-2c04-4897-ba2b-221c7834ad8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "6b9edcf9-3110-4a55-a20f-41e46a56a503"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # SampleData Import To Silver Layer
# Use this notebook for importing sample data from csv files stored in /Files to silver layer. 

# CELL ********************

%run Fund1_Fundraising_SL_CreateSchema { "enable_create_tables": false }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Imports & Setup

# PARAMETERS CELL ********************

from notebookutils import mssparkutils
import os
import traceback

# Define lakehouse where the data should be stored
target_lakehouse = silver_lakehouse_name

# Feature flags
enable_delete_sample_data = False
enable_import_sample_data = True

# Define paths
input_folder = "file:/lakehouse/default/Files/nds-silver-sampledata/"
output_folder = "/lakehouse/default/Tables/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Delete sample data 

# CELL ********************

if enable_delete_sample_data:
    # Get all table names (same logic as import)
    files = mssparkutils.fs.ls(input_folder)
    csv_files = [f.path for f in files if f.path.endswith(".csv")]
    table_names = [os.path.basename(f).replace(".csv", "") for f in csv_files]

    # Track failures
    failures = []

    print("🧹 Starting table data deletion...\n")

    for table_name in table_names:
        print(f"🗑️ Deleting data from table: {table_name}")
        
        try:
            full_table_name = get_full_table_name(target_lakehouse, table_name)
            spark.sql(f"DELETE FROM {full_table_name}")
            # not tested yet
            #spark.sql(f"DELETE FROM {full_table_name} USING Source WHERE {full_table_name}.SourceId = Source.SourceId AND Source.Name = 'SampleData'")
            print(f"✅ Cleared: {table_name}")
        
        except Exception as e:
            print(f"❌ Failed to clear: {table_name}")
            print(traceback.format_exc())
            failures.append((table_name, str(e)))

    # Summary
    print("\n🧾 Deletion Summary:")
    print(f"✔️ Cleared: {len(table_names) - len(failures)} tables")
    print(f"❌ Failed: {len(failures)} tables")

    if failures:
        print("\n📌 Failures:")
        for tbl, err in failures:
            print(f" - {tbl}: {err}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample Data Import

# CELL ********************

from pyspark.sql.functions import col, broadcast, lit
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable
import os
import logging
from datetime import datetime, timezone
import traceback

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def get_csv_files(input_folder):
    """
    Get list of CSV files from the input folder.
    
    Returns:
        list: List of CSV file paths
    """
    files = mssparkutils.fs.ls(input_folder)
    csv_files = [f.path for f in files if f.path.endswith(".csv")]
    csv_files.sort()  # Sort alphabetically
    
    return csv_files

def process_table(file_path, failures_list):
    """
    Process a single table from CSV file to lakehouse.
    
    Args:
        file_path: Path to the CSV file
        failures_list: List to append failures to
    
    Returns:
        bool: True if successful, False if failed
    """
    table_name = os.path.basename(file_path).replace(".csv", "")
    logging.info(f"\n🚀 Processing table: {table_name}")
    
    try:
        full_table_name = get_full_table_name(target_lakehouse, table_name)
        logging.info(f"📄 Reading file: {file_path}")

        method_name = f"get_{table_name.lower()}_schema"
        expected_schema = getattr(NonprofitSilverModel, method_name)()

        if not expected_schema:
            raise ValueError(f"❌ No schema defined for table: {table_name}")

        # Step 1: Read CSV without applying schema
        raw_df = spark.read \
            .option("header", True) \
            .csv(file_path)

        # Step 2: Select and cast columns according to schema (by name, order-independent)
        casted_cols = []
        for field in expected_schema:
            if field.name in raw_df.columns:
                casted_cols.append(col(field.name).cast(field.dataType).alias(field.name))
            else:
                raise ValueError(f"❌ Missing expected column: {field.name} in {file_path}")

        # Generate dynamic UPDATE clause for WHEN MATCHED
        update_assignments = []
        for field in expected_schema:
            if field.name in raw_df.columns:
                update_assignments.append(f"target.{field.name} = source.{field.name}")

        df = raw_df.select(casted_cols)

        # Generalized key detection
        key_col = f"{table_name}Id"
        if key_col in df.columns:
            on_condition = f"target.{key_col} = source.{key_col}"
        elif "SourceId" in df.columns and "SourceSystemId" in df.columns:
            on_condition = "target.SourceId = source.SourceId AND target.SourceSystemId = source.SourceSystemId"
        else:
            raise ValueError(f"❌ Cannot determine deduplication key for {table_name} (looked for '{key_col}' or ('SourceId','SourceSystemId'))")

        logging.info(f"💾 Writing to Lakehouse table: {full_table_name}")

        if not table_exists(full_table_name):
            raise ValueError(f"❌ Target table does not exist: {full_table_name}")

        # Upsert logic using Delta Lake MERGE
        delta_table = DeltaTable.forName(spark, full_table_name)
        staging_view = f"staging_{table_name.lower()}"
        df.createOrReplaceTempView(staging_view)

        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING {staging_view} AS source
            ON {on_condition}
            WHEN MATCHED THEN UPDATE SET {", ".join(update_assignments)}
            WHEN NOT MATCHED THEN INSERT *
        """

        spark.sql(merge_sql)
        logging.info(f"🆗 Merge complete for {table_name}")
        logging.info(f"✅ Done: {table_name}")
        return True
    
    except Exception as e:
        logging.error(f"❌ Failed to process {table_name}")
        if isinstance(e, ValueError):
            logging.error(f"Error: {e}")
        else:
            logging.error(traceback.format_exc())
        failures_list.append((table_name, str(e)))
        return False

def print_summary(total_files, failures_list):
    """
    Print processing summary and handle failures.
    
    Args:
        total_files: Total number of files processed
        failures_list: List of failed tables
    """
    logging.info("\n🧾 Summary:")
    logging.info(f"✔️ Successfully processed: {len(total_files) - len(failures_list)} tables")
    logging.info(f"❌ Failed: {len(failures_list)} tables")

    if failures_list:
        logging.info("\n📌 Failures:")
        for tbl, err in failures_list:
            logging.error(f" - {tbl}: {err}")
        
        # Fail the notebook if there were any failures
        failure_summary = f"Failed to process {len(failures_list)} table(s): {', '.join([tbl for tbl, _ in failures_list])}"
        raise Exception(f"❌ Data import completed with failures. {failure_summary}")

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if enable_import_sample_data:
    # Initialize
    logging.info("🚀 Starting sample data import process...")
    
    # Get CSV files
    csv_files = get_csv_files(input_folder)
    
    logging.info(f"📂 Found {len(csv_files)} CSV files to process")
    
    # Track failures
    failures = []
    
    # Process all CSV files
    for file_path in csv_files:
        process_table(file_path, failures)
    
    # Summary and cleanup
    print_summary(csv_files, failures)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
