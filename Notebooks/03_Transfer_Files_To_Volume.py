# Databricks notebook source
# MAGIC %md
# MAGIC # ACME Retail - Incremental File Transfer Between Volumes
# MAGIC
# MAGIC This notebook transfers export files from one Unity Catalog volume to another volume incrementally,
# MAGIC processing one day at a time and tracking progress in a Delta table.
# MAGIC
# MAGIC **Features:**
# MAGIC - Incremental processing (one date at a time per folder)
# MAGIC - Progress tracking via Delta table
# MAGIC - Handles multiple file patterns:
# MAGIC   - Direct files: `table/YYYY-MM-DD_file.ext`
# MAGIC   - Date folders: `table/YYYY-MM-DD/files`
# MAGIC   - Simple dated: `YYYY-MM-DD.ext`
# MAGIC - Handles sparse dates (e.g., product_csv weekly snapshots)
# MAGIC - Restart capability to begin from scratch
# MAGIC - Supports cross-catalog and cross-schema transfers
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Source volume must exist with export files
# MAGIC - Target volume must exist
# MAGIC - Catalog and schema must exist for tracking table
# MAGIC
# MAGIC **Note:**
# MAGIC - This notebook handles volume-to-volume transfer only
# MAGIC - Upload from local export folder to source volume is out of scope

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Parameters

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("source_volume_path", "/Volumes/main/acme_retail/raw_exports", "Source Volume Path")
dbutils.widgets.text("target_volume_path", "/Volumes/main/acme_retail/landing", "Target Volume Path")
dbutils.widgets.text("tracking_catalog", "main", "Tracking Table Catalog")
dbutils.widgets.text("tracking_schema", "acme_retail", "Tracking Table Schema")
dbutils.widgets.dropdown("restart", "false", ["false", "true"], "Restart from Beginning")
dbutils.widgets.text("date_limit", "", "Optional: Process only this specific date (YYYY-MM-DD)")

# COMMAND ----------

# DBTITLE 1,Get Parameters
source_volume_path = dbutils.widgets.get("source_volume_path")
target_volume_path = dbutils.widgets.get("target_volume_path")
tracking_catalog = dbutils.widgets.get("tracking_catalog")
tracking_schema = dbutils.widgets.get("tracking_schema")
restart = dbutils.widgets.get("restart").lower() == "true"
date_limit = dbutils.widgets.get("date_limit").strip()

print(f"Configuration:")
print(f"  Source Volume: {source_volume_path}")
print(f"  Target Volume: {target_volume_path}")
print(f"  Tracking Catalog: {tracking_catalog}")
print(f"  Tracking Schema: {tracking_schema}")
print(f"  Restart: {restart}")
print(f"  Date Limit: {date_limit if date_limit else 'None (process next available)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as sql_max
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, TimestampType, IntegerType
from datetime import datetime, date
import re
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tracking Table Management

# COMMAND ----------

# DBTITLE 1,Define Tracking Table Schema
tracking_table = f"{tracking_catalog}.{tracking_schema}.file_transfer_tracker"

table_schema = StructType([
    StructField("simulation_date", DateType(), False),
    StructField("total_folders_found", IntegerType(), False),
    StructField("folders_with_files", IntegerType(), False),
    StructField("folders_skipped", IntegerType(), False),
    StructField("total_files_transferred", IntegerType(), False),
    StructField("total_bytes_transferred", LongType(), False),
    StructField("last_updated", TimestampType(), False),
    StructField("status", StringType(), False)  # 'success', 'partial', 'failed'
])

# COMMAND ----------

# DBTITLE 1,Initialize Tracking Table
def initialize_tracking_table(recreate: bool = False):
    """
    Initialize or recreate the tracking table.

    Args:
        recreate: If True, drop and recreate the table
    """
    if recreate:
        print(f"🔄 Dropping existing tracking table: {tracking_table}")
        spark.sql(f"DROP TABLE IF EXISTS {tracking_table}")

    # Create table if it doesn't exist
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {tracking_table} (
        simulation_date DATE NOT NULL,
        total_folders_found INT NOT NULL,
        folders_with_files INT NOT NULL,
        folders_skipped INT NOT NULL,
        total_files_transferred INT NOT NULL,
        total_bytes_transferred BIGINT NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        status STRING NOT NULL,
        CONSTRAINT pk_simulation_date PRIMARY KEY (simulation_date)
    )
    USING DELTA
    COMMENT 'Tracks incremental file transfer progress by simulation date - all folders process same date on each run'
    """

    spark.sql(create_sql)
    print(f"✅ Tracking table ready: {tracking_table}")

# Initialize table
initialize_tracking_table(recreate=restart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. File Discovery and Pattern Detection

# COMMAND ----------

# DBTITLE 1,Discover Export Folders
def get_export_folders(base_path: str) -> List[str]:
    """
    Discover all subdirectories in the export path.

    Args:
        base_path: Base export path

    Returns:
        List of folder names (relative to base path)
    """
    try:
        files = dbutils.fs.ls(base_path)
        folders = [f.name.rstrip('/') for f in files if f.isDir() and not f.name.startswith('.')]
        return sorted(folders)
    except Exception as e:
        print(f"❌ Error listing folders: {e}")
        return []

# COMMAND ----------

# DBTITLE 1,Extract Dates from Files
def extract_dates_from_folder(folder_path: str, folder_name: str) -> List[date]:
    """
    Extract all available dates from files in a folder.

    Handles three patterns:
    1. YYYY-MM-DD_filename.ext (direct files)
    2. YYYY-MM-DD/files (date folders)
    3. YYYY-MM-DD.ext (simple dated files)

    Args:
        folder_path: Full path to folder
        folder_name: Folder name for pattern detection

    Returns:
        Sorted list of dates found
    """
    dates = set()
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')

    try:
        items = dbutils.fs.ls(folder_path)

        for item in items:
            # Extract date from name
            match = date_pattern.search(item.name)
            if match:
                date_str = match.group(1)
                try:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    dates.add(parsed_date)
                except ValueError:
                    pass

        return sorted(list(dates))

    except Exception as e:
        print(f"⚠️  Error reading folder {folder_name}: {e}")
        return []

# COMMAND ----------

# DBTITLE 1,Get Files for Specific Date
def get_files_for_date(folder_path: str, folder_name: str, target_date: date) -> List[Tuple[str, int]]:
    """
    Get all files for a specific date in a folder.

    Args:
        folder_path: Full path to folder
        folder_name: Folder name
        target_date: Date to find files for

    Returns:
        List of tuples (file_path, size_bytes)
    """
    files = []
    date_str = target_date.strftime('%Y-%m-%d')

    try:
        items = dbutils.fs.ls(folder_path)

        for item in items:
            # Check if item matches the target date
            if date_str in item.name:
                if item.isDir():
                    # If it's a date folder, get all files inside
                    sub_items = dbutils.fs.ls(item.path)
                    for sub_item in sub_items:
                        if sub_item.isFile():
                            files.append((sub_item.path, sub_item.size))
                elif item.isFile():
                    # Direct file with date
                    files.append((item.path, item.size))

        return files

    except Exception as e:
        print(f"❌ Error getting files for {folder_name} on {date_str}: {e}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transfer Logic

# COMMAND ----------

# DBTITLE 1,Get Last Processed Simulation Date
def get_last_processed_simulation_date() -> Optional[date]:
    """
    Get the last globally processed simulation date from tracking table.

    This is a global date that applies to all folders - all folders
    process the same simulation date on each run.

    Returns:
        Last processed simulation date or None if table is empty (first run)
    """
    try:
        result = spark.sql(f"""
            SELECT MAX(simulation_date) as last_date
            FROM {tracking_table}
        """).collect()

        if result and result[0]['last_date']:
            return result[0]['last_date']
        return None

    except Exception as e:
        # Table might be empty or not exist
        return None

# COMMAND ----------

# DBTITLE 1,Get Next Simulation Date
def get_next_simulation_date(last_processed_global: Optional[date]) -> Optional[date]:
    """
    Determine the next simulation date to process globally across all folders.

    This function scans all folders to find all available dates, then returns
    the next date after the last processed simulation date. All folders will
    attempt to process this date (folders without files will be skipped).

    Args:
        last_processed_global: Last processed simulation date (None if first run)

    Returns:
        Next simulation date to process or None if all dates processed
    """
    # Discover all folders
    folders = get_export_folders(source_volume_path)

    if not folders:
        print("⚠️  No folders found in source volume")
        return None

    # Collect all unique dates from all folders
    all_dates = set()
    for folder_name in folders:
        folder_path = f"{source_volume_path}/{folder_name}"
        folder_dates = extract_dates_from_folder(folder_path, folder_name)
        all_dates.update(folder_dates)

    # Sort dates chronologically
    sorted_dates = sorted(list(all_dates))

    if not sorted_dates:
        print("⚠️  No dated files found in any folder")
        return None

    # First run - return earliest date
    if last_processed_global is None:
        return sorted_dates[0]

    # Find next date after last_processed_global
    next_dates = [d for d in sorted_dates if d > last_processed_global]

    return next_dates[0] if next_dates else None

# COMMAND ----------

# DBTITLE 1,Transfer Files for Date
def transfer_files_for_date(
    folder_name: str,
    folder_path: str,
    target_date: date,
    files: List[Tuple[str, int]]
) -> Dict:
    """
    Transfer all files for a specific date.

    Args:
        folder_name: Folder name
        folder_path: Source folder path
        target_date: Date being processed
        files: List of (file_path, size) tuples

    Returns:
        Statistics dict with transferred/failed counts
    """
    stats = {
        'transferred': 0,
        'failed': 0,
        'bytes': 0,
        'errors': []
    }

    if not files:
        print(f"  ⚠️  No files found for {target_date}")
        return stats

    # Create target directory structure
    target_base = f"{target_volume_path}/{folder_name}"

    for file_path, file_size in files:
        try:
            # Preserve directory structure relative to folder
            # Extract relative path from source
            rel_path = file_path.replace(f"{folder_path}/", "")
            target_path = f"{target_base}/{rel_path}"

            # Copy file
            dbutils.fs.cp(file_path, target_path, recurse=False)

            stats['transferred'] += 1
            stats['bytes'] += file_size

        except Exception as e:
            stats['failed'] += 1
            stats['errors'].append(f"{file_path}: {str(e)}")
            print(f"    ❌ Failed: {file_path} - {e}")

    return stats

# COMMAND ----------

# DBTITLE 1,Update Tracking Table
def update_tracking(simulation_date: date, date_stats: Dict):
    """
    Update tracking table with results for a simulation date.

    Args:
        simulation_date: The simulation date that was processed
        date_stats: Dictionary with aggregated statistics:
            - total_folders_found: Number of folders found
            - folders_with_files: Number of folders that had files for this date
            - folders_skipped: Number of folders that had no files for this date
            - total_files_transferred: Total files transferred across all folders
            - total_bytes_transferred: Total bytes transferred across all folders
            - status: Overall status ('success', 'partial', 'failed')
    """
    # Create DataFrame for new record
    new_record = spark.createDataFrame([
        (
            simulation_date,
            date_stats['total_folders_found'],
            date_stats['folders_with_files'],
            date_stats['folders_skipped'],
            date_stats['total_files_transferred'],
            date_stats['total_bytes_transferred'],
            datetime.now(),
            date_stats['status']
        )
    ], table_schema)

    # Append to tracking table
    new_record.write.format("delta").mode("append").saveAsTable(tracking_table)

    print(f"\n📊 Tracking Updated:")
    print(f"   Simulation Date: {simulation_date}")
    print(f"   Folders with files: {date_stats['folders_with_files']}/{date_stats['total_folders_found']}")
    print(f"   Folders skipped: {date_stats['folders_skipped']}")
    print(f"   Total files: {date_stats['total_files_transferred']}")
    print(f"   Total bytes: {date_stats['total_bytes_transferred']:,}")
    print(f"   Status: {date_stats['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Main Processing Loop

# COMMAND ----------

# DBTITLE 1,Process All Folders for Simulation Date
def process_all_folders(specific_date: Optional[str] = None):
    """
    Main processing function - processes all folders for the same simulation date.

    All folders are synchronized to process the same simulation date on each run.
    Folders without files for that date are skipped (not an error).

    Args:
        specific_date: Optional specific date to process (YYYY-MM-DD)
    """
    print("=" * 80)
    print("🚀 Starting Incremental File Transfer (Global Simulation Date)")
    print("=" * 80)

    # 1. Discover all export folders
    folders = get_export_folders(source_volume_path)

    if not folders:
        print("❌ No export folders found!")
        return

    print(f"\n📁 Found {len(folders)} export folders")

    # 2. Determine which simulation date to process
    if specific_date:
        # User specified a specific date
        target_date = datetime.strptime(specific_date, '%Y-%m-%d').date()
        print(f"🎯 User-specified simulation date: {target_date}")
    else:
        # Get last processed simulation date (global across all folders)
        last_processed_global = get_last_processed_simulation_date()

        if last_processed_global:
            print(f"✓ Last processed simulation date: {last_processed_global}")
        else:
            print(f"ℹ️  First run - no previous simulation dates processed")

        # Get next simulation date across all folders
        target_date = get_next_simulation_date(last_processed_global)

    if target_date is None:
        print("\n✅ All simulation dates have been processed!")
        print("=" * 80)
        return

    print(f"\n{'='*80}")
    print(f"📅 Processing Simulation Date: {target_date}")
    print(f"{'='*80}\n")

    # 3. Initialize statistics for this simulation date
    date_stats = {
        'total_folders_found': len(folders),
        'folders_with_files': 0,
        'folders_skipped': 0,
        'total_files_transferred': 0,
        'total_bytes_transferred': 0,
        'errors': []
    }

    # 4. Process each folder for THIS simulation date
    for folder_name in folders:
        folder_path = f"{source_volume_path}/{folder_name}"

        # Get available dates in this folder
        available_dates = extract_dates_from_folder(folder_path, folder_name)

        # Check if folder has files for target_date
        if target_date not in available_dates:
            print(f"  ⏭️  {folder_name}: Skipped (no files for {target_date})")
            date_stats['folders_skipped'] += 1
            continue

        # Get files for target_date
        files = get_files_for_date(folder_path, folder_name, target_date)

        if not files:
            print(f"  ⏭️  {folder_name}: Skipped (no files found)")
            date_stats['folders_skipped'] += 1
            continue

        print(f"  📄 {folder_name}: {len(files)} file(s) found")

        # Transfer files for this folder
        transfer_stats = transfer_files_for_date(folder_name, folder_path, target_date, files)

        # Update date-level stats
        date_stats['folders_with_files'] += 1
        date_stats['total_files_transferred'] += transfer_stats['transferred']
        date_stats['total_bytes_transferred'] += transfer_stats['bytes']

        if transfer_stats['errors']:
            date_stats['errors'].extend(transfer_stats['errors'])

        print(f"  ✅ {folder_name}: {transfer_stats['transferred']} file(s) transferred ({transfer_stats['bytes']:,} bytes)")

    # 5. Determine overall status
    if date_stats['errors']:
        status = 'partial' if date_stats['total_files_transferred'] > 0 else 'failed'
    else:
        status = 'success'

    date_stats['status'] = status

    # 6. Update tracking table for this simulation date
    update_tracking(target_date, date_stats)

    # 7. Print summary
    print("\n" + "=" * 80)
    print("📊 Simulation Date Summary")
    print("=" * 80)
    print(f"Simulation Date: {target_date}")
    print(f"Folders with files: {date_stats['folders_with_files']}")
    print(f"Folders skipped: {date_stats['folders_skipped']}")
    print(f"Total files transferred: {date_stats['total_files_transferred']}")
    print(f"Total bytes transferred: {date_stats['total_bytes_transferred']:,}")
    print(f"Status: {status}")

    if date_stats['errors']:
        print(f"\n❌ Errors encountered: {len(date_stats['errors'])}")
        for error in date_stats['errors'][:10]:
            print(f"  - {error}")
        if len(date_stats['errors']) > 10:
            print(f"  ... and {len(date_stats['errors']) - 10} more")
    else:
        print("\n✅ All transfers completed successfully!")

    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Execute Transfer

# COMMAND ----------

# DBTITLE 1,Run Transfer
# Execute the transfer process
process_all_folders(specific_date=date_limit if date_limit else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. View Tracking Table

# COMMAND ----------

# DBTITLE 1,Show Recent Transfer History
display(spark.sql(f"""
    SELECT
        simulation_date,
        total_folders_found,
        folders_with_files,
        folders_skipped,
        total_files_transferred,
        ROUND(total_bytes_transferred / 1024 / 1024, 2) as mb_transferred,
        last_updated,
        status
    FROM {tracking_table}
    ORDER BY simulation_date DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Utility Queries

# COMMAND ----------

# DBTITLE 1,Progress Overview
display(spark.sql(f"""
    SELECT
        COUNT(*) as dates_processed,
        MIN(simulation_date) as first_date,
        MAX(simulation_date) as latest_date,
        SUM(total_files_transferred) as total_files,
        ROUND(SUM(total_bytes_transferred) / 1024 / 1024, 2) as total_mb,
        AVG(folders_with_files) as avg_folders_with_files,
        AVG(folders_skipped) as avg_folders_skipped,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs
    FROM {tracking_table}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Manual Reset (Optional)
# MAGIC
# MAGIC Uncomment and run to reset tracking for a specific folder:

# COMMAND ----------

# DBTITLE 1,Reset Specific Date (Commented Out)
# date_to_reset = "2024-01-15"
# spark.sql(f"DELETE FROM {tracking_table} WHERE simulation_date = '{date_to_reset}'")
# print(f"✅ Reset tracking for {date_to_reset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Usage Notes
# MAGIC
# MAGIC ### Volume-to-Volume Transfer
# MAGIC This notebook transfers files between Unity Catalog volumes:
# MAGIC - **Source**: Files must already be in a Unity Catalog volume (e.g., `/Volumes/main/acme_retail/raw_exports`)
# MAGIC - **Target**: Files are copied to another Unity Catalog volume (e.g., `/Volumes/main/acme_retail/landing`)
# MAGIC - **Cross-Catalog**: Supports transfers across different catalogs and schemas
# MAGIC
# MAGIC ### Global Simulation Date Synchronization
# MAGIC **Important**: All folders process the **same simulation date** on each run:
# MAGIC - **Run 1**: All folders process 2024-01-01
# MAGIC - **Run 2**: All folders process 2024-01-02 (product_csv skipped if no file)
# MAGIC - **Run 8**: All folders process 2024-01-08 (product_csv included again)
# MAGIC
# MAGIC This ensures data consistency across folders - no folder gets ahead of others.
# MAGIC
# MAGIC ### First Run
# MAGIC 1. Set `source_volume_path` to source volume containing export files
# MAGIC 2. Set `target_volume_path` to destination volume
# MAGIC 3. Set `restart=true` to create tracking table
# MAGIC 4. Run notebook - processes first date from each folder
# MAGIC 5. Check tracking table for results
# MAGIC
# MAGIC ### Subsequent Runs
# MAGIC 1. Set `restart=false` (default)
# MAGIC 2. Run notebook - processes next **simulation date** across all folders
# MAGIC 3. Repeat daily or as needed
# MAGIC
# MAGIC ### Process Specific Date
# MAGIC 1. Set `date_limit` to specific date (e.g., "2024-01-15")
# MAGIC 2. Run notebook - processes only that date across all folders
# MAGIC 3. Folders without files for that date are skipped (status remains 'success')
# MAGIC
# MAGIC ### Handle Sparse Dates
# MAGIC - Script automatically handles gaps (e.g., product_csv weekly snapshots)
# MAGIC - Folders without files for the current simulation date are **skipped** (not an error)
# MAGIC - Example: On 2024-01-02, product_csv is skipped because it only has weekly files
# MAGIC - All folders stay synchronized - no folder advances ahead of others
# MAGIC
# MAGIC ### Monitor Progress
# MAGIC - Check "View Tracking Table" section to see history by simulation date
# MAGIC - Check "Progress Overview" for overall statistics
# MAGIC - Review status column for any failures
# MAGIC - Track `folders_skipped` to see which dates had missing files
# MAGIC
# MAGIC ### Example Configuration
# MAGIC
# MAGIC **Same Catalog, Different Volumes:**
# MAGIC - `source_volume_path`: `/Volumes/main/acme_retail/raw_exports`
# MAGIC - `target_volume_path`: `/Volumes/main/acme_retail/landing`
# MAGIC - `tracking_catalog`: `main`
# MAGIC - `tracking_schema`: `acme_retail`
# MAGIC
# MAGIC **Cross-Catalog Transfer:**
# MAGIC - `source_volume_path`: `/Volumes/dev/acme_retail/exports`
# MAGIC - `target_volume_path`: `/Volumes/prod/acme_retail/landing`
# MAGIC - `tracking_catalog`: `prod`
# MAGIC - `tracking_schema`: `acme_retail`
