
from typing import Optional, Tuple
from pyspark.sql.functions import current_timestamp, when, col, regexp_extract, lit
from pyspark.sql import DataFrame


def next_product_snapshot_and_version(
    latest_snapshot_version: Optional[str],
) -> Optional[Tuple[DataFrame, str]]:
    """
    Process product snapshots incrementally from append-only bronze table.

    Args:
        latest_snapshot_version: Most recent snapshot date processed (YYYY-MM-DD format),
                               or None for the first run

    Returns:
        Tuple of (DataFrame, snapshot_date) containing the next snapshot's data and its date,
        or None if no more snapshots are available to process
    """

    # Load bronze table
    df = spark.sql("""
                    SELECT *
                    FROM {catalog}.{bronze_schema}.bronze_sap_prd
                   """)
    
    # Extract date from file path, handling HIST_ prefix
    # HIST_ files should be assigned to 2000-01-01 (historical baseline)
    df = df.withColumn(
        "_snapshot_date",
        when(
            col("_source_file_path").contains("HIST_"),
            lit("2000-01-01")
        ).otherwise(
            regexp_extract(col("_source_file_path"), r"(\d{4}-\d{2}-\d{2})", 1)
        )
    )

    df.createOrReplaceTempView("bronze_sap_prd_snapshot")

    if latest_snapshot_version is None:

        min_snapshot_result = spark.sql("""
            select MIN(_snapshot_date) as min_date from bronze_sap_prd_snapshot
        """).collect()[0]

        min_snapshot = min_snapshot_result.min_date

        if min_snapshot is None or min_snapshot == "":

            return None

        df = spark.sql(f"""
            SELECT * FROM bronze_sap_prd_snapshot
            WHERE _snapshot_date = '{min_snapshot}'
        """)

        df.withColumn("_processing_timestamp", current_timestamp())
        return (df, min_snapshot)

    else:

        next_snapshot_result = spark.sql(f"""
            SELECT MIN(_snapshot_date) as next_date
            FROM bronze_sap_prd_snapshot
            WHERE _snapshot_date > '{latest_snapshot_version}'
        """).collect()[0]

        next_snapshot = next_snapshot_result.next_date

        if next_snapshot is None or next_snapshot == "":

            return None  # No more snapshots to process

        df = spark.sql(f"""
            SELECT * FROM bronze_sap_prd_snapshot
            WHERE _snapshot_date = '{next_snapshot}'
        """)

        df.withColumn("_processing_timestamp", current_timestamp())
        return (df, next_snapshot)
