"""
Timestamp conversion utilities for Lakehouse Plumber pipelines.

This module provides functions to convert timestampntz (timestamp without timezone)
columns to standard timestamp type for compatibility with Delta Lake and Databricks.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampNTZType, TimestampType
from pyspark.sql.functions import col


def convert_timestampntz_to_timestamp(df: DataFrame, spark, parameters: dict) -> DataFrame:
    """
    Convert all timestampntz columns to timestamp type.
    
    This function automatically detects all columns with TimestampNTZType in the DataFrame
    schema and converts them to standard TimestampType. This is necessary because some
    Parquet files use timestampntz which may not be fully supported in all contexts.
    
    Args:
        df: Input DataFrame with potential timestampntz columns
        spark: SparkSession instance (required by LHP but unused in this function)
        parameters: Configuration parameters from YAML (required by LHP but unused)
        
    Returns:
        DataFrame: DataFrame with all timestampntz columns converted to timestamp.
                   All other columns and data remain unchanged.
    
    Example:
        >>> # In LHP YAML template:
        >>> # - name: convert_timestamp
        >>> #   type: transform
        >>> #   transform_type: python
        >>> #   function:
        >>> #     file: "py_functions/timestamp_converter.py"
        >>> #     function: "convert_timestampntz_to_timestamp"
    """
    # Iterate through schema to find timestampntz columns
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampNTZType):
            # Cast timestampntz to timestamp (simple cast, no timezone conversion)
            df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
    
    return df

