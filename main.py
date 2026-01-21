"""Example usage of the IO Manager."""

import polars as pl

from io_manager import (
    FilterExpr,
    HiveReader,
    HiveWriter,
    IOManager,
    ReadMode,
    SqlServerReader,
    WriteMode,
    create_sql_to_lake_manager,
)


def example_partition_filtering():
    """Example: Read with partition filtering (directory-level pruning)."""
    reader = HiveReader("/data/lake")

    # Read only specific partitions - skips reading other directories entirely
    df = reader.read(
        source="sales",
        partitions={"year": [2024], "region": ["US", "EU"]},
    )

    # Partitions translate to DuckDB WHERE clause for pushdown:
    # SELECT * FROM read_parquet('sales/**/*.parquet', hive_partitioning=true)
    # WHERE year = 2024 AND region IN ('US', 'EU')


def example_row_filtering():
    """Example: Read with row-level filters."""
    reader = HiveReader("/data/lake")

    # Filters are for row-level selection (within the data)
    filters: list[FilterExpr] = [
        ("amount", ">", 1000),
        ("status", "=", "active"),
        ("category", "in", ["electronics", "furniture"]),
    ]

    df = reader.read(
        source="orders",
        filters=filters,
    )


def example_combined_partition_and_filters():
    """Example: Combine partition pruning with row filtering."""
    reader = HiveReader("/data/lake")

    # Partitions: which directories to read (coarse, physical)
    # Filters: which rows to keep (fine, logical)
    df = reader.read(
        source="transactions",
        partitions={"year": [2024], "month": [1, 2, 3]},  # Q1 2024 only
        filters=[
            ("amount", ">=", 500),
            ("currency", "=", "USD"),
        ],
    )


def example_read_mode():
    """Example: Control behavior when source doesn't exist."""
    reader = HiveReader("/data/lake")

    # STRICT (default): Raises FileNotFoundError if source missing
    # df = reader.read("missing_table", mode=ReadMode.STRICT)

    # EMPTY_IF_MISSING: Returns empty DataFrame silently
    df = reader.read(
        source="maybe_exists",
        mode=ReadMode.EMPTY_IF_MISSING,
    )

    # WARN_IF_MISSING: Logs warning, returns empty DataFrame
    df = reader.read(
        source="optional_source",
        mode=ReadMode.WARN_IF_MISSING,
    )


def example_sql_with_filters():
    """Example: SQL Server reader with partition/filter pushdown."""
    conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=...;Database=..."
    reader = SqlServerReader(conn_str)

    # Filters translate to SQL WHERE clauses
    df = reader.read(
        source="orders",  # Table name
        partitions={"region": ["US"]},
        filters=[("order_date", ">=", "2024-01-01")],
    )
    # Generates: SELECT * FROM orders WHERE region = 'US' AND order_date >= '2024-01-01'

    # Also works with custom queries (wraps as subquery)
    df = reader.read(
        source="SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        filters=[("amount", ">", 1000)],
    )
    # Generates: SELECT * FROM (SELECT o.*, ...) AS subq WHERE amount > 1000


def example_target_partitions():
    """Example: Explicit partition targeting for writes."""
    writer = HiveWriter("/data/lake")

    # Scenario: Reprocessing Q1 2024, but month 3 has no valid records after transform
    df = pl.DataFrame({
        "year": [2024, 2024],
        "month": [1, 2],
        "value": [100, 200],
    })

    # Without target_partitions: month=3 directory would retain stale data
    # With target_partitions: explicitly clear all Q1 partitions
    writer.write(
        df,
        destination="metrics",
        partition_by=["year", "month"],
        target_partitions={"year": [2024], "month": [1, 2, 3]},  # Clear all Q1
        mode=WriteMode.OVERWRITE_PARTITIONS,
    )


def example_transfer_convenience():
    """Example: IOManager.transfer() with convenience parameters."""
    io = create_sql_to_lake_manager(
        sql_connection_string="Driver={ODBC Driver 18 for SQL Server};Server=...",
        lake_base_path="/data/lake",
    )

    # Partitions and filters as top-level params (convenience)
    io.transfer(
        source="sales",
        destination="sales_filtered",
        partitions={"year": [2024]},
        filters=[("amount", ">", 0)],
        transform=lambda df: df.with_columns(processed=pl.lit(True)),
        write_kwargs={"partition_by": ["year", "region"]},
    )


def example_full_etl_pipeline():
    """Example: Complete ETL with all new features."""
    conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=..."

    reader = SqlServerReader(conn_str)
    writer = HiveWriter("/data/lake")
    io = IOManager(reader, writer)

    # Extract with filters pushed down to SQL
    df = io.read(
        source="raw_events",
        partitions={"event_date": ["2024-01-15", "2024-01-16"]},
        filters=[
            ("event_type", "in", ["purchase", "refund"]),
            ("amount", ">", 0),
        ],
        mode=ReadMode.WARN_IF_MISSING,
    )

    if df.is_empty():
        print("No data to process")
        return

    # Transform
    df = df.with_columns(
        pl.col("event_date").str.slice(0, 7).alias("month"),
    )

    # Load with explicit partition targeting
    io.write(
        df,
        destination="events_processed",
        partition_by=["month"],
        target_partitions={"month": ["2024-01"]},
        mode=WriteMode.OVERWRITE_PARTITIONS,
    )


if __name__ == "__main__":
    print("IO Manager examples - see source for usage patterns")
    print("\nNew features:")
    print("  - partitions: dict[str, list] for partition filtering")
    print("  - filters: list[tuple] for row-level filtering")
    print("  - ReadMode: STRICT, EMPTY_IF_MISSING, WARN_IF_MISSING")
    print("  - target_partitions: explicit partition targeting for writes")
    print("\nAvailable classes:")
    print("  Readers: SqlServerReader, HiveReader")
    print("  Writers: SqlServerWriter, HiveWriter, ExcelWriter")
    print("  Manager: IOManager (composes any reader + writer)")
