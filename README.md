# IO Manager

A modular ETL framework for Python with composable readers and writers. Built on Polars DataFrames with support for SQL Server, Hive-partitioned data lakes (via DuckDB), and Excel.

## Features

- **Composable Architecture**: Mix and match any reader with any writer
- **Partition Filtering**: Directory-level pruning for Hive-partitioned data
- **Row Filtering**: Pushdown filters to SQL and DuckDB engines
- **Write Modes**: `OVERWRITE`, `APPEND`, `FAIL_IF_EXISTS`, `OVERWRITE_PARTITIONS`
- **Read Modes**: `STRICT`, `EMPTY_IF_MISSING`, `WARN_IF_MISSING`
- **Explicit Partition Targeting**: Control exactly which partitions to overwrite

## Installation

```bash
uv add polars duckdb pyarrow
```

For SQL Server support:
```bash
uv add connectorx
```

For Excel support:
```bash
uv add xlsxwriter
```

## Quick Start

```python
from io_manager import (
    IOManager,
    HiveReader,
    HiveWriter,
    SqlServerReader,
    WriteMode,
)

# Compose a reader and writer
io = IOManager(
    reader=SqlServerReader("Driver={ODBC Driver 18 for SQL Server};Server=..."),
    writer=HiveWriter("/data/lake"),
)

# Transfer data with transformation
io.transfer(
    source="SELECT * FROM sales WHERE year = 2024",
    destination="sales",
    transform=lambda df: df.filter(df["amount"] > 0),
    write_kwargs={"partition_by": ["year", "month"]},
)
```

## Partition Filtering

Filter at the directory level to skip reading unnecessary files entirely:

```python
reader = HiveReader("/data/lake")

# Only reads from year=2024/region=US/ and year=2024/region=EU/ directories
df = reader.read(
    source="sales",
    partitions={"year": [2024], "region": ["US", "EU"]},
)
```

## Row Filtering

Apply row-level filters that push down to the query engine:

```python
# Filters translate to WHERE clauses in SQL/DuckDB
df = reader.read(
    source="orders",
    filters=[
        ("amount", ">", 1000),
        ("status", "=", "active"),
        ("category", "in", ["electronics", "furniture"]),
    ],
)
```

## Combining Partitions and Filters

Use both for efficient querying:

```python
df = reader.read(
    source="transactions",
    partitions={"year": [2024], "month": [1, 2, 3]},  # Directory-level (coarse)
    filters=[("amount", ">=", 500)],                   # Row-level (fine)
)
```

## Read Modes

Control behavior when source doesn't exist:

```python
from io_manager import ReadMode

# STRICT (default): Raises FileNotFoundError
df = reader.read("data", mode=ReadMode.STRICT)

# EMPTY_IF_MISSING: Returns empty DataFrame silently
df = reader.read("maybe_exists", mode=ReadMode.EMPTY_IF_MISSING)

# WARN_IF_MISSING: Logs warning, returns empty DataFrame
df = reader.read("optional", mode=ReadMode.WARN_IF_MISSING)
```

## Write Modes

Control write behavior:

```python
from io_manager import WriteMode

writer.write(df, "output", mode=WriteMode.OVERWRITE)           # Replace all
writer.write(df, "output", mode=WriteMode.APPEND)              # Add to existing
writer.write(df, "output", mode=WriteMode.FAIL_IF_EXISTS)      # Skip if exists
writer.write(df, "output", mode=WriteMode.OVERWRITE_PARTITIONS)  # Replace matching partitions
```

## Explicit Partition Targeting

When using `OVERWRITE_PARTITIONS`, specify exactly which partitions to clear:

```python
# Scenario: Reprocessing Q1 2024, but month 3 has no data after filtering
df = pl.DataFrame({
    "year": [2024, 2024],
    "month": [1, 2],  # No data for month 3
    "value": [100, 200],
})

# Without target_partitions: month=3 retains stale data
# With target_partitions: all Q1 partitions are cleared
writer.write(
    df,
    destination="metrics",
    partition_by=["year", "month"],
    target_partitions={"year": [2024], "month": [1, 2, 3]},
    mode=WriteMode.OVERWRITE_PARTITIONS,
)
```

## Available Components

### Readers

| Class | Description |
|-------|-------------|
| `SqlServerReader` | Read from SQL Server via ODBC |
| `HiveReader` | Read Hive-partitioned Parquet via DuckDB |

### Writers

| Class | Description |
|-------|-------------|
| `SqlServerWriter` | Write to SQL Server tables |
| `HiveWriter` | Write Hive-partitioned Parquet via DuckDB |
| `ExcelWriter` | Write to Excel files |

### Factory Functions

```python
from io_manager import (
    create_sql_to_lake_manager,
    create_lake_to_sql_manager,
    create_sql_to_excel_manager,
)

# Pre-configured IOManager instances
io = create_sql_to_lake_manager(conn_str, "/data/lake")
io = create_lake_to_sql_manager("/data/lake", conn_str)
io = create_sql_to_excel_manager(conn_str, "/reports")
```

## Filter Operators

Supported operators for the `filters` parameter:

| Operator | Example |
|----------|---------|
| `=` | `("status", "=", "active")` |
| `!=` | `("status", "!=", "deleted")` |
| `>` | `("amount", ">", 100)` |
| `>=` | `("amount", ">=", 100)` |
| `<` | `("amount", "<", 1000)` |
| `<=` | `("amount", "<=", 1000)` |
| `in` | `("region", "in", ["US", "EU"])` |
| `not in` | `("region", "not in", ["TEST"])` |

## License

MIT
