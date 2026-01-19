# IO Manager Enhancement Plan

## Overview

Add structured read/write scaffolding with explicit partition and filter parameters.

## New Types and Enums

### 1. ReadMode Enum

```python
class ReadMode(Enum):
    STRICT = "strict"           # Fail if source doesn't exist (default)
    EMPTY_IF_MISSING = "empty"  # Return empty DataFrame
    WARN_IF_MISSING = "warn"    # Log warning, return empty DataFrame
```

### 2. Filter Type Alias

```python
# (column, operator, value)
# Operators: "=", "!=", ">", ">=", "<", "<=", "in", "not in"
FilterExpr = tuple[str, str, Any]
```

---

## Phase 1: Update Base Classes

### Reader ABC Changes

```python
class Reader(ABC):
    @abstractmethod
    def read(
        self,
        source: str,
        partitions: dict[str, list] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        mode: ReadMode = ReadMode.STRICT,
        **kwargs,
    ) -> pl.DataFrame:
        """
        Read data from source.

        Args:
            source: Data source identifier (path, table name, query).
            partitions: Partition column filters as {column: [values]}.
                       For Hive readers, enables directory-level pruning.
                       For SQL readers, adds WHERE col IN (...) clauses.
            filters: Row-level filters as list of (column, operator, value) tuples.
                    Operators: =, !=, >, >=, <, <=, in, not in
            mode: Behavior when source doesn't exist.
            **kwargs: Engine-specific options.

        Returns:
            Polars DataFrame.
        """
        pass
```

### Writer ABC Changes (minor)

No changes to base Writer class. Keep `partition_by` and `mode` as-is.
Optional: Add `target_partitions` parameter to HiveWriter only.

---

## Phase 2: Implement Filter Builder Utility

Create a helper to convert filters to SQL/DuckDB WHERE clauses:

```python
def build_where_clause(
    partitions: dict[str, list] | None = None,
    filters: list[tuple[str, str, Any]] | None = None,
) -> str | None:
    """
    Build a WHERE clause from partitions and filters.

    Returns None if no conditions, otherwise "WHERE ..." string.
    """
    conditions = []

    # Partition conditions (always uses IN)
    if partitions:
        for col, values in partitions.items():
            if len(values) == 1:
                conditions.append(f"{col} = {_quote_value(values[0])}")
            else:
                quoted = ", ".join(_quote_value(v) for v in values)
                conditions.append(f"{col} IN ({quoted})")

    # Filter conditions
    if filters:
        for col, op, value in filters:
            op_upper = op.upper()
            if op_upper == "IN":
                quoted = ", ".join(_quote_value(v) for v in value)
                conditions.append(f"{col} IN ({quoted})")
            elif op_upper == "NOT IN":
                quoted = ", ".join(_quote_value(v) for v in value)
                conditions.append(f"{col} NOT IN ({quoted})")
            else:
                conditions.append(f"{col} {op} {_quote_value(value)}")

    if not conditions:
        return None

    return "WHERE " + " AND ".join(conditions)


def _quote_value(value: Any) -> str:
    """Quote a value for SQL."""
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif value is None:
        return "NULL"
    else:
        return str(value)
```

---

## Phase 3: Update HiveReader

```python
class HiveReader(Reader):
    def __init__(self, base_path: str | Path | None = None):
        self.base_path = Path(base_path) if base_path else None

    def read(
        self,
        source: str,
        partitions: dict[str, list] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        mode: ReadMode = ReadMode.STRICT,
        hive_partitioning: bool = True,
        **kwargs,
    ) -> pl.DataFrame:
        path = self._resolve_path(source)

        # Handle missing source based on mode
        if not self._source_exists(path):
            if mode == ReadMode.STRICT:
                raise FileNotFoundError(f"Source not found: {path}")
            elif mode == ReadMode.WARN_IF_MISSING:
                logger.warning(f"Source not found: {path}. Returning empty DataFrame.")
            return pl.DataFrame()  # Empty DataFrame for non-strict modes

        # Build query with pushdown
        hive_opt = "true" if hive_partitioning else "false"
        where_clause = build_where_clause(partitions, filters) or ""

        query = f"""
            SELECT * FROM read_parquet('{path}/**/*.parquet', hive_partitioning={hive_opt})
            {where_clause}
        """

        with duckdb.connect() as con:
            result = con.execute(query).pl()

        return result

    def _source_exists(self, path: str) -> bool:
        """Check if source path exists and has parquet files."""
        p = Path(path)
        if not p.exists():
            return False
        return any(p.rglob("*.parquet"))
```

**Key changes:**
- Adds `partitions` parameter for directory-level filtering
- Adds `filters` parameter for row-level filtering
- Adds `mode` parameter for missing source handling
- Pushes both partitions and filters down to DuckDB query
- Source path no longer needs glob pattern (automatically adds `/**/*.parquet`)

---

## Phase 4: Update SqlServerReader

```python
class SqlServerReader(Reader):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def read(
        self,
        source: str,
        partitions: dict[str, list] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        mode: ReadMode = ReadMode.STRICT,
        **kwargs,
    ) -> pl.DataFrame:
        # Determine base query
        if source.strip().upper().startswith("SELECT"):
            # User provided a query - wrap it to add filters
            if partitions or filters:
                where_clause = build_where_clause(partitions, filters)
                # Remove "WHERE " prefix since we need to use it differently
                conditions = where_clause[6:] if where_clause else None
                if conditions:
                    query = f"SELECT * FROM ({source}) AS subq WHERE {conditions}"
                else:
                    query = source
            else:
                query = source
        else:
            # Table name provided
            where_clause = build_where_clause(partitions, filters) or ""
            query = f"SELECT * FROM {source} {where_clause}"

        try:
            return pl.read_database(query, self.connection_string, **kwargs)
        except Exception as e:
            if mode == ReadMode.STRICT:
                raise
            elif mode == ReadMode.WARN_IF_MISSING:
                logger.warning(f"Failed to read from {source}: {e}. Returning empty DataFrame.")
            return pl.DataFrame()
```

**Key changes:**
- Adds `partitions` and `filters` that translate to SQL WHERE clauses
- If user provides a full SELECT query, wraps it as subquery to add filters
- ReadMode handling for error cases

---

## Phase 5: Update HiveWriter (Optional Enhancement)

Add `target_partitions` for explicit partition targeting:

```python
class HiveWriter(Writer):
    def write(
        self,
        df: pl.DataFrame,
        destination: str,
        partition_by: list[str] | None = None,
        target_partitions: dict[str, list] | None = None,  # NEW
        mode: WriteMode = WriteMode.OVERWRITE,
        **kwargs,
    ) -> None:
        """
        Args:
            partition_by: Columns to partition output by.
            target_partitions: Explicit partitions to delete before writing
                              (for OVERWRITE_PARTITIONS mode). If None, infers
                              from DataFrame values.
        """
        path = self._resolve_path(destination)
        dest_path = Path(path)

        # Handle OVERWRITE_PARTITIONS with explicit targets
        if mode == WriteMode.OVERWRITE_PARTITIONS and partition_by:
            if target_partitions:
                # Use explicit partition targets
                self._delete_explicit_partitions(dest_path, target_partitions, partition_by)
            else:
                # Infer from DataFrame (existing behavior)
                self._delete_partitions(dest_path, df, partition_by)

        # ... rest of existing logic ...

    def _delete_explicit_partitions(
        self,
        path: Path,
        target_partitions: dict[str, list],
        partition_cols: list[str],
    ) -> None:
        """Delete explicitly specified partition directories."""
        if not path.exists():
            return

        # Generate all combinations of partition values
        import itertools

        col_values = [[(col, v) for v in target_partitions.get(col, [])]
                      for col in partition_cols]

        for combo in itertools.product(*col_values):
            partition_path = path
            for col, val in combo:
                partition_path = partition_path / f"{col}={val}"

            if partition_path.exists():
                logger.info(f"Deleting partition directory: {partition_path}")
                shutil.rmtree(partition_path)
```

---

## Phase 6: Update IOManager.transfer()

```python
class IOManager:
    def transfer(
        self,
        source: str,
        destination: str,
        transform: Callable | None = None,
        partitions: dict[str, list] | None = None,  # NEW - convenience
        filters: list[tuple[str, str, Any]] | None = None,  # NEW - convenience
        read_kwargs: dict[str, Any] | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """
        Read, optionally transform, and write data.

        Args:
            partitions: Partition filters passed to reader (convenience).
            filters: Row filters passed to reader (convenience).
            read_kwargs: Additional arguments passed to reader.
            write_kwargs: Additional arguments passed to writer.
        """
        read_kwargs = read_kwargs or {}

        # Merge convenience params into read_kwargs
        if partitions:
            read_kwargs.setdefault("partitions", partitions)
        if filters:
            read_kwargs.setdefault("filters", filters)

        write_kwargs = write_kwargs or {}

        df = self.read(source, **read_kwargs)

        if transform:
            df = transform(df)

        self.write(df, destination, **write_kwargs)
        return df
```

---

## Implementation Order

1. **Add ReadMode enum and FilterExpr type** (simple, no breaking changes)
2. **Add build_where_clause() utility** (standalone helper)
3. **Update HiveReader** (most impactful, enables partition pushdown)
4. **Update SqlServerReader** (applies filters to SQL)
5. **Update HiveWriter with target_partitions** (optional enhancement)
6. **Update IOManager.transfer()** (convenience parameters)
7. **Update docstrings and type hints throughout**

---

## Breaking Changes

**None if defaults are preserved:**
- All new parameters have default values (`None` or enum defaults)
- Existing code continues to work unchanged
- New parameters are opt-in

**Behavioral change:**
- HiveReader source path interpretation changes slightly:
  - Before: User must include glob pattern (`sales/**/*.parquet`)
  - After: Glob pattern added automatically (`sales` → `sales/**/*.parquet`)
  - Mitigation: Detect if source already contains glob characters and use as-is

---

## Testing Checklist

- [ ] HiveReader with partitions only
- [ ] HiveReader with filters only
- [ ] HiveReader with both partitions and filters
- [ ] HiveReader with ReadMode.STRICT (missing source)
- [ ] HiveReader with ReadMode.EMPTY_IF_MISSING
- [ ] SqlServerReader with partitions (translates to WHERE IN)
- [ ] SqlServerReader with filters
- [ ] SqlServerReader with user-provided SELECT + filters (subquery wrapping)
- [ ] HiveWriter with target_partitions
- [ ] IOManager.transfer() with convenience parameters
- [ ] Backward compatibility: all existing code works unchanged
