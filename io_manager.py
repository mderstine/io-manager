"""
Modular IO Manager for ETL pipelines.

Provides composable Reader and Writer classes for:
- SQL Server databases
- Hive-partitioned data lakes (via DuckDB)
- Excel files (writer only)

All operations use Polars DataFrames.
"""

import logging
import shutil
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class WriteMode(Enum):
    """Write mode options for data writers."""

    OVERWRITE = "overwrite"  # Replace all data at destination
    APPEND = "append"  # Add to existing data
    FAIL_IF_EXISTS = "fail"  # Warn & skip if destination has data
    OVERWRITE_PARTITIONS = "overwrite_partitions"  # Delete & replace only partitions in DataFrame


# =============================================================================
# Base Classes
# =============================================================================


class Reader(ABC):
    """Abstract base class for data readers."""

    @abstractmethod
    def read(self, source: str, **kwargs) -> pl.DataFrame:
        """Read data from source and return a Polars DataFrame."""
        pass


class Writer(ABC):
    """Abstract base class for data writers."""

    @abstractmethod
    def write(self, df: pl.DataFrame, destination: str, **kwargs) -> None:
        """Write a Polars DataFrame to destination."""
        pass


# =============================================================================
# SQL Server Implementations
# =============================================================================


class SqlServerReader(Reader):
    """Read data from SQL Server using ODBC."""

    def __init__(self, connection_string: str):
        """
        Args:
            connection_string: ODBC connection string.
                Example: "Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;UID=...;PWD=..."
        """
        self.connection_string = connection_string

    def read(self, source: str, **kwargs) -> pl.DataFrame:
        """
        Read data from SQL Server.

        Args:
            source: SQL query or table name.
            **kwargs: Additional arguments passed to pl.read_database.

        Returns:
            Polars DataFrame with query results.
        """
        query = source if source.strip().upper().startswith("SELECT") else f"SELECT * FROM {source}"
        return pl.read_database(query, self.connection_string, **kwargs)


class SqlServerWriter(Writer):
    """Write data to SQL Server."""

    def __init__(self, connection_string: str):
        """
        Args:
            connection_string: ODBC connection string.
        """
        self.connection_string = connection_string

    def write(
        self,
        df: pl.DataFrame,
        destination: str,
        mode: WriteMode = WriteMode.OVERWRITE,
        **kwargs,
    ) -> None:
        """
        Write DataFrame to SQL Server table.

        Args:
            df: Polars DataFrame to write.
            destination: Target table name.
            mode: Write mode controlling overwrite behavior.
                OVERWRITE_PARTITIONS falls back to OVERWRITE (not applicable to SQL).
            **kwargs: Additional arguments passed to df.write_database.
        """
        # Map WriteMode to Polars if_table_exists values
        mode_mapping = {
            WriteMode.OVERWRITE: "replace",
            WriteMode.APPEND: "append",
            WriteMode.FAIL_IF_EXISTS: "fail",
            WriteMode.OVERWRITE_PARTITIONS: "replace",  # Not applicable to SQL
        }
        if_table_exists = mode_mapping[mode]

        df.write_database(
            table_name=destination,
            connection=self.connection_string,
            if_table_exists=if_table_exists,
            **kwargs,
        )


# =============================================================================
# Hive-Partitioned Data Lake Implementations (DuckDB)
# =============================================================================


class HiveReader(Reader):
    """Read hive-partitioned parquet data lakes using DuckDB."""

    def __init__(self, base_path: str | Path | None = None):
        """
        Args:
            base_path: Optional base path for relative source paths.
        """
        self.base_path = Path(base_path) if base_path else None

    def read(
        self,
        source: str,
        hive_partitioning: bool = True,
        **kwargs,
    ) -> pl.DataFrame:
        """
        Read hive-partitioned parquet files.

        Args:
            source: Path to parquet files (can include globs like '**/*.parquet').
            hive_partitioning: Whether to parse hive partition columns.
            **kwargs: Additional arguments passed to DuckDB.

        Returns:
            Polars DataFrame.
        """
        path = self._resolve_path(source)

        # Build DuckDB query
        hive_opt = "true" if hive_partitioning else "false"
        query = f"SELECT * FROM read_parquet('{path}', hive_partitioning={hive_opt})"

        # Execute with DuckDB and convert to Polars
        with duckdb.connect() as con:
            result = con.execute(query).pl()

        return result

    def _resolve_path(self, source: str) -> str:
        """Resolve path relative to base_path if set."""
        if self.base_path:
            return str(self.base_path / source)
        return source


class HiveWriter(Writer):
    """Write hive-partitioned parquet data lakes using DuckDB."""

    def __init__(self, base_path: str | Path | None = None):
        """
        Args:
            base_path: Optional base path for relative destination paths.
        """
        self.base_path = Path(base_path) if base_path else None

    def write(
        self,
        df: pl.DataFrame,
        destination: str,
        partition_by: list[str] | None = None,
        mode: WriteMode = WriteMode.OVERWRITE,
        **kwargs,
    ) -> None:
        """
        Write DataFrame as hive-partitioned parquet.

        Args:
            df: Polars DataFrame to write.
            destination: Directory path for output.
            partition_by: Columns to partition by (creates hive-style directories).
            mode: Write mode controlling overwrite behavior.
            **kwargs: Additional arguments passed to DuckDB COPY.
        """
        path = self._resolve_path(destination)
        dest_path = Path(path)

        # Handle FAIL_IF_EXISTS mode
        if mode == WriteMode.FAIL_IF_EXISTS:
            if self._destination_has_data(dest_path):
                logger.warning(
                    f"Destination '{path}' already has data. Skipping write (mode=FAIL_IF_EXISTS)."
                )
                return

        # Handle OVERWRITE_PARTITIONS mode
        if mode == WriteMode.OVERWRITE_PARTITIONS and partition_by:
            self._delete_partitions(dest_path, df, partition_by)

        dest_path.mkdir(parents=True, exist_ok=True)

        with duckdb.connect() as con:
            # Register the Polars DataFrame
            con.register("df", df.to_arrow())

            # Build COPY statement
            options = ["FORMAT PARQUET"]
            if partition_by:
                cols = ", ".join(partition_by)
                options.append(f"PARTITION_BY ({cols})")
            # DuckDB requires OVERWRITE_OR_IGNORE for non-empty directories
            # APPEND still works correctly as new partition files get unique names
            if mode != WriteMode.FAIL_IF_EXISTS:
                options.append("OVERWRITE_OR_IGNORE")

            options_str = ", ".join(options)
            query = f"COPY df TO '{path}' ({options_str})"
            con.execute(query)

    def _resolve_path(self, destination: str) -> str:
        """Resolve path relative to base_path if set."""
        if self.base_path:
            return str(self.base_path / destination)
        return destination

    def _destination_has_data(self, path: Path) -> bool:
        """Check if destination path has any parquet files."""
        if not path.exists():
            return False
        return any(path.rglob("*.parquet"))

    def _get_existing_partitions(
        self, path: Path, partition_cols: list[str]
    ) -> list[dict[str, Any]]:
        """Get existing partition values from the destination directory."""
        if not path.exists():
            return []

        parquet_files = list(path.rglob("*.parquet"))
        if not parquet_files:
            return []

        glob_pattern = str(path / "**" / "*.parquet")
        with duckdb.connect() as con:
            query = f"""
                SELECT DISTINCT {", ".join(partition_cols)}
                FROM read_parquet('{glob_pattern}', hive_partitioning=true)
            """
            result = con.execute(query).fetchall()
            columns = partition_cols
            return [dict(zip(columns, row)) for row in result]

    def _delete_partitions(
        self, path: Path, df: pl.DataFrame, partition_cols: list[str]
    ) -> None:
        """Delete partition directories that match partitions in the DataFrame."""
        if not path.exists():
            return

        # Get unique partition values from the DataFrame
        partition_values = df.select(partition_cols).unique().to_dicts()

        for partition in partition_values:
            # Build the partition directory path (e.g., year=2023/month=01)
            partition_path = path
            for col in partition_cols:
                partition_path = partition_path / f"{col}={partition[col]}"

            if partition_path.exists():
                logger.info(f"Deleting partition directory: {partition_path}")
                shutil.rmtree(partition_path)


# =============================================================================
# Excel Writer
# =============================================================================


class ExcelWriter(Writer):
    """Write data to Excel files for testing/one-off exports."""

    def __init__(self, base_path: str | Path | None = None):
        """
        Args:
            base_path: Optional base path for relative destination paths.
        """
        self.base_path = Path(base_path) if base_path else None

    def write(
        self,
        df: pl.DataFrame,
        destination: str,
        sheet_name: str = "Sheet1",
        mode: WriteMode = WriteMode.OVERWRITE,
        **kwargs,
    ) -> None:
        """
        Write DataFrame to Excel file.

        Args:
            df: Polars DataFrame to write.
            destination: Path to .xlsx file.
            sheet_name: Name of the worksheet.
            mode: Write mode controlling overwrite behavior.
                APPEND mode logs a warning (not fully supported for Excel).
                OVERWRITE_PARTITIONS behaves like OVERWRITE.
            **kwargs: Additional arguments passed to df.write_excel.
        """
        path = self._resolve_path(destination)
        file_path = Path(path)

        # Handle FAIL_IF_EXISTS mode
        if mode == WriteMode.FAIL_IF_EXISTS:
            if file_path.exists():
                logger.warning(
                    f"File '{path}' already exists. Skipping write (mode=FAIL_IF_EXISTS)."
                )
                return

        # Handle APPEND mode (not fully supported for Excel)
        if mode == WriteMode.APPEND:
            logger.warning(
                "APPEND mode is not fully supported for Excel. "
                "Overwriting file instead."
            )

        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_excel(path, worksheet=sheet_name, **kwargs)

    def _resolve_path(self, destination: str) -> str:
        """Resolve path relative to base_path if set."""
        if self.base_path:
            return str(self.base_path / destination)
        return destination


# =============================================================================
# IO Manager
# =============================================================================


class IOManager:
    """
    Composable IO Manager for ETL pipelines.

    Combines a Reader and Writer to handle data input/output.
    Reader and Writer can be different types (e.g., read from SQL, write to parquet).
    """

    def __init__(self, reader: Reader, writer: Writer):
        """
        Args:
            reader: Reader instance for data input.
            writer: Writer instance for data output.
        """
        self.reader = reader
        self.writer = writer

    def read(self, source: str, **kwargs) -> pl.DataFrame:
        """Read data using the configured reader."""
        return self.reader.read(source, **kwargs)

    def write(self, df: pl.DataFrame, destination: str, **kwargs) -> None:
        """Write data using the configured writer."""
        self.writer.write(df, destination, **kwargs)

    def transfer(
        self,
        source: str,
        destination: str,
        transform: Callable | None = None,
        read_kwargs: dict[str, Any] | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """
        Read, optionally transform, and write data.

        Args:
            source: Source for reader.
            destination: Destination for writer.
            transform: Optional function to transform the DataFrame.
            read_kwargs: Arguments passed to reader.
            write_kwargs: Arguments passed to writer.

        Returns:
            The (possibly transformed) DataFrame.
        """
        read_kwargs = read_kwargs or {}
        write_kwargs = write_kwargs or {}

        df = self.read(source, **read_kwargs)

        if transform:
            df = transform(df)

        self.write(df, destination, **write_kwargs)
        return df


# =============================================================================
# Factory Functions
# =============================================================================


def create_sql_to_lake_manager(
    sql_connection_string: str,
    lake_base_path: str | Path,
) -> IOManager:
    """Create an IOManager that reads from SQL Server and writes to a data lake."""
    return IOManager(
        reader=SqlServerReader(sql_connection_string),
        writer=HiveWriter(lake_base_path),
    )


def create_lake_to_sql_manager(
    lake_base_path: str | Path,
    sql_connection_string: str,
) -> IOManager:
    """Create an IOManager that reads from a data lake and writes to SQL Server."""
    return IOManager(
        reader=HiveReader(lake_base_path),
        writer=SqlServerWriter(sql_connection_string),
    )


def create_sql_to_excel_manager(
    sql_connection_string: str,
    excel_base_path: str | Path | None = None,
) -> IOManager:
    """Create an IOManager that reads from SQL Server and writes to Excel."""
    return IOManager(
        reader=SqlServerReader(sql_connection_string),
        writer=ExcelWriter(excel_base_path),
    )
