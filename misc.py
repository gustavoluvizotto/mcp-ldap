"""Shared helpers for the DuckDB ingest scripts."""

import argparse
from pathlib import Path

import duckdb

SRC_DIR = "research_data/ldap-sequel/processing"
DB_PATH = "research_data/ldap-sequel/ldap.duckdb"


def base_arg_parser(description: str, src_default: str = SRC_DIR, csv: bool = True) -> argparse.ArgumentParser:
    """Argument parser with the options every ingest script shares.

    csv=True adds the CSV-only options (--all-varchar, --ignore-errors).
    """
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", default=src_default, help="Directory containing the input files")
    parser.add_argument("--db", default=DB_PATH, help="DuckDB database file")
    if csv:
        parser.add_argument("--all-varchar", action="store_true", help="Read every column as text")
        parser.add_argument("--ignore-errors", action="store_true", help="Skip malformed rows instead of failing")
    return parser


def open_db(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open (or create) the database for writing, with a temp dir for spilling."""
    db = Path(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db.as_posix())
    con.execute(f"SET temp_directory = '{db.with_suffix('.tmp').as_posix()}'")
    return con


def close_db(con: duckdb.DuckDBPyConnection, db_path: str | Path) -> None:
    """Flush to disk, close, and report the database size."""
    con.execute("CHECKPOINT")
    con.close()
    db = Path(db_path)
    print(f"\nDatabase written: {db.resolve()} ({db.stat().st_size / 1e9:.2f} GB)")


def csv_options(all_varchar: bool = False, ignore_errors: bool = False, extra: list[str] | None = None) -> str:
    """Build the option list for DuckDB's read_csv()."""
    opts = ["header = true", "sample_size = -1"]
    if all_varchar:
        opts.append("all_varchar = true")
    if ignore_errors:
        opts.append("ignore_errors = true")
    opts.extend(extra or [])
    return ", ".join(opts)


def types_option(types: dict[str, str]) -> str:
    """Turn {'col': 'TYPE', ...} into a read_csv types = {...} option."""
    inner = ", ".join(f"'{col}': '{dtype}'" for col, dtype in types.items())
    return f"types = {{{inner}}}"


def print_file_size(path: Path) -> None:
    print(f"  {path.name}  ({path.stat().st_size / 1e9:.2f} GB)")


def total_size_gb(paths: list[Path]) -> float:
    return sum(p.stat().st_size for p in paths) / 1e9


def print_schema(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Print total row count and column types of a table."""
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    if total is None:
        print(f"Cannot print totals. Is the ingest file ok?")
    else:
        total = total[0]
        print(f"Total rows in {table}: {total:,}")
        print("Schema:")
    for name, dtype, *_ in con.execute(f"DESCRIBE {table}").fetchall():
        print(f"  {name:<40} {dtype}")


# --- Hive-style path helpers (.../port=<P>/.../year=<Y>/month=<M>/day=<D>/...) ---------------

GOSCANNER_RAW_DIR = "research_data/ldap-sequel/catrin/measurements/tool=goscanner/format=raw"
HIVE_PORT_REGEX = r"port=(\d+)/"
HIVE_DATE_REGEX = r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})"


def hive_path_columns_sql(filename_col: str = "filename") -> str:
    """SQL select-list items deriving `port` and `scan_date` from a file path column.

    Use with read_csv/read_parquet(..., filename = true, hive_partitioning = false).
    Backslashes are normalised so the regexes also match Windows paths.
    """
    path = f"replace({filename_col}, '\\', '/')"

    def date_part(i: int) -> str:
        return f"regexp_extract({path}, '{HIVE_DATE_REGEX}', {i})::INTEGER"

    return (
        f"regexp_extract({path}, '{HIVE_PORT_REGEX}', 1)::INTEGER AS port,\n"
        f"            make_date({date_part(1)}, {date_part(2)}, {date_part(3)}) AS scan_date"
    )


def find_files(src_dir: Path, pattern: str) -> list[Path]:
    """Sorted regular files under src_dir matching a glob pattern."""
    return sorted(p for p in src_dir.glob(pattern) if p.is_file())
