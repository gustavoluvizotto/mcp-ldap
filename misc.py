"""Shared helpers for the DuckDB ingest scripts."""

import argparse
from pathlib import Path

import duckdb

SRC_DIR = "research_data/ldap-sequel/processing"
DB_PATH = "research_data/ldap-sequel/ldap.duckdb"


def base_arg_parser(description: str) -> argparse.ArgumentParser:
    """Argument parser with the options every ingest script shares."""
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", default=SRC_DIR, help="Directory containing the CSV files")
    parser.add_argument("--db", default=DB_PATH, help="DuckDB database file")
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


def print_schema(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Print total row count and column types of a table."""
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    if total is not None:
        total  = total[0]
        print(f"Total rows in {table}: {total:,}")
        print("Schema:")
    else:
        print(f"Cannot print totals. Is the ingested file correct?")

    for name, dtype, *_ in con.execute(f"DESCRIBE {table}").fetchall():
        print(f"  {name:<40} {dtype}")
