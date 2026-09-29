"""
Ingest LDAP root DSE fingerprint CSVs into a DuckDB database.

Expected file names: YYYYMMDD_<port>_ldap_root_dse_fp.csv
The scan date and port are parsed from the file name and stored as columns,
so rows from different scans stay distinguishable after merging.

Usage:
    python ingest_ldap.py
    python ingest_ldap.py --src research_data/ldap-sequel/processing --db ldap.duckdb
    python ingest_ldap.py --parquet ldap_root_dse.parquet
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb

FILE_PATTERN = "*_ldap_root_dse_fp.csv"
FILENAME_REGEX = r"(\d{8})_(\d+)_ldap_root_dse_fp\.csv$"
TABLE = "ldap_root_dse_fp"


def main(args) -> None:
    global FILE_PATTERN, FILENAME_REGEX, TABLE

    src = Path(args.src)
    files = sorted(src.glob(FILE_PATTERN))
    if not files:
        sys.exit(f"No files matching {FILE_PATTERN} found in {src.resolve()}")
 
    print(f"Found {len(files)} file(s):")
    for f in files:
        print(f"  {f.name}  ({f.stat().st_size / 1e9:.2f} GB)")
 
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
 
    # Let DuckDB use a temp dir next to the DB if it needs to spill to disk
    con.execute(f"SET temp_directory = '{Path(args.db).with_suffix('.tmp').as_posix()}'")
 
    file_list = [f.as_posix() for f in files]
    read_opts = [
        "header = true",
        "sample_size = -1",      # scan all rows for accurate type inference
        "union_by_name = true",  # tolerate column-order differences between files
        "filename = true",       # adds a 'filename' column we parse below
    ]
    if args.all_varchar:
        read_opts.append("all_varchar = true")
    if args.ignore_errors:
        read_opts.append("ignore_errors = true")
 
    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            strptime(regexp_extract(filename, '{FILENAME_REGEX}', 1), '%Y%m%d')::DATE AS scan_date,
            * EXCLUDE (filename)
        FROM read_csv(?, {", ".join(read_opts)})
        ORDER BY port, scan_date
        """,
        [file_list],
    )
    print(f"\nLoaded in {time.perf_counter() - start:.1f}s")
 
    # Summary
    print("\nSchema:")
    for name, dtype, *_ in con.execute(f"DESCRIBE {TABLE}").fetchall():
        print(f"  {name:<40} {dtype}")

    print("Rows per scan:")
    for scan_date, port, n in con.execute(
        f"SELECT scan_date, port, count(*) FROM {TABLE} GROUP BY ALL ORDER BY port"
    ).fetchall():
        print(f"  {scan_date}  port {port:<5}  {n:,}")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()
    if total is not None:
        total = total[0]
        print(f"Total rows: {total:,}\n")
    else:
        print(f"Could not count the table. Is the input file ok?")

    if args.parquet:
        con.execute(f"COPY {TABLE} TO '{args.parquet}' (FORMAT parquet, COMPRESSION zstd)")
        print(f"\nExported Parquet: {args.parquet}")
 
    con.execute("CHECKPOINT")
    con.close()
    print(f"\nDatabase written: {Path(args.db).resolve()} ({Path(args.db).stat().st_size / 1e9:.2f} GB)")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--src", default="research_data/ldap-sequel/processing", help="Directory containing the CSV files")
    parser.add_argument("--db", default="research_data/ldap-sequel/ldap.duckdb", help="DuckDB database file to create/replace")

    parser.add_argument("--parquet", help="Optional: also export the table to this Parquet file")
    parser.add_argument("--all-varchar", action="store_true", help="Read every column as text (use if type inference fails)")
    parser.add_argument("--ignore-errors", action="store_true", help="Skip malformed rows instead of failing")

    args = parser.parse_args()

    main(args)
