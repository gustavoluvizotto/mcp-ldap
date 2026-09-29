"""
Ingest goscanner TLS certificate CSVs into the goscanner_certs table.

The files live in a Hive-style layout, one file per port and scan date:
    .../tool=goscanner/format=raw/port=<port>/scan=tls/result=certs/year=<Y>/month=<M>/day=<D>/certs.csv

All files share one schema. Expected ports: 389, 636, 3268, 3269.
port and scan_date are derived from the path and stored as columns.
Join: id -> elements of goscanner_tcp_hosts.peer_certificates (same port and scan_date);
goscanner_tcp_hosts.cert_id is the leaf certificate.

Usage:
    python ingest_goscanner_certs.py
    python ingest_goscanner_certs.py --src <other base dir>
"""

import sys
import time
from pathlib import Path

from misc import (
    GOSCANNER_RAW_DIR,
    base_arg_parser,
    close_db,
    csv_options,
    find_files,
    hive_path_columns_sql,
    open_db,
    print_schema,
    total_size_gb,
    types_option,
)

TABLE = "goscanner_certs"
FILE_GLOB = "port=*/scan=tls/result=certs/year=*/month=*/day=*/*.csv"
EXPECTED_PORTS = {389, 636, 3268, 3269}

# cert is a multi-line PEM block; system_cert_store is a 0/1 flag.
COLUMN_TYPES = {
    "id": "BIGINT",
    "cert": "VARCHAR",
    "system_cert_store": "BOOLEAN",
}


def ingest_goscanner_certs(con, src_dir: Path, all_varchar: bool = False, ignore_errors: bool = False) -> None:
    files = find_files(src_dir, FILE_GLOB)
    if not files:
        sys.exit(f"No files matching {FILE_GLOB} found in {src_dir.resolve()}")

    print(f"[{TABLE}] {len(files)} file(s), {total_size_gb(files):.2f} GB:")
    for f in files:
        print(f"  {f.relative_to(src_dir)}  ({f.stat().st_size / 1e9:.2f} GB)")

    extra = ["hive_partitioning = false", "union_by_name = true", "filename = true"]
    if not all_varchar:
        extra.append(types_option(COLUMN_TYPES))
    opts = csv_options(all_varchar, ignore_errors, extra)

    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            {hive_path_columns_sql()},
            COLUMNS(c -> c NOT IN ('filename', 'port'))
        FROM read_csv(?, {opts})
        ORDER BY port, scan_date, id
        """,
        [[f.as_posix() for f in files]],
    )
    print(f"Loaded in {time.perf_counter() - start:.1f}s")

    print("Rows per scan:")
    rows = con.execute(f"SELECT scan_date, port, count(*) FROM {TABLE} GROUP BY ALL ORDER BY port").fetchall()
    for scan_date, port, n in rows:
        print(f"  {scan_date}  port {port:<5}  {n:>12,}")
    unexpected = sorted({port for _, port, _ in rows} - EXPECTED_PORTS)
    if unexpected:
        print(f"  WARNING: unexpected port(s) {unexpected} (expected {sorted(EXPECTED_PORTS)})")
    print_schema(con, TABLE)


def main() -> None:
    args = base_arg_parser(__doc__, src_default=GOSCANNER_RAW_DIR).parse_args()
    con = open_db(args.db)
    ingest_goscanner_certs(con, Path(args.src), args.all_varchar, args.ignore_errors)
    close_db(con, args.db)


if __name__ == "__main__":
    main()
