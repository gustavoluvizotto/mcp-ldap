"""
Ingest the goscanner IPv4 netstats + ip2location CSV into the goscanner_netstats table.

Usage:
    python ingest_goscanner_netstats.py
    python ingest_goscanner_netstats.py --file other_month_goscanner.csv
"""

import sys
import time
from pathlib import Path

from misc import base_arg_parser, close_db, csv_options, open_db, print_file_size, print_schema, types_option

TABLE = "goscanner_netstats"
DEFAULT_FILE = "202411_goscanner_ipv4_netstats_ip2location.csv"

# List-like and free-text columns stay VARCHAR so DuckDB doesn't misread them.
# 'date' is left out on purpose so DuckDB infers DATE or TIMESTAMP.
COLUMN_TYPES = {
    "ip": "VARCHAR",
    "port": "INTEGER",
    "id": "BIGINT",
    "leaf_data_names": "VARCHAR",
    "subject_rdns": "VARCHAR",
    "cipher": "VARCHAR",
    "protocol": "VARCHAR",
    "pubkey_bit_size": "INTEGER",
    "attribute_names": "VARCHAR",
    "attribute_values_list": "VARCHAR",
    "chain_error": "VARCHAR",
    "ip_decimal": "BIGINT",
    "cc": "VARCHAR",
    "c_name": "VARCHAR",
    "isp": "VARCHAR",
    "usage_type": "VARCHAR",
}


def ingest_goscanner_netstats(con, csv_path: Path, all_varchar: bool = False, ignore_errors: bool = False) -> None:
    if not csv_path.exists():
        sys.exit(f"File not found: {csv_path.resolve()}")

    print(f"[{TABLE}]")
    print_file_size(csv_path)

    extra = [] if all_varchar else [types_option(COLUMN_TYPES)]
    opts = csv_options(all_varchar, ignore_errors, extra)

    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT *
        FROM read_csv(?, {opts})
        ORDER BY port, ip_decimal   -- speeds up filters on port and IP ranges
        """,
        [csv_path.as_posix()],
    )
    print(f"Loaded in {time.perf_counter() - start:.1f}s")

    print("Rows per port:")
    for port, n in con.execute(f"SELECT port, count(*) FROM {TABLE} GROUP BY ALL ORDER BY port").fetchall():
        print(f"  port {port:<5}  {n:,}")
    print_schema(con, TABLE)


def main() -> None:
    parser = base_arg_parser(__doc__)
    parser.add_argument("--file", default=DEFAULT_FILE, help="CSV file name inside --src")
    args = parser.parse_args()

    con = open_db(args.db)
    ingest_goscanner_netstats(con, Path(args.src) / args.file, args.all_varchar, args.ignore_errors)
    close_db(con, args.db)


if __name__ == "__main__":
    main()
