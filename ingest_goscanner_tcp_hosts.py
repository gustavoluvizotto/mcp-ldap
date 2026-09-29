"""
Ingest goscanner TCP/TLS host results (hosts.csv) into the goscanner_tcp_hosts table.

The files live in a Hive-partitioned layout, one file per port and scan date:
    .../tool=goscanner/format=raw/port=<port>/scan=tcp/result=hosts/year=<Y>/month=<M>/day=<D>/hosts.csv

All files share one schema. scan_date is derived from the path; port comes from the CSV itself.
Epoch columns (synStart, synEnd, scanEnd) become TIMESTAMPTZ and the JSON-like list columns
(error_data, tls_alerts_*, peer_certificates) become DuckDB lists, unless --all-varchar is given.

Usage:
    python ingest_goscanner_tcp_hosts.py
    python ingest_goscanner_tcp_hosts.py --src <other base dir> --ignore-errors
"""

import sys
import time
from pathlib import Path

from misc import base_arg_parser, close_db, csv_options, open_db, print_file_size, print_schema, types_option

TABLE = "goscanner_tcp_hosts"
SRC_DIR = "research_data/ldap-sequel/catrin/measurements/tool=goscanner/format=raw"
FILE_GLOB = "port=*/scan=tcp/result=hosts/year=*/month=*/day=*/*.csv"
PATH_DATE_REGEX = r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/"

# cipher is a hex suite ID (e.g. 1301, c02f), so it must stay text.
# protocol is the decimal TLS version (769 = TLS 1.0 ... 772 = TLS 1.3); 0 means no handshake.
COLUMN_TYPES = {
    "id": "BIGINT",
    "ip": "VARCHAR",
    "port": "INTEGER",
    "server_name": "VARCHAR",
    "synStart": "BIGINT",
    "synEnd": "BIGINT",
    "scanEnd": "BIGINT",
    "protocol": "INTEGER",
    "cipher": "VARCHAR",
    "resultString": "VARCHAR",
    "error_data": "VARCHAR",
    "cert_id": "BIGINT",
    "cert_hash": "VARCHAR",
    "pub_key_hash": "VARCHAR",
    "cert_valid": "BOOLEAN",
    "tls_alerts_send": "VARCHAR",
    "peer_certificates": "VARCHAR",
    "tls_alerts_received": "VARCHAR",
    "client_hello": "VARCHAR",
}

# Conversions applied on top of the raw columns (skipped with --all-varchar).
TYPED_COLUMNS = """
            to_timestamp(synStart)                        AS synStart,
            to_timestamp(synEnd)                          AS synEnd,
            to_timestamp(scanEnd)                         AS scanEnd,
            TRY_CAST(error_data AS JSON)::VARCHAR[]       AS error_data,
            TRY_CAST(tls_alerts_send AS JSON)::INTEGER[]  AS tls_alerts_send,
            TRY_CAST(peer_certificates AS JSON)::BIGINT[] AS peer_certificates,
            TRY_CAST(tls_alerts_received AS JSON)::INTEGER[] AS tls_alerts_received"""


def ingest_goscanner_tcp_hosts(con, src_dir: Path, all_varchar: bool = False, ignore_errors: bool = False) -> None:
    files = sorted(src_dir.glob(FILE_GLOB))
    if not files:
        sys.exit(f"No files matching {FILE_GLOB} found in {src_dir.resolve()}")

    print(f"[{TABLE}] {len(files)} file(s):")
    for f in files:
        print(f"  {f.relative_to(src_dir).parent}")
        print_file_size(f)

    extra = ["hive_partitioning = false", "union_by_name = true", "filename = true"]
    if not all_varchar:
        extra.append(types_option(COLUMN_TYPES))
    opts = csv_options(all_varchar, ignore_errors, extra)

    if all_varchar:
        select = "* EXCLUDE (filename)"
    else:
        select = f"* EXCLUDE (filename) REPLACE ({TYPED_COLUMNS}\n        )"
    # Normalise Windows separators so the path regex always matches.
    path = "replace(filename, '\\', '/')"

    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            make_date(
                regexp_extract({path}, '{PATH_DATE_REGEX}', 1)::INTEGER,
                regexp_extract({path}, '{PATH_DATE_REGEX}', 2)::INTEGER,
                regexp_extract({path}, '{PATH_DATE_REGEX}', 3)::INTEGER
            ) AS scan_date,
            {select}
        FROM read_csv(?, {opts})
        ORDER BY port, scan_date, ip
        """,
        [[f.as_posix() for f in files]],
    )
    print(f"Loaded in {time.perf_counter() - start:.1f}s")

    print("Rows per scan:")
    for scan_date, port, n, ok in con.execute(
        f"""
        SELECT scan_date, port, count(*), count(*) FILTER (resultString = 'SUCCESS')
        FROM {TABLE} GROUP BY ALL ORDER BY port, scan_date
        """
    ).fetchall():
        print(f"  {scan_date}  port {port:<5}  {n:>12,} rows  {ok:>10,} SUCCESS")
    print_schema(con, TABLE)


def main() -> None:
    args = base_arg_parser(__doc__, src_default=SRC_DIR).parse_args()
    con = open_db(args.db)
    ingest_goscanner_tcp_hosts(con, Path(args.src), args.all_varchar, args.ignore_errors)
    close_db(con, args.db)


if __name__ == "__main__":
    main()
