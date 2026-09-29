"""
Ingest LDAP root DSE fingerprint CSVs into the ldap_root_dse table.

Expected file names: YYYYMMDD_<port>_ldap_root_dse_fp.csv
scan_date is parsed from the file name; port comes from the CSV itself.

Usage:
    python ingest_rootdse_fps.py
    python ingest_rootdse_fps.py --src research_data/ldap-sequel/processing --db ldap.duckdb
"""

import sys
import time
from pathlib import Path

from misc import base_arg_parser, close_db, csv_options, open_db, print_file_size, print_schema

TABLE = "ldap_root_dse_fps"
FILE_PATTERN = "*_ldap_root_dse_fp.csv"
FILENAME_REGEX = r"(\d{8})_(\d+)_ldap_root_dse_fp\.csv$"


def ingest_rootdse(con, src_dir: Path, all_varchar: bool = False, ignore_errors: bool = False) -> None:
    files = sorted(src_dir.glob(FILE_PATTERN))
    if not files:
        sys.exit(f"No files matching {FILE_PATTERN} found in {src_dir.resolve()}")

    print(f"[{TABLE}] {len(files)} file(s):")
    for f in files:
        print_file_size(f)

    opts = csv_options(all_varchar, ignore_errors, ["union_by_name = true", "filename = true"])
    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            strptime(regexp_extract(filename, '{FILENAME_REGEX}', 1), '%Y%m%d')::DATE AS scan_date,
            * EXCLUDE (filename)
        FROM read_csv(?, {opts})
        ORDER BY port, scan_date
        """,
        [[f.as_posix() for f in files]],
    )
    print(f"Loaded in {time.perf_counter() - start:.1f}s")

    print("Rows per scan:")
    for scan_date, port, n in con.execute(
        f"SELECT scan_date, port, count(*) FROM {TABLE} GROUP BY ALL ORDER BY port"
    ).fetchall():
        print(f"  {scan_date}  port {port:<5}  {n:,}")
    print_schema(con, TABLE)


def main() -> None:
    args = base_arg_parser(__doc__).parse_args()
    con = open_db(args.db)
    ingest_rootdse(con, Path(args.src), args.all_varchar, args.ignore_errors)
    close_db(con, args.db)


if __name__ == "__main__":
    main()
