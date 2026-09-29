"""
Ingest cert-validator Parquet output into the cert_validator table.

The files live in a Hive-style layout where the day=<D> entry is itself the Parquet
file (no .parquet extension):
    .../tool=cert-validator/format=parquet/port=<port>/year=<Y>/month=<M>/day=<D>

A day=<D> directory holding *.parquet files is accepted too. Every candidate is checked
for the Parquet magic bytes (PAR1) and the files to be read are listed before loading.
port and scan_date are derived from the path and stored as columns.

Usage:
    python ingest_cert_validator.py
    python ingest_cert_validator.py --src <other base dir>
"""

import sys
import time
from pathlib import Path

from misc import base_arg_parser, close_db, open_db, print_schema, total_size_gb

TABLE = "cert_validator"
SRC_DIR = "research_data/ldap-sequel/catrin/data_processing/tool=cert-validator/format=parquet"
FILE_GLOB = "port=*/year=*/month=*/day=*"
PATH_REGEX = r"port=(\d+)/year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})"
PARQUET_MAGIC = b"PAR1"


def is_parquet(path: Path) -> bool:
    """True if the file starts with the Parquet magic bytes, whatever its extension."""
    try:
        with path.open("rb") as fh:
            return fh.read(4) == PARQUET_MAGIC
    except OSError:
        return False


def find_parquet_files(src_dir: Path) -> list[Path]:
    """Collect day=<D> entries: the entry itself if it's a file, or the *.parquet files inside it."""
    candidates = []
    for entry in sorted(src_dir.glob(FILE_GLOB)):
        if entry.is_file():
            candidates.append(entry)
        elif entry.is_dir():
            candidates.extend(sorted(entry.glob("*.parquet")))

    files = []
    for f in candidates:
        if is_parquet(f):
            files.append(f)
        else:
            print(f"  skipping (not Parquet): {f.relative_to(src_dir)}")
    return files


def ingest_cert_validator(con, src_dir: Path) -> None:
    files = find_parquet_files(src_dir)
    if not files:
        sys.exit(f"No Parquet files matching {FILE_GLOB} found in {src_dir.resolve()}")

    print(f"[{TABLE}] reading {len(files)} Parquet file(s), {total_size_gb(files):.2f} GB:")
    for f in files:
        print(f"  {f.relative_to(src_dir)}  ({f.stat().st_size / 1e9:.2f} GB)")

    # Normalise Windows separators so the path regex always matches.
    path = "replace(filename, '\\', '/')"

    def part(i: int) -> str:
        return f"regexp_extract({path}, '{PATH_REGEX}', {i})::INTEGER"

    start = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {TABLE} AS
        SELECT
            {part(1)} AS port,
            make_date({part(2)}, {part(3)}, {part(4)}) AS scan_date,
            * EXCLUDE (filename)
        FROM read_parquet(
            ?,
            hive_partitioning = false,   -- day=<D> is a file, so hive parsing would miss it
            union_by_name = true,
            filename = true
        )
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
    args = base_arg_parser(__doc__, src_default=SRC_DIR, csv=False).parse_args()
    con = open_db(args.db)
    ingest_cert_validator(con, Path(args.src))
    close_db(con, args.db)


if __name__ == "__main__":
    main()
