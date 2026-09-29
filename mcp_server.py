"""
LAN-only MCP server that gives read-only SQL access to the ldap.duckdb database.

Runs on one laptop and serves MCP over Streamable HTTP so another laptop on the
same Wi-Fi can use it. Every request must pass two checks:

  1. the client IP is inside an allowed local network (default: this machine's
     own subnet), and
  2. the request carries  Authorization: Bearer <token>.

The database is opened read-only with DuckDB's file-system access disabled, so
SQL cannot modify the data, read other files, ATTACH databases or load extensions.

Usage:
    python mcp_server.py                         # bind to this laptop's LAN IP, port 8765
    python mcp_server.py --port 9000
    python mcp_server.py --allow 192.168.1.0/24  # set the allowed subnet explicitly
    python mcp_server.py --show-token            # print the token (to set up the client)
    python mcp_server.py --rotate-token          # replace the token

The token is created on first run and kept in .mcp_token (git-ignored). It can
also be supplied with the LDAP_MCP_TOKEN environment variable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import hmac
import ipaddress
import json
import logging
import os
import re
import secrets
import socket
import sys
import threading
import uuid
from pathlib import Path
from typing import Any

import duckdb
import uvicorn
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE / "research_data/ldap-sequel/ldap.duckdb"
TOKEN_FILE = HERE / ".mcp_token"
DEFAULT_PORT = 8765

MAX_ROWS_CAP = 5000          # hard ceiling on rows returned by one query
MAX_CELL_CHARS = 2000        # long text values are truncated in results
DEFAULT_TIMEOUT_S = 60

log = logging.getLogger("ldap-mcp")


# --------------------------------------------------------------------------- database

class Database:
    """Opens a fresh read-only, sandboxed connection for each call.

    Short-lived connections mean the ingest scripts can still open the file for
    writing whenever the server is idle (DuckDB allows one writer process, and
    no readers while it writes).
    """

    def __init__(self, path: Path, timeout_s: int, memory_limit: str | None):
        self.path = path
        self.timeout_s = timeout_s
        self.config: dict[str, Any] = {
            "enable_external_access": False,   # no read_csv('/etc/...'), COPY, ATTACH, INSTALL
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
            "lock_configuration": True,        # SQL cannot switch the above back on
        }
        if memory_limit:
            self.config["memory_limit"] = memory_limit

    def run(self, sql: str, params: list[Any] | None = None, max_rows: int = MAX_ROWS_CAP):
        if not self.path.exists():
            raise ToolError(f"Database not found: {self.path}")
        try:
            con = duckdb.connect(self.path.as_posix(), read_only=True, config=self.config)
        except duckdb.IOException as e:
            if "lock" in str(e).lower():
                raise ToolError("The database is locked by another process (an ingest script is probably "
                                   "writing to it). Try again when it has finished.") from e
            raise ToolError(f"Cannot open database: {e}") from e
        timer = threading.Timer(self.timeout_s, con.interrupt)
        timer.start()
        try:
            cur = con.execute(sql, params or [])
            if cur.description is None:
                return [], [], False
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            return columns, rows[:max_rows], truncated
        except duckdb.InterruptException as e:
            raise ToolError(f"Query cancelled after {self.timeout_s}s timeout. Add filters or a LIMIT.") from e
        except duckdb.Error as e:
            raise ToolError(f"{type(e).__name__}: {e}") from e
        finally:
            timer.cancel()
            con.close()


def _cell(v: Any) -> Any:
    """Make a DuckDB value JSON-friendly."""
    if v is None or isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, str):
        return v if len(v) <= MAX_CELL_CHARS else v[:MAX_CELL_CHARS] + f"… [{len(v) - MAX_CELL_CHARS} more chars]"
    if isinstance(v, (dt.date, dt.datetime, dt.time, uuid.UUID)):
        return v.isoformat() if hasattr(v, "isoformat") else str(v)
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        return "0x" + b[:256].hex() + ("…" if len(b) > 256 else "")
    if isinstance(v, dict):
        return {str(k): _cell(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_cell(x) for x in v]
    return str(v)


def _result(columns: list[str], rows: list[tuple], truncated: bool) -> str:
    out = {
        "columns": columns,
        "rows": [[_cell(v) for v in r] for r in rows],
        "row_count": len(rows),
        "truncated": truncated,
    }
    return json.dumps(out, ensure_ascii=False, default=str)


# Single statement that only reads. The read-only connection is the real
# guarantee; this check just gives a clearer error message.
_READ_PREFIX = re.compile(r"^\s*(\(?\s*)*(select|with|from|values|describe|summarize|show|explain|pragma|table)\b",
                          re.IGNORECASE)
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _check_select(sql: str) -> str:
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise ToolError("Empty query.")
    if ";" in _strip_literals(stripped):
        raise ToolError("Only one SQL statement per call.")
    if not _READ_PREFIX.match(stripped):
        raise ToolError("Only read queries are allowed (SELECT, WITH, FROM, DESCRIBE, SUMMARIZE, SHOW, EXPLAIN).")
    return stripped


def _strip_literals(sql: str) -> str:
    """Remove quoted strings/identifiers and comments so we can look for ';'."""
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    return re.sub(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"", "''", sql)


def _check_table(db: Database, table: str) -> str:
    if not _IDENT.match(table):
        raise ToolError(f"Not a valid table name: {table!r}")
    _, rows, _ = db.run(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE lower(table_name) = lower(?) "
        "UNION ALL SELECT schema_name, view_name FROM duckdb_views() WHERE NOT internal AND lower(view_name) = lower(?)",
        [table.split(".")[-1], table.split(".")[-1]],
    )
    if not rows:
        raise ToolError(f"No table or view named {table!r}. Use list_tables to see what exists.")
    schema, name = rows[0]
    return f'"{schema}"."{name}"'


# --------------------------------------------------------------------------- MCP server

def build_server(db: Database) -> MCPServer:
    mcp = MCPServer(
        name="ldap-duckdb",
        instructions=(
            "Read-only access to an LDAP internet-measurement research database (DuckDB). "
            "Tables hold goscanner scan results (TCP hosts, LDAP/LDAPS/StartTLS responses, root DSE), "
            "certificates, certificate validation, and IP/ASN network statistics, mostly keyed by "
            "(id, port, scan_date). Start with list_tables and describe_table, then use query. "
            "Use DuckDB SQL; aggregate or LIMIT large tables since results are capped."
        ),
    )
    ro = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

    @mcp.tool(annotations=ro, structured_output=False)
    def list_tables() -> str:
        """List all tables and views with their approximate row counts and column counts."""
        cols, rows, trunc = db.run(
            """
            SELECT table_name AS name, 'table' AS kind, estimated_size AS approx_rows, column_count, comment
            FROM duckdb_tables()
            UNION ALL
            SELECT view_name, 'view', NULL, column_count, comment FROM duckdb_views() WHERE NOT internal
            ORDER BY name
            """
        )
        return _result(cols, rows, trunc)

    @mcp.tool(annotations=ro, structured_output=False)
    def describe_table(table: str) -> str:
        """Show the columns and types of a table or view."""
        ident = _check_table(db, table)
        return _result(*db.run(f"DESCRIBE {ident}"))

    @mcp.tool(annotations=ro, structured_output=False)
    def sample_rows(table: str, n: int = 10) -> str:
        """Return up to n (max 100) example rows from a table or view."""
        ident = _check_table(db, table)
        n = max(1, min(int(n), 100))
        return _result(*db.run(f"SELECT * FROM {ident} LIMIT {n}"))

    @mcp.tool(annotations=ro, structured_output=False)
    def query(sql: str, max_rows: int = 200) -> str:
        """Run one read-only DuckDB SQL statement and return the result as JSON.

        Allowed: SELECT / WITH / FROM / DESCRIBE / SUMMARIZE / SHOW / EXPLAIN.
        max_rows limits the returned rows (default 200, max 5000); 'truncated'
        is true when more rows existed. Prefer aggregation over pulling raw rows.
        """
        stmt = _check_select(sql)
        limit = max(1, min(int(max_rows), MAX_ROWS_CAP))
        return _result(*db.run(stmt, max_rows=limit))

    return mcp


# --------------------------------------------------------------------------- network guard

class LanGuard:
    """ASGI middleware: allow only clients from allowed networks carrying the bearer token."""

    def __init__(self, app, networks: list[ipaddress._BaseNetwork], token: str):
        self.app = app
        self.networks = networks
        self.token = token.encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        client_ip = (scope.get("client") or ("", 0))[0]
        if not self._ip_allowed(client_ip):
            log.warning("Rejected %s: address not in allowed networks", client_ip or "unknown")
            return await _deny(send, 403, "Forbidden: only local-network clients are allowed.")

        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization", b"")
        scheme, _, presented = auth.partition(b" ")
        if scheme.lower() != b"bearer" or not hmac.compare_digest(presented.strip(), self.token):
            log.warning("Rejected %s: missing or wrong token", client_ip)
            return await _deny(send, 401, "Unauthorized: missing or invalid bearer token.",
                               [(b"www-authenticate", b"Bearer")])

        return await self.app(scope, receive, send)

    def _ip_allowed(self, ip: str) -> bool:
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
            addr = addr.ipv4_mapped
        return any(addr in net for net in self.networks)


async def _deny(send, status: int, text: str, extra_headers=None):
    body = text.encode()
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"text/plain; charset=utf-8"),
                            (b"content-length", str(len(body)).encode())] + (extra_headers or [])})
    await send({"type": "http.response.body", "body": body})


def detect_lan_ip() -> str | None:
    """Best guess at this machine's Wi-Fi/LAN IPv4 address (no packets are sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 9))  # TEST-NET address; only picks the outgoing interface
        ip = s.getsockname()[0]
        return None if ip.startswith("127.") else ip
    except OSError:
        return None
    finally:
        s.close()


def load_token(rotate: bool) -> str:
    env = os.environ.get("LDAP_MCP_TOKEN")
    if env and not rotate:
        return env.strip()
    if TOKEN_FILE.exists() and not rotate:
        return TOKEN_FILE.read_text().strip()
    token = secrets.token_urlsafe(32)
    TOKEN_FILE.write_text(token + "\n")
    TOKEN_FILE.chmod(0o600)
    print(f"New access token written to {TOKEN_FILE}")
    return token


# --------------------------------------------------------------------------- main

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=Path(os.environ.get("LDAP_MCP_DB", DEFAULT_DB)), help="DuckDB file")
    p.add_argument("--host", default=os.environ.get("LDAP_MCP_HOST"),
                   help="Address to listen on (default: this laptop's LAN IP)")
    p.add_argument("--port", type=int, default=int(os.environ.get("LDAP_MCP_PORT", DEFAULT_PORT)))
    p.add_argument("--allow", action="append", metavar="CIDR",
                   help="Allowed client network, repeatable (default: the /24 of the LAN IP)")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="Per-query timeout in seconds")
    p.add_argument("--memory-limit", default=None, help="DuckDB memory_limit, e.g. 4GB")
    p.add_argument("--show-token", action="store_true", help="Print the access token and exit")
    p.add_argument("--rotate-token", action="store_true", help="Generate a new token (old one stops working)")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    token = load_token(args.rotate_token)
    if args.show_token:
        print(token)
        return

    lan_ip = detect_lan_ip()
    host = args.host or lan_ip
    if not host:
        sys.exit("Could not detect a LAN IP address. Are you connected to Wi-Fi? Pass --host <ip> explicitly.")
    if host not in ("0.0.0.0", "::") and not ipaddress.ip_address(host).is_private:
        sys.exit(f"Refusing to listen on non-private address {host}. Only local-network addresses are allowed.")

    if args.allow:
        networks = [ipaddress.ip_network(c, strict=False) for c in args.allow]
    else:
        base = lan_ip or host
        if base in ("0.0.0.0", "::"):
            sys.exit("With --host 0.0.0.0 you must also pass --allow <your LAN CIDR>, e.g. 192.168.1.0/24")
        networks = [ipaddress.ip_network(f"{base}/24", strict=False)]
    for net in networks:
        if not net.is_private:
            sys.exit(f"--allow {net} is not a private (local) network. Only local access is permitted.")
    networks.append(ipaddress.ip_network("127.0.0.0/8"))  # local testing on the server itself

    db = Database(args.db.resolve(), args.timeout, args.memory_limit)
    if not db.path.exists():
        log.warning("Database %s does not exist yet; tools will fail until it is created.", db.path)

    mcp = build_server(db)
    allowed_hosts = [f"{h}:*" for h in {host, lan_ip, "127.0.0.1", "localhost", socket.gethostname(),
                                        socket.gethostname().split(".")[0] + ".local"} if h]
    app = mcp.streamable_http_app(
        stateless_http=True,
        json_response=True,
        host=host,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed_hosts,
            allowed_origins=[],  # browsers (Origin header) are rejected; MCP clients send none
        ),
    )
    guarded = LanGuard(app, networks, token)

    shown = lan_ip if host in ("0.0.0.0", "::") else host
    print("\nldap-duckdb MCP server")
    print(f"  database : {db.path}")
    print(f"  endpoint : http://{shown}:{args.port}/mcp")
    print(f"  allowed  : {', '.join(str(n) for n in networks)}")
    print(f"  token    : run  python {Path(__file__).name} --show-token\n")

    uvicorn.run(guarded, host=host, port=args.port, log_level="info", proxy_headers=False,
                server_header=False, date_header=False)


if __name__ == "__main__":
    main()
