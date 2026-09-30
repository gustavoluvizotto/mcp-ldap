# ldap-duckdb MCP server (LAN only)

> _A.I. generated text._

`mcp_server.py` serves read-only SQL access to `research_data/ldap-sequel/ldap.duckdb`
over MCP (Streamable HTTP). It runs on the **server laptop**; the **client laptop**
connects from Claude Desktop over the same Wi-Fi.

## Tools

| Tool | What it does |
|---|---|
| `list_tables` | Tables/views with approximate row and column counts |
| `describe_table(table)` | Column names and types |
| `sample_rows(table, n=10)` | Up to 100 example rows |
| `query(sql, max_rows=200)` | One read-only DuckDB statement, up to 5000 rows returned |

## How access is restricted

1. **Binds to the LAN IP only** (e.g. `192.168.1.23`), not `0.0.0.0`. It refuses to start on a public address.
2. **Client IP allowlist**: by default only the server's own `/24` subnet (plus localhost). Anything else gets `403`. Public CIDRs are rejected in `--allow`.
3. **Bearer token**: every request needs `Authorization: Bearer <token>`, otherwise `401`. This keeps other devices on the same Wi-Fi out.
4. **Browser requests are refused**: requests with an `Origin` header or an unexpected `Host` header get `403` (DNS-rebinding protection).
5. **Sandboxed, read-only DuckDB**: opened with `read_only=True` and `enable_external_access=false` (locked), so SQL can't write, `COPY`, `ATTACH`, read other files or install extensions. Each query has a 60 s timeout.

Traffic is plain HTTP inside your LAN (the token is visible to anyone sniffing that Wi-Fi). That's fine on a home network you trust. On shared Wi-Fi, don't run it.

---

## 1. Server laptop (the one with the database)

```bash
cd ~/workspace/mcp-ldap
python -m venv .venv && source .venv/bin/activate      # or your pyenv setup
pip install -r requirements-mcp.txt

python mcp_server.py
```

On first start it creates `.mcp_token` (git-ignored, mode 600) and prints:

```
ldap-duckdb MCP server
  endpoint : http://192.168.1.23:8765/mcp
  allowed  : 192.168.1.0/24, 127.0.0.0/8
```

Note the endpoint and get the token:

```bash
python mcp_server.py --show-token
```

**macOS firewall:** the first time, macOS may ask "Allow python to accept incoming network connections?" Click **Allow**. If the firewall is set to "Block all incoming connections", the client can't reach it.

Useful options:

```bash
python mcp_server.py --port 9000
python mcp_server.py --allow 192.168.1.0/24     # set the allowed subnet explicitly (repeatable)
python mcp_server.py --memory-limit 4GB --timeout 120
python mcp_server.py --rotate-token             # new token; old one stops working
```

**Ingest scripts and the server together:** DuckDB allows one writer and no readers
while it writes. The server opens the database only for the length of each call, so you
can run the `ingest_*.py` scripts while it's up. Queries sent while an ingest is writing
return a "database is locked" message; try again once the ingest finishes.

**Tip:** give the server laptop a fixed IP (DHCP reservation in your router) so the client config doesn't break when the IP changes.

## 2. Client laptop (Claude Desktop)

Claude Desktop's "Add custom connector" connects through Anthropic's cloud, which can't
see your LAN, so we use the local `mcp-remote` bridge instead. It needs
[Node.js](https://nodejs.org) 18+ on the client laptop.

Check the connection first (replace IP and token):

```bash
curl -i -X POST http://192.168.1.23:8765/mcp -H "Authorization: Bearer <TOKEN>"
```

Getting any HTTP response (e.g. `406` or `400`) means the network path and token work.
`401` means a wrong token, `403` means the client's IP isn't allowed, and a timeout
means the firewall or the IP is wrong.

Open Claude Desktop → **Settings → Developer → Edit Config** and add:

```json
{
  "mcpServers": {
    "ldap-duckdb": {
      "command": "npx",
      "args": [
        "-y", "mcp-remote@0.14.3",
        "http://192.168.1.23:8765/mcp",
        "--allow-http",
        "--transport", "http-only",
        "--header", "Authorization:${AUTH_HEADER}"
      ],
      "env": {
        "AUTH_HEADER": "Bearer <TOKEN>"
      }
    }
  }
}
```

(`--allow-http` is needed because `mcp-remote` only allows plain HTTP for localhost by
default. Keep `Authorization:${AUTH_HEADER}` without a space; the space-in-args bug on
Windows is why the value goes in `env`.)

Fully quit and reopen Claude Desktop. `ldap-duckdb` should appear under the tools
(🔨/connectors) menu. Try: *"List the tables in the ldap database and count rows per scan date in goscanner_ldap."*

## Troubleshooting

| Symptom | Fix |
|---|---|
| Server exits "Could not detect a LAN IP" | Connect to Wi-Fi, or pass `--host <ip>` |
| Client times out | Same Wi-Fi? Server IP changed? macOS firewall blocking python? Some guest/"AP isolation" networks block laptop-to-laptop traffic |
| `403 Forbidden: only local-network clients` | Client is outside the default `/24`; add `--allow <cidr>` |
| `403 Invalid Host header` | You connected by a hostname the server doesn't know; use the IP it prints |
| `401` | Token mismatch; re-copy from `--show-token` |
| "database is locked" | An ingest script is writing; wait for it to finish |

---

## Running with Docker Compose

Files: `Dockerfile` (Python environment only), `docker-compose.yml`, `.env.example`, `.dockerignore`.
The service's `command:` in `docker-compose.yml` runs each `ingest_*.py` one after another
(`ingest_goscanner_tcp_hosts.py` first), then `exec`s `mcp_server.py`.

```bash
cp .env.example .env          # set LAN_IP (ipconfig getifaddr en0), HOST_UID/HOST_GID (id -u / id -g)
docker compose up -d --build
docker compose logs -f        # ingest progress + server banner
cat secrets/.mcp_token        # token (dir set by TOKEN_DIR)
```

- `./research_data` is mounted at `/app/research_data` (inputs + `ldap.duckdb`).
- Token goes to `TOKEN_DIR` (default `./secrets`) via `LDAP_MCP_TOKEN_FILE=/secrets/.mcp_token`.
- A failing ingest is logged and skipped (`;` between commands); use `&&` to stop on the first failure.
- To start without ingesting: `docker compose run -d --service-ports ldap-mcp python mcp_server.py --allow 172.28.0.0/24`.
- Port is published only on `LAN_IP`. LAN clients arrive via Docker NAT with the network gateway's IP, so `ALLOW_CIDR` is the Docker subnet (`172.28.0.0/24`); the bearer token is the real gate.
- `LDAP_MCP_PUBLIC_HOST=${LAN_IP}` adds the LAN IP to the allowed Host headers (else 421).
- A `403 ... only local-network clients` in the logs shows the source IP Docker used; set `ALLOW_CIDR` to cover it.

Server changes: `LDAP_MCP_TOKEN_FILE` env var, and `--public-host` / `LDAP_MCP_PUBLIC_HOST`.

# Testing

> _Human generated text._

You can `curl` using the same laptop the MCP server is running.
For example, set up a few variables:

```shell
URL=http://192.168.1.23:8765/mcp
TOKEN="...bzM" # generated at .mcp_token once the server starts up
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream")
```

and do a few queries:

```shell
# list the tools
curl -s "${H[@]}" $URL -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | jq '.'

# run a query
curl -s "${H[@]}" $URL -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query","arguments":{"sql":"select port, scan_date, count(*) from goscanner_ldap group by all"}}}' | jq '.'

# list tables
curl -s "${H[@]}" $URL -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_tables","arguments":{}}}' | jq '.'
```
