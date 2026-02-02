# Text-to-SQL Demo (RAG + Oracle)

This is a demo stack for converting natural language questions into safe Oracle SQL with RAG context, policy gating, and budget tracking. It includes a simple UI with Demo and Advanced flows.

## Project Layout

- `backend/` FastAPI API (RAG, Oracle, policy, budget)
- `ui/` Next.js UI
- `scripts/` validation + demo cache generation
- `deploy/` Docker compose + Dockerfiles
- `var/` runtime data (metadata, chroma, cache, logs) (ignored by git)

## Quickstart (Docker Compose)

1) Create `.env` (copy from `.env.example`) and set Oracle credentials:

```
ORACLE_DSN=host:1521/service_name
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DEFAULT_SCHEMA=SSO
OPENAI_API_KEY=...  # only needed for Advanced mode
```

2) If your Oracle DB requires Thick mode (NNE/TCPS), download **Linux x64 Instant Client** and unzip to:

```
oracle/instantclient_23_26/
```

Ensure `libclntsh.so` exists (create symlink if needed):

```
ln -s libclntsh.so.23.1 libclntsh.so
```

3) Run:

```
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

Services:
- API: `http://localhost:8001`
- UI: `http://localhost:3000`

## Initial Data Setup

1) Sync Oracle metadata (owner/schema):

```
curl -X POST http://localhost:8001/admin/metadata/sync \
  -H "Content-Type: application/json" \
  -d '{"owner":"SSO"}'
```

2) RAG reindex:

```
curl -X POST http://localhost:8001/admin/rag/reindex
curl http://localhost:8001/admin/rag/status
```

## Demo Cache (Optional but Recommended)

Local run (requires Python + deps):

```
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
export PYTHONPATH=$PWD/backend
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26
python scripts/pregen_demo_cache.py
```

This produces `var/cache/demo_cache.json` used by Demo mode.

## UI Flow

- `/ask` Demo buttons use cached answers.
- Advanced questions link to `/review/{qid}` for user acknowledgment.
- Review page shows SQL diff and change history, then executes with `user_ack=true`.

## Budget Settings

Budget is tracked in `var/logs/cost_state.json`.
You can update thresholds in the UI (Ask page) which are persisted to:

```
var/logs/budget_config.json
```

API endpoints:
- `GET /admin/budget/status`
- `POST /admin/budget/config`

## Troubleshooting

- `DPY-4011` when connecting: enable Thick mode with Instant Client.
- `DPI-1047 libaio.so.1 missing`: ensure `libaio` is installed on host, or run in Docker.
- `ORA-00942 table or view does not exist`: verify schema owner and `ORACLE_DEFAULT_SCHEMA`.

