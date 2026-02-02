# Text-to-SQL Demo Technical Documentation

This document summarizes the current implementation, architecture, and operating procedures for the Text-to-SQL demo stack (RAG + Oracle + UI). It is written to be self-contained and actionable for development, deployment, and QA.

---

## 1) System Overview

The system converts natural-language questions into Oracle SQL using:
- RAG context (schema, examples, templates, glossary)
- PolicyGate safety checks
- Budget tracking and gating
- Demo cache for low-cost, instant responses
- Advanced mode with LLM generation and review flow

High-level flow:
1) User asks a question in UI (`/ask`)
2) Demo mode returns cached answer if available
3) Otherwise, Advanced mode:
   - RAG context -> LLM generates SQL
   - Review screen for user acknowledgment
   - Execution via Oracle with row cap and timeout

---

## 2) Repository Layout

Root: `text-to-sql/`

- `backend/`
  - FastAPI API, RAG, Oracle services, policy, budget, logging
- `ui/`
  - Next.js UI (Ask/Review/Admin)
- `scripts/`
  - Asset validation, demo cache, evaluation scripts
- `deploy/`
  - Docker Compose and Dockerfiles
- `var/`
  - Runtime data (metadata, chroma, cache, logs) (gitignored)
- `oracle/`
  - Instant Client (required for Thick mode)

---

## 3) Core Services

### 3.1 Config
File: `backend/app/core/config.py`
- Loads `.env`
- Central config for models, budgets, Oracle, RAG
- Important env keys:
  - `DEMO_MODE`, `BUDGET_LIMIT_KRW`, `COST_ALERT_THRESHOLD_KRW`
  - `ENGINEER_MODEL`, `EXPERT_MODEL`, `INTENT_MODEL`
  - `CONTEXT_TOKEN_BUDGET`, `EXAMPLES_PER_QUERY`, `TEMPLATES_PER_QUERY`
  - `MAX_RETRY_ATTEMPTS`, `EXPERT_SCORE_THRESHOLD`
  - `ROW_CAP`, `DB_TIMEOUT_SEC`, `MAX_DB_JOINS`
  - `ORACLE_DSN`, `ORACLE_USER`, `ORACLE_PASSWORD`
  - `ORACLE_DEFAULT_SCHEMA`
  - `ORACLE_LIB_DIR`, `ORACLE_TNS_ADMIN`
  - `BUDGET_CONFIG_PATH`

### 3.2 Oracle Layer
Files:
- `backend/app/services/oracle/connection.py`
- `backend/app/services/oracle/executor.py`
- `backend/app/services/oracle/metadata_extractor.py`

Key points:
- Pool initialization with timeouts
- Supports Thick mode via `ORACLE_LIB_DIR`
- Metadata extraction from `ALL_*` views
- Execution enforces:
  - `ROW_CAP` (ROWNUM)
  - `DB_TIMEOUT_SEC`
  - SELECT-only policy
  - `ORACLE_DEFAULT_SCHEMA` via `ALTER SESSION SET CURRENT_SCHEMA = ...`

### 3.3 RAG Pipeline
Files:
- `backend/app/services/rag/chroma_store.py`
- `backend/app/services/rag/indexer.py`
- `backend/app/services/rag/retrieval.py`
- `backend/app/services/runtime/context_budget.py`

Key points:
- Indexes schema, glossary, SQL examples, join templates
- Uses Chroma if installed, otherwise SimpleStore fallback
- Top-K retrieval + token budget trimming

### 3.4 Agents
Files:
- `backend/app/services/agents/sql_engineer.py`
- `backend/app/services/agents/sql_expert.py`
- `backend/app/services/agents/orchestrator.py`

Key points:
- Engineer generates SQL from RAG context
- Expert optionally refines if risk score exceeds threshold
- Demo cache path: `var/cache/demo_cache.json`
- PolicyGate precheck is enforced in Advanced mode

### 3.5 Policy & Budget
Files:
- `backend/app/services/policy/gate.py`
- `backend/app/services/cost_tracker.py`
- `backend/app/services/budget_gate.py`

PolicyGate blocks:
- Non-SELECT queries
- Missing WHERE clause
- Too many JOINs
- Write operations

Budget:
- Budget status stored in `var/logs/cost_state.json`
- Runtime overrides stored in `var/logs/budget_config.json`

---

## 4) API Endpoints

### Metadata & RAG
- `POST /admin/metadata/sync`
  - Body: `{ "owner": "SSO" }`
  - Output: schema_catalog + join_graph
- `POST /admin/rag/reindex`
- `GET /admin/rag/status`

### Query Flow
- `POST /query/oneshot`
  - Body: `{ "question": "..." }`
  - Returns `qid` and payload
- `GET /query/get?qid=...`
  - Returns stored payload
- `POST /query/run`
  - Body: `{ "qid": "...", "user_ack": true }`
  - Executes SQL (PolicyGate enforced)

### Budget & System
- `GET /admin/budget/status`
- `POST /admin/budget/config`
- `GET /admin/oracle/pool/status`

### Report
- `POST /report/evidence`

---

## 5) UI Flows

### Ask Page (`/ask`)
- Demo questions list loaded from `/query/demo/questions`
- Demo results display cached preview tables
- Advanced results show a "Review & Run" link
- Budget banner shows usage and threshold

### Review Page (`/review/{qid}`)
- SQL displayed for review
- User must acknowledge policy (`user_ack`)
- Runs SQL and shows result table
- PolicyGate error messages mapped to human-friendly explanations

### Admin Page (`/admin`)
- Budget settings (limit + alert)
- Budget status (raw)
- RAG status
- Oracle pool status

---

## 6) Data Assets

Generated/managed under `var/metadata`:
- `schema_catalog.json`
- `join_graph.json`
- `sql_examples.jsonl` (50+ recommended)
- `join_templates.jsonl` (5+ recommended)
- `glossary_docs.jsonl`
- `demo_questions.jsonl`

Cache:
- `var/cache/demo_cache.json`

Logs:
- `var/logs/events.jsonl`
- `var/logs/cost_state.json`
- `var/logs/budget_config.json`

---

## 7) Deployment (Docker Compose)

File: `deploy/compose/docker-compose.yml`

Ports:
- API: `8001` -> container `8000`
- UI: `3000`

Instant Client:
- Mount `oracle/instantclient_23_26` -> `/opt/oracle/instantclient`
- `ORACLE_LIB_DIR=/opt/oracle/instantclient`
- `LD_LIBRARY_PATH=/opt/oracle/instantclient`

Run:
```
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

---

## 8) Local Development

Backend:
```
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
export PYTHONPATH=$PWD/backend
uvicorn app.main:app --reload --port 8000
```

UI:
```
cd ui
npm install
API_BASE_URL=http://localhost:8001 npm run dev
```

---

## 9) Demo Cache Generation

```
export PYTHONPATH=$PWD/backend
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26
python scripts/pregen_demo_cache.py
```

Output:
- `var/cache/demo_cache.json`

---

## 10) Evaluation / Accuracy

Scripts:
- `scripts/eval_questions.py` (question -> SQL -> result compare)
- `scripts/eval_report_summary.py` (summary + CSV)

Example:
```
export PYTHONPATH=$PWD/backend
export DEMO_MODE=false
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26

python scripts/eval_questions.py \
  --input var/metadata/sql_examples.jsonl \
  --output var/logs/eval_report.jsonl \
  --ignore-order \
  --max 50

python scripts/eval_report_summary.py \
  --input var/logs/eval_report.jsonl \
  --csv var/logs/eval_report.csv
```

---

## 11) Troubleshooting

### DPY-4011
- Indicates Thin mode is blocked (NNE enabled)
- Use Thick mode with Instant Client

### DPI-1047 / libaio.so.1
- Missing runtime libs on host
- Install `libaio1` or link `libaio.so.1t64` to `libaio.so.1`

### ORA-00942
- Wrong schema/owner or missing permissions
- Check `ORACLE_DEFAULT_SCHEMA` and privileges

### PolicyGate Errors
- WHERE required
- Too many JOINs
- SELECT only
- Write operations blocked

---

## 12) Security & Safety

- `.env` is gitignored
- PolicyGate blocks risky SQL
- Row caps + timeouts enforced
- Budget gate stops requests when limit exceeded

---

## 13) Next Steps (Optional)

- Add CI pipeline for eval scripts
- Expand glossary and example coverage
- Add advanced UI filters and chart summaries

