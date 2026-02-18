# Text-to-SQL 데모 (RAG + MongoDB + Oracle)

자연어 질문을 안전한 Oracle SQL로 변환하는 데모 스택입니다. RAG 컨텍스트, 정책 게이팅, 예산 추적을 포함하며, Demo/Advanced 흐름을 제공하는 간단한 UI가 있습니다.

## 프로젝트 구조

- `backend/` FastAPI API (RAG, Oracle, 정책, 예산)
- `../ui/` Next.js UI (repo root, 현재 연결 대상)
- `ui/` 레거시 UI (text-to-sql 내부)
- `scripts/` 검증 + 데모 캐시 생성
- `deploy/` Docker Compose + Dockerfiles
- `var/` 런타임 데이터 (metadata, rag, cache, logs, mongo) (git에서 제외)

## 빠른 시작 (Docker Compose)

1) `.env` 생성 (`.env.example` 복사) 후 Oracle 자격증명 설정:

```
ORACLE_DSN=host:1521/service_name
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DEFAULT_SCHEMA=SSO
MONGO_URI=mongodb://localhost:27017
MONGO_DB=text_to_sql
OPENAI_API_KEY=...  # Advanced 모드에서만 필요
```

2) Oracle DB가 Thick 모드(NNE/TCPS)를 요구한다면 **Linux x64 Instant Client**를 내려받아 아래 경로에 압축 해제:

```
oracle/instantclient_23_26/
```

`libclntsh.so`가 존재하는지 확인하고(필요 시 심볼릭 링크 생성):

```
ln -s libclntsh.so.23.1 libclntsh.so
```

3) 실행:

```
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

서비스:
- API: `http://localhost:8001`
- UI: `http://localhost:3000`

## 초기 데이터 설정

1) Oracle 메타데이터(소유자/스키마) 동기화:

```
curl -X POST http://localhost:8001/admin/metadata/sync \
  -H "Content-Type: application/json" \
  -d '{"owner":"SSO"}'
```

2) RAG 재색인:

```
curl -X POST http://localhost:8001/admin/rag/reindex
curl http://localhost:8001/admin/rag/status
```

## 데모 캐시 (선택이지만 권장)

로컬 실행 (Python + 의존성 필요):

```
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
export PYTHONPATH=$PWD/backend
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26
python scripts/pregen_demo_cache.py
```

이 과정에서 `var/cache/demo_cache.json`이 생성되며 Demo 모드에서 사용됩니다.

## UI 흐름 (레거시 UI 기준)

- `/ask` Demo 버튼은 캐시된 답변을 사용합니다.
- Advanced 질문은 사용자 확인을 위해 `/review/{qid}`로 이동합니다.
- Review 페이지는 SQL diff 및 변경 이력을 보여주고, `user_ack=true`로 실행합니다.

## 예산 설정

예산은 `var/logs/cost_state.json`에서 추적됩니다.
Ask 페이지 UI에서 임계값을 변경하면 아래 파일에 저장됩니다:

```
var/logs/budget_config.json
```

API 엔드포인트:
- `GET /admin/budget/status`
- `POST /admin/budget/config`

## 트러블슈팅

- `DPY-4011` 연결 오류: Instant Client로 Thick 모드 활성화
- `DPI-1047 libaio.so.1 missing`: 호스트에 `libaio` 설치 또는 Docker 사용
- `ORA-00942 table or view does not exist`: 스키마/소유자 및 `ORACLE_DEFAULT_SCHEMA` 확인
## 2026-02-18 선택 반영 상세 (핵심 코드만 반영)

아래 내용은 `origin/feature` 대비 이번 선택 반영(`feature-clean`)에서 실제로 들어간 코드 변경입니다.

- 반영 범위: `text-to-sql` 핵심 코드 18개 파일
- 제외 범위: 로그/캐시/메타데이터 산출물(`var/logs`, 대용량 실험 산출물)
- 코드 diff 규모: `+593 / -119`

### 1) API/오케스트레이션/프롬프트 계층

`backend/app/api/routes/query.py`
- 변경 전: SQL 실패 시 원문 에러 문자열만 사용해 복구 LLM을 호출.
- 변경 후:
  - `parse_sql_error` 도입(`from app.services.agents.sql_error_parser import parse_sql_error`).
  - `_repair_sql_once(...)` 시그니처에 `structured_error` 추가.
  - no-row 케이스에 구조화 에러(`NO_ROWS_RETURNED`, `hint`)를 명시 전달.
  - 예외 처리 시 `structured_error = parse_sql_error(error_message, sql=current_sql)` 생성 후 복구 호출에 전달.
  - 복구 이력(`repair_history`)에 `error_detail` 저장.

`backend/app/services/agents/orchestrator.py`
- 변경 전: planner 사용 여부를 complexity gate(조건 미충족 시 planner 미사용)로 결정.
- 변경 후:
  - `_normalize_question` 정규식 버그 수정:
    - `r"[^a-z0-9\\s]"` -> `r"[^a-z0-9\s]"`
    - `r"[\\uac00-\\ud7a3]"` -> `r"[가-힣]"`
    - `r"\\s+"` -> `r"\s+"`
  - `_decide_planner_usage(...)`에서 `complex_only` 모드라도 planner를 기본 사용(`enabled=True`)하도록 변경.
  - 이유 값은 `intent_normalization_default` 또는 수집된 complexity 이유를 기록.

`backend/app/services/agents/prompts.py`
- `PLANNER_SYSTEM_PROMPT`:
  - 변경 전: 누락 필드를 안전 기본값으로 채우라는 지시만 존재.
  - 변경 후: 모호 표현(`recent/latest`)을 구체 시간 가정으로 정규화하고 assumptions에 남기도록 강화.
- `ENGINEER_SYSTEM_PROMPT`:
  - 변경 전: 분포형 질문(count by ...)은 기본 top 10 제한.
  - 변경 후: 기본은 전체 그룹 결과 반환, 사용자가 top/sample/preview를 명시할 때만 제한.
- `ERROR_REPAIR_SYSTEM_PROMPT`:
  - 변경 전: 입력은 `question/context/failed_sql/error_message`.
  - 변경 후: `error_detail`(구조화 에러)가 있으면 최우선 신호로 사용하도록 명시.

### 2) LLM JSON 파싱/호출 안정화

`backend/app/services/agents/llm_client.py`
- 변경 전: `chat(messages, model, max_tokens)` 고정 호출.
- 변경 후:
  - `chat(..., expect_json: bool = False)` 추가.
  - `expect_json=True`일 때 `response_format={"type":"json_object"}` 전달.
  - 일부 SDK/provider 미지원 대비 `TypeError` 발생 시 `response_format` 제거 후 재시도.

`backend/app/services/agents/json_utils.py` (신규)
- 신규 유틸 `extract_json_object(text)` 추가.
- 처리 순서:
  - 문자열 전체 `json.loads` 시도
  - fenced json(````json ... ````) 추출 후 파싱
  - balanced `{...}` 후보를 순회하며 첫 유효 JSON object 파싱
  - 실패 시 `ValueError("LLM response is not valid JSON")`

`backend/app/services/agents/clarifier.py`
- 변경 전: 파일 내부 `_extract_json`(정규식 1회 추출) 사용.
- 변경 후:
  - 내부 `_extract_json` 제거.
  - `extract_json_object` 공통 유틸 사용.
  - LLM 호출 시 `expect_json=True`로 JSON 응답 강제.

`backend/app/services/agents/sql_planner.py`
- 변경 전: planner 내부 `_extract_json` 사용.
- 변경 후: `extract_json_object` + `expect_json=True` 적용.

`backend/app/services/agents/sql_engineer.py`
- 변경 전: engineer 내부 `_extract_json` 사용.
- 변경 후: `extract_json_object` + `expect_json=True` 적용.

`backend/app/services/agents/sql_expert.py`
- 변경 전: expert/repair 모두 내부 `_extract_json` 사용.
- 변경 후:
  - `extract_json_object` + `expect_json=True`로 통일.
  - `repair_sql_after_error(...)` 시그니처에 `structured_error` 추가.
  - `structured_error`가 있으면 payload에 `error_detail`로 포함하여 LLM 복구 입력 강화.

### 3) SQL 실패 복구 강화

`backend/app/services/agents/sql_error_parser.py` (신규)
- 신규 함수 `parse_sql_error(error_message, sql="")` 추가.
- 파싱 범위:
  - 공통 에러코드: `ORA-xxxxx`, `DPY-xxxx`, `DPI-xxxx`
  - `ORA-00904`: invalid identifier/alias 파싱(`invalid_identifier`, `owner_or_alias`, `hint`)
  - `ORA-00979`: SQL에서 top-level `SELECT`/`GROUP BY` 항목 추출 후 힌트 생성
  - `ORA-00933`: 문법 종료 오류 힌트
  - `ORA-00942`, `DPY-4024`, `DPI-1067`, `ORA-03156`, `ORA-01031`, `ORA-01722` 등 기본 힌트

`backend/app/services/agents/sql_error_templates.py`
- `_ERR_IDENT_RE` 확장:
  - 변경 전: `"ALIAS"."COLUMN"` 형태만 매칭.
  - 변경 후: `"COLUMN"` 단독 형태도 매칭.
- `_repair_invalid_identifier(...)` 규칙 확장:
  - `PROCEDURE_COUNT`/`DIAGNOSIS_COUNT`/`AVERAGE_VALUE` -> `CNT` 매핑 규칙 추가.
  - `CNT` 참조 실패 시 inner projection의 명명 alias(`*_COUNT`, `EVENT_COUNT`, `RX_ORDER_CNT`)로 역매핑.
  - alias scope mismatch(`dx.icd_code` 등) 발생 시 alias prefix 제거 fallback 추가.
- `_repair_timeout(...)` 샘플링 cap 변경:
  - 변경 전: `2000~10000` 범위.
  - 변경 후: `500~1000` 범위(타임아웃 시 더 공격적으로 스캔 축소).

`backend/app/services/agents/sql_postprocess.py`
- `_rewrite_avg_count_alias(...)` 개선:
  - 변경 전: `AVG(..._COUNT)`를 일괄 `AVG(CNT)`로 치환.
  - 변경 후:
    - 실제 projection alias 집합을 먼저 추출.
    - outer aggregate alias가 projection에 없을 때, 단일 count-like alias로 안전 매핑.
    - `CNT`가 projection에 있을 때만 `AVG(..._COUNT)->AVG(CNT)` 치환 수행.
- `_strip_unrequested_top_n_cap(question, sql)` 신규:
  - 사용자가 top/sample/preview를 요청하지 않았는데 `ROWNUM <= N (N<=200)`이 들어간 경우 제거.
  - group/order 집계형 결과가 무의미하게 잘리지 않도록 방지.
- 적용 경로:
  - `_postprocess_sql_relaxed(...)`와 `postprocess_sql(...)`에서
    `enforce_top_n_wrapper` 전에 `strip_unrequested_top_n_cap`를 선행 적용.

### 4) Oracle 연결/실행 안정화

`backend/app/services/oracle/connection.py`
- `_has_client_lib`, `_candidate_client_dirs` 신규:
  - Instant Client 자동 탐색 경로 추가:
    - `ORACLE_LIB_DIR`
    - `oracle/instantclient_23_26`
    - `query-visualization/oracle/instantclient_23_26`
    - `text-to-sql/oracle/instantclient_23_26`
- `_init_oracle_client()` 동작 변경:
  - 변경 전: `ORACLE_LIB_DIR` 없으면 즉시 thin mode로 간주.
  - 변경 후: 후보 경로 자동 탐색 후 라이브러리 존재 시 Thick 초기화 시도.
  - 자동탐색 실패/초기화 실패(명시 경로 아님) 시 hard fail 대신 thin mode fallback.
  - `network/admin` 존재 시 `config_dir` 자동 세팅.
- `get_pool()`:
  - runtime override의 `sslMode` 반영.
  - `sslMode in {require, verify-ca, verify-full}`이면 DSN을 `tcps://host:port/service`로 생성.
- `acquire_connection()`:
  - `DPY-4011`, `DPY-6005`, `DPI-1080`, connection reset/closed 계열 오류를 recoverable로 판정.
  - recoverable 오류 시 `reset_pool()` 후 1회 재시도.

`backend/app/services/oracle/executor.py`
- 변경 전: 스키마는 `settings.oracle_default_schema`만 사용.
- 변경 후:
  - `load_connection_settings()` 도입.
  - `defaultSchema` runtime override가 있으면 해당 값을 우선 사용해 `ALTER SESSION SET CURRENT_SCHEMA`.

### 5) 정책 게이트 완화/정합성

`backend/app/services/policy/gate.py`
- `_DISTINCT_SELECT_RE`, `_WHERE_OPTIONAL_SAMPLE_HINTS` 신규.
- `_can_skip_where(question, sql)` 변경:
  - 변경 전: trend/aggregate 힌트 중심으로만 WHERE 생략 허용.
  - 변경 후: `SELECT DISTINCT` + 샘플/미리보기/고유값(list distinct) 계열 질문도 WHERE 생략 허용.

### 6) RAG 문서화 품질 개선

`backend/app/services/rag/indexer.py`
- `_schema_docs(schema_catalog, join_graph=None)`로 시그니처 확장.
- 변경 전: 테이블 문서에 `컬럼명:타입`, PK만 기록.
- 변경 후:
  - 컬럼 표현을 `name:type:nullability`로 확장.
  - `join_graph.json`의 edges를 읽어 FK(`SRC_COL->DST_TABLE.DST_COL`)를 테이블 문서에 포함.
- `reindex(...)`에서 `join_graph.json` 로딩 후 schema docs 생성/카운트에 반영.

`backend/app/services/rag/retrieval.py`
- 변경 전: 로컬 schema 캐시에 `컬럼명:타입`, PK만 반영.
- 변경 후:
  - `join_graph.json` 로딩 및 FK 인덱스 구성.
  - schema 문서 텍스트를 `name:type:nullability`, PK, FK까지 포함하도록 확장.

### 7) 평가 스크립트 운영 모드

`scripts/eval_questions.py`
- CLI 인자 추가: `--respect-budget-gate`
- `service_pipeline` 평가 시 동작 변경:
  - 변경 전: 런타임 budget gate 상태에 따라 평가가 조기 중단될 수 있음.
  - 변경 후: 기본값으로 이 스크립트 프로세스에서만 budget gate를 비활성화(`ensure_budget_ok = lambda: None`).
  - 필요 시 `--respect-budget-gate` 옵션으로 기존 게이트를 유지 가능.

### 이번 선택 반영 요약

- 목적: `origin/feature` 기반 안정성을 유지하면서, 실제 성능/정확도 개선에 기여하는 핵심 코드만 선별 반영.
- 핵심 축:
  - 구조화 에러 기반 SQL 복구 정확도 개선
  - JSON 응답 파싱 안정화
  - Oracle 연결 복원력/환경 호환성 개선
  - RAG schema 문서 품질 개선(FK/nullability 포함)
  - 평가 시 budget gate로 인한 조기 종료 방지
