# Python & RAG 파일 상세 안내서 (초보자용)

이 문서는 **text-to-sql 프로젝트의 Python 파일과 RAG 관련 파일만** 초보자도 이해하기 쉽게 설명합니다.
코드가 실제로 어떤 흐름으로 실행되는지, 각 파일이 맡은 역할이 무엇인지 중심으로 정리했습니다.

---

## 1) 전체 흐름 한눈에 보기 (요약) — 추가 설명

```
사용자 질문
  -> (RAG) 관련 스키마/예시/템플릿/용어 검색
  -> LLM이 SQL 초안 생성 (SQL Engineer)
  -> 위험도 높으면 LLM이 재검토 (SQL Expert)
  -> SQL 후처리 규칙으로 Oracle 문법/스키마 보정
  -> 정책 검사(SELECT만, WHERE 필수, JOIN 제한)
  -> Oracle 실행 및 결과 반환
  -> 비용/이벤트 로그 기록
```

추가 설명 (초보자 관점)
- **RAG 검색**은 “관련 참고자료를 찾아서 LLM에게 같이 보여주는 단계”입니다. 사람이 문제 풀 때 참고자료 찾아보는 것과 같습니다.
- **SQL Engineer**는 “처음 답안을 만드는 사람”, **SQL Expert**는 “검토/수정하는 사람” 역할입니다.
- **후처리 규칙**은 “LLM이 실수했을 때 자동으로 고치는 룰북”입니다. 예: LIMIT → ROWNUM.
- **정책 검사**는 보안/안전 장치입니다. 위험한 SQL(삭제/수정)이나 너무 큰 쿼리를 막습니다.
- **실행 결과**는 Oracle에서 실제로 조회한 결과이며, 비용/로그는 운영 추적용입니다.

---

## 2) Python 파일 상세

### 2.1 `scripts/` (운영/평가/검증용 스크립트)

- `text-to-sql/scripts/eval_questions.py`
  - **역할**: 질문-정답 SQL(jsonl)을 기준으로 **Text-to-SQL 정확도 평가**를 수행합니다.
  - **입력**: `var/metadata/sql_examples.jsonl` (기본)
  - **출력**: `var/logs/eval_report.jsonl` (기본)
  - **핵심 옵션**:
    - `--max`: 일부만 평가
    - `--ignore-order`: 결과 행 순서 무시
    - `--skip-policy`: Policy Gate 무시하고 생성
    - `--require-advanced`: 데모 캐시 사용 금지(LLM 생성 강제)
  - **동작**: 예상 SQL/생성 SQL을 모두 실행해 결과 비교 → match/mismatch/exec_error 기록

- `text-to-sql/scripts/eval_report_summary.py`
  - **역할**: `eval_report.jsonl` 요약 통계 출력 + CSV 변환
  - **출력**: 요약 JSON, 옵션 `--csv`로 CSV 생성

- `text-to-sql/scripts/validate_examples.py`
  - **역할**: `sql_examples.jsonl`의 SQL을 **실제 Oracle에서 실행**해 유효성 검증
  - **출력**: 실패 시 ORA-코드 또는 에러 메시지 출력

- `text-to-sql/scripts/validate_assets.py`
  - **역할**: RAG 자산 최소 개수 검증
  - **검사 내용**: 예시/템플릿/용어집 개수 및 필수 용어(예: LOS) 포함 여부

- `text-to-sql/scripts/validate_index.py`
  - **역할**: RAG 인덱스 대상 파일들의 개수 상태 점검
  - **출력**: schema/docs/example/template/glossary 개수

- `text-to-sql/scripts/pregen_demo_cache.py`
  - **역할**: 데모 모드에서 사용할 **질문→SQL/결과 미리 생성**
  - **입력**: `demo_questions.jsonl` + `sql_examples.jsonl`
  - **출력**: `var/cache/demo_cache.json`
  - **특징**: LLM 생성 실패 시 예제 SQL을 fallback

- `text-to-sql/scripts/test_oracle_connection.py`
  - **역할**: Oracle 접속/환경변수 점검 및 `SELECT 1 FROM dual` 테스트
  - **출력**: 접속 정보 스냅샷 + 성공/실패 로그

---

### 2.2 `backend/app/main.py` (FastAPI 진입점)

- `text-to-sql/backend/app/main.py`
  - **역할**: FastAPI 앱 생성 및 라우터 등록
  - **등록 라우터**:
    - `/query` (질문→SQL 생성 및 실행)
    - `/admin/metadata`, `/admin/rag` (메타데이터 및 RAG 관리)
    - `/admin/budget`, `/admin/oracle` (예산/DB 상태)
    - `/report` (보고서 생성)

---

### 2.3 `backend/app/core/config.py` (환경설정)

- `text-to-sql/backend/app/core/config.py`
  - **역할**: `.env` 로딩 + 설정값을 `Settings`로 제공
  - **RAG 관련 주요 설정**: `RAG_PERSIST_DIR`, `RAG_TOP_K`, `RAG_EMBEDDING_DIM`, `CONTEXT_TOKEN_BUDGET`
  - **MongoDB 관련 설정**: `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION`, `MONGO_VECTOR_INDEX`
  - **Oracle 관련 설정**: `ORACLE_DSN`, `ORACLE_USER`, `ORACLE_PASSWORD`, `ROW_CAP`, `DB_TIMEOUT_SEC`

---

### 2.4 API 라우터 (`backend/app/api/routes/`)

- `text-to-sql/backend/app/api/routes/query.py`
  - **역할**: 질문 처리 및 SQL 실행의 **메인 API**
  - **핵심 엔드포인트**:
    - `POST /query/oneshot`: 질문→SQL 생성
    - `POST /query/run`: SQL 실행 (사용자 확인 필요)
    - `GET /query/demo/questions`: 데모 질문 제공

- `text-to-sql/backend/app/api/routes/admin_metadata.py`
  - **역할**: Oracle 메타데이터 추출 + RAG 재색인
  - **핵심 엔드포인트**:
    - `/admin/metadata/sync`: 스키마 추출
    - `/admin/rag/reindex`: RAG 인덱스 재생성
    - `/admin/rag/status`: 인덱스 상태 요약

- `text-to-sql/backend/app/api/routes/admin_budget.py`
  - **역할**: 예산/비용 상태 조회 및 설정 변경

- `text-to-sql/backend/app/api/routes/admin_oracle.py`
  - **역할**: Oracle 커넥션 풀 상태 조회

- `text-to-sql/backend/app/api/routes/report.py`
  - **역할**: 간단한 JSON 내용을 PDF 리포트로 변환
  - **주의**: `reportlab` 라이브러리 필요

---

### 2.5 Agents (LLM 관련 로직) — 추가 설명

- `text-to-sql/backend/app/services/agents/orchestrator.py`
  - **역할**: 전체 파이프라인 총괄
  - **흐름**:
    1) 데모 캐시 확인 → 있으면 바로 반환
    2) 위험도 평가
    3) RAG 컨텍스트 구성
    4) SQL 생성(Engineer) → 필요 시 Expert 리뷰
    5) SQL 후처리(postprocess)
    6) 정책 검사(precheck)
    7) 비용 기록
  - **초보자 팁**: 문제가 생기면 이 파일을 기준으로 어디에서 실패했는지 흐름을 따라가면 됩니다.

- `text-to-sql/backend/app/services/agents/sql_engineer.py`
  - **역할**: LLM에게 SQL 초안을 생성하게 함
  - **입력**: 질문 + RAG 컨텍스트
  - **출력**: JSON 형태 결과(`final_sql`, `warnings` 등)
  - **초보자 팁**: 여기서 응답이 JSON이 아니면 `_extract_json`이 실패합니다. LLM 프롬프트가 중요합니다.

- `text-to-sql/backend/app/services/agents/sql_expert.py`
  - **역할**: SQL 초안을 더 안전하게 보정
  - **사용 조건**: 위험도(score)가 기준 이상일 때
  - **초보자 팁**: 위험도 기준은 `EXPERT_SCORE_THRESHOLD` 설정으로 조정합니다.

- `text-to-sql/backend/app/services/agents/llm_client.py`
  - **역할**: OpenAI API 호출 래퍼
  - **특징**: `OpenAI` 라이브러리 기반, 기본 chat.completions 사용
  - **초보자 팁**: API 키가 없으면 여기서 실패합니다. `.env`를 확인하세요.

- `text-to-sql/backend/app/services/agents/prompts.py`
  - **역할**: LLM 시스템 프롬프트 정의
  - **특징**: Oracle 문법(LIMIT 금지, ROWNUM 사용 등) 강제
  - **초보자 팁**: 문법 오류가 많다면 프롬프트를 먼저 강화하는 것이 효과적입니다.

- `text-to-sql/backend/app/services/agents/sql_postprocess.py`
  - **역할**: LLM 결과 SQL을 **Oracle 문법/스키마에 맞게 자동 보정**
  - **주요 보정 유형**:
    - Oracle 문법 수정: `LIMIT/TOP/FETCH` → `ROWNUM`, `WHERE TRUE` 정리
    - 테이블/컬럼 매핑: 유사한 이름 → 실제 스키마 컬럼명으로 변경
    - 질문 힌트 기반 테이블 강제 (예: “미생물”→`MICROBIOLOGYEVENTS`)
    - 라벨/용어 조인 보강: `D_ITEMS`, `D_LABITEMS`, ICD 코드 테이블 조인
    - 집계/정렬/별칭 보정: COUNT/AVG alias 통일, ORDER BY 오류 수정
    - 시간/나이 계산 보정: `ANCHOR_AGE`, `ANCHOR_YEAR` 기준으로 변환
  - **초보자 팁**: “자주 틀리는 패턴”을 이 파일에 룰로 추가하는 방식이 가장 빠른 개선 루트입니다.

---

### 2.6 RAG 런타임 (컨텍스트 구성) — 추가 설명

- `text-to-sql/backend/app/services/runtime/context_builder.py`
  - **역할**: 질문에 맞는 RAG 후보 문서를 모아 컨텍스트로 변환
  - **초보자 팁**: 여기서는 검색 결과를 그대로 전달하므로, “검색 품질”이 성능에 큰 영향을 줍니다.

- `text-to-sql/backend/app/services/runtime/context_budget.py`
  - **역할**: 컨텍스트 토큰 예산 초과 시 **앞쪽부터 잘라냄**
  - **특징**: `tiktoken`이 없으면 단어 수로 간단 추정
  - **초보자 팁**: 예산이 너무 작으면 좋은 예시가 잘릴 수 있습니다.

- `text-to-sql/backend/app/services/runtime/risk_classifier.py`
  - **역할**: 질문 위험도/복잡도 간단 분류
  - **예시 기준**: write 키워드, JOIN 수, 길이 등
  - **초보자 팁**: 여기 규칙이 엄격하면 Expert가 자주 호출되어 비용이 늘 수 있습니다.

---

### 2.7 RAG 인덱스/검색 — 추가 설명

- `text-to-sql/backend/app/services/rag/indexer.py`
  - **역할**: metadata JSONL/JSON 파일을 읽어 문서화 후 벡터 저장
  - **입력**: `schema_catalog.json`, `glossary_docs.jsonl`, `sql_examples.jsonl`, `join_templates.jsonl`, `sql_templates.jsonl`
  - **출력**: MongoDB 벡터스토어에 저장
  - **초보자 팁**: 여기서 “문서가 어떻게 텍스트로 변환되는지”가 검색 품질에 큰 영향을 줍니다.

- `text-to-sql/backend/app/services/rag/retrieval.py`
  - **역할**: 질문에 대해 스키마/예시/템플릿/용어 문서를 **top-k**로 검색
  - **초보자 팁**: `RAG_TOP_K`, `EXAMPLES_PER_QUERY`, `TEMPLATES_PER_QUERY`가 직접적인 품질 파라미터입니다.

- `text-to-sql/backend/app/services/rag/mongo_store.py`
  - **역할**: 벡터스토어 래퍼
  - **특징**:
    - `MONGO_URI`가 있으면 MongoDB 저장소 사용
    - `MONGO_VECTOR_INDEX` 설정 시 `$vectorSearch` 사용
    - 설정이 없으면 `SimpleStore`(해시 기반 임베딩)로 대체
  - **초보자 팁**: SimpleStore는 가벼우나 검색 품질이 떨어질 수 있습니다.

---

### 2.8 Oracle 관련 서비스

- `text-to-sql/backend/app/services/oracle/connection.py`
  - **역할**: Oracle 클라이언트 초기화 및 커넥션 풀 관리
  - **환경변수 사용**: `ORACLE_LIB_DIR`, `ORACLE_TNS_ADMIN`

- `text-to-sql/backend/app/services/oracle/executor.py`
  - **역할**: SQL 실행 + 결과 반환
  - **특징**: `ROWNUM` 기반 row cap 강제, 기본 스키마 설정 가능

- `text-to-sql/backend/app/services/oracle/metadata_extractor.py`
  - **역할**: Oracle 스키마를 읽어 `schema_catalog.json`, `join_graph.json` 생성

---

### 2.9 비용/정책/로그

- `text-to-sql/backend/app/services/cost_tracker.py`
  - **역할**: LLM 호출 및 SQL 실행 비용 누적/저장
  - **저장 경로**: `var/logs/cost_state.json`, `var/logs/budget_config.json`

- `text-to-sql/backend/app/services/budget_gate.py`
  - **역할**: 예산 초과 시 API 호출 차단

- `text-to-sql/backend/app/services/policy/gate.py`
  - **역할**: SQL 안전성 검사 (SELECT만, WHERE 필수, JOIN 제한)

- `text-to-sql/backend/app/services/logging_store/store.py`
  - **역할**: JSONL 로그 저장/읽기
  - **주로 사용**: 비용 기록

---

### 2.10 기타 Python 파일

- `text-to-sql/backend/app/scripts/excel_to_glossary.py`
  - **역할**: Excel 용어집을 `glossary_docs.jsonl` 형식으로 변환
  - **주의**: `pandas` 필요

- `text-to-sql/backend/app/__init__.py`, `text-to-sql/backend/app/api/__init__.py` 등
  - **역할**: 파이썬 패키지 인식용 (내용 없음)

---

## 3) RAG 관련 파일 상세

### 3.1 핵심 RAG 입력 데이터 (`var/metadata/`) — 추가 설명

- `text-to-sql/var/metadata/schema_catalog.json`
  - **내용**: 스키마 전체 테이블/컬럼 구조
  - **생성**: `metadata_extractor.py`
  - **RAG에서 사용**: 테이블/컬럼 설명 문서 생성
  - **초보자 팁**: 실제 DB와 다르면 SQL이 계속 실패합니다. 가장 먼저 확인하세요.

- `text-to-sql/var/metadata/join_graph.json`
  - **내용**: 테이블 간 FK 조인 관계 그래프
  - **용도**: 조인 추론/참고용

- `text-to-sql/var/metadata/glossary_docs.jsonl`
  - **내용**: 용어 → 정의 (의료/도메인 용어)
  - **RAG에서 사용**: 용어 설명 문서
  - **초보자 팁**: “병원/약어/약물/검사” 같은 도메인 용어는 여기서 보강하면 효과가 큽니다.

- `text-to-sql/var/metadata/sql_examples.jsonl`
  - **내용**: 질문-정답 SQL 예시
  - **용도**: RAG 예시 제공 + 평가 데이터
  - **초보자 팁**: 예시 품질이 곧 모델 힌트 품질입니다. 정답 SQL이 정확한지 주기적으로 점검하세요.

- `text-to-sql/var/metadata/join_templates.jsonl`
  - **내용**: 자주 쓰는 JOIN SQL 템플릿
  - **초보자 팁**: 조인 오류가 잦다면 여기에 “정석 조인 패턴”을 넣는 것이 가장 빠릅니다.

- `text-to-sql/var/metadata/sql_templates.jsonl`
  - **내용**: 일반 SQL 템플릿(샘플/집계 등)
  - **초보자 팁**: 자주 쓰는 집계/샘플 패턴을 넣어 두면 LLM이 그 패턴을 복제합니다.

- `text-to-sql/var/metadata/demo_questions.jsonl`
  - **내용**: 데모 UI용 질문 목록
  - **사용처**: demo 질문 목록 + `pregen_demo_cache.py`
  - **초보자 팁**: 데모 질문이 실제 시나리오를 잘 대표하도록 관리해야 합니다.

- `text-to-sql/var/metadata/mimic_eval_*.jsonl`, `mimic_eval_questions.jsonl`
  - **내용**: 평가용 질문/SQL 데이터
  - **사용처**: `eval_questions.py` 평가 입력
  - **초보자 팁**: 평가 세트가 실제 서비스 질문과 다르면 수치가 왜곡될 수 있습니다.

---

### 3.2 RAG 인덱스 저장소 (MongoDB + `var/rag/`)

- `MongoDB` 컬렉션 (`MONGO_DB`, `MONGO_COLLECTION`)
  - **기본 저장소**. Atlas Vector Search를 쓰면 `MONGO_VECTOR_INDEX` 설정

- `text-to-sql/var/rag/simple_store.json`
  - **Mongo 설정이 없을 때** 사용하는 간단 임베딩 저장소

- `text-to-sql/var/mongo/`
  - Docker Compose로 띄운 MongoDB의 로컬 데이터 디렉터리

---

## 4) RAG 업데이트/재색인 흐름 — 추가 설명

1) Oracle 메타데이터 갱신
   - `/admin/metadata/sync` 또는 `metadata_extractor.py` 실행
2) `var/metadata` 내 문서 수정/추가
3) RAG 재색인
   - `/admin/rag/reindex` 또는 `rag/indexer.py` 사용

추가 설명 (운영 체크리스트)
- **스키마 변경**이 발생했으면 1) → 3) 순서로 반드시 재색인해야 합니다.
- **용어/예시 추가**만 했다면 2) → 3)만으로 충분합니다.
- 재색인 후에는 `validate_index.py`로 문서 개수 확인을 권장합니다.

---

## 5) 체크 포인트

- **SQL이 자꾸 실패한다면**: `sql_postprocess.py`의 매핑/보정 규칙 확인
- **RAG 결과가 부정확하다면**: `schema_catalog.json`, `glossary_docs.jsonl`, `sql_examples.jsonl` 품질 점검
- **평가가 필요하면**: `eval_questions.py` → `eval_report_summary.py` 순서 추천

---

이 문서는 Git에 넣지 않아도 되는 용도로 작성되었습니다. 필요하면 파일명만 바꿔서 개인 문서로 활용해 주세요.
