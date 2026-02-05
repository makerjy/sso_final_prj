# Text-to-SQL 전체 구조 + 핵심 상세 문서 

## 1) 전체 구조 요약

- `(root)` 프로젝트 최상위 파일 (README, .gitignore 등)
- `backend/` API 서버(FastAPI), RAG, Oracle, 정책, 예산, 로깅
- `ui/` Next.js 기반 웹 UI
- `scripts/` 검증/평가/데모캐시 스크립트
- `deploy/` Docker Compose 및 Dockerfile
- `docs/` 문서 모음
- `var/` 런타임 데이터(메타데이터, 캐시, 로그 등)


## 2) 핵심 상세 설명

### 2.1 전체 처리 흐름 (한눈에 보기)

1) 사용자가 UI(`/ask`)에서 질문 입력
2) Demo 모드이면 캐시(`var/cache/demo_cache.json`)에서 즉시 응답
3) Advanced 모드이면 RAG 컨텍스트 생성
4) ENGINEER 모델이 SQL 초안 생성
5) 위험도가 높으면 EXPERT 모델이 재검토
6) 후처리 규칙으로 Oracle 문법/매핑 보정
7) PolicyGate가 안전성 검사 (SELECT-only, WHERE 필수 등)
8) Oracle 실행 후 결과 반환

---

### 2.2 Backend (API + RAG + Oracle) 상세

**핵심 설정**
- `backend/app/core/config.py`
  - `.env`를 읽어 모든 핵심 설정을 로딩합니다.
  - 모델/예산/DB/RAG 설정은 이 파일이 기준입니다.
  - 예: `ENGINEER_MODEL`, `EXPERT_MODEL`, `ROW_CAP`, `DB_TIMEOUT_SEC` 등

**API 진입점**
- `backend/app/main.py`
  - FastAPI 앱 생성 및 라우터 등록

**API 라우트**
- `backend/app/api/routes/query.py` : 질문 처리(oneshot/run 등)
- `backend/app/api/routes/admin_metadata.py` : 메타데이터 동기화
- `backend/app/api/routes/admin_budget.py` : 예산 상태/설정
- `backend/app/api/routes/admin_oracle.py` : Oracle 풀 상태
- `backend/app/api/routes/report.py` : 리포트/증빙 업로드

**RAG 파이프라인**
- `backend/app/services/rag/indexer.py`
  - `var/metadata/*` 문서를 색인에 적재
- `backend/app/services/rag/retrieval.py`
  - 질문에 관련된 스키마/예시/템플릿/용어집을 검색
- `backend/app/services/rag/mongo_store.py`
  - MongoDB 또는 SimpleStore 기반 검색 저장소
- `backend/app/services/runtime/context_budget.py`
  - 토큰 예산에 맞춰 컨텍스트를 잘라냄

**SQL 생성(LLM)과 오케스트레이션**
- `backend/app/services/agents/orchestrator.py`
  - Demo 캐시, RAG, 엔지니어/엑스퍼트 모델 호출을 총괄
- `backend/app/services/agents/sql_engineer.py`
  - ENGINEER 모델로 SQL 초안 생성
- `backend/app/services/agents/sql_expert.py`
  - 위험도가 높으면 EXPERT 모델로 재검토
- `backend/app/services/agents/llm_client.py`
  - OpenAI 호환 API 호출 래퍼
- `backend/app/services/agents/prompts.py`
  - 모델 프롬프트(Oracle 문법 등 지시사항)
- `backend/app/services/agents/sql_postprocess.py`
  - 생성 SQL을 Oracle 문법/스키마에 맞게 자동 보정

**Oracle 연결/실행**
- `backend/app/services/oracle/connection.py`
  - Oracle 연결 풀 관리
- `backend/app/services/oracle/executor.py`
  - SQL 실행, row cap/timeout 적용
- `backend/app/services/oracle/metadata_extractor.py`
  - 스키마 정보 추출 (ALL_* 뷰)

**정책/예산/로그**
- `backend/app/services/policy/gate.py`
  - SELECT-only, WHERE 필수, JOIN 제한 등
- `backend/app/services/budget_gate.py`
  - 예산 한도/알림 처리
- `backend/app/services/cost_tracker.py`
  - 비용 누적 및 로그 기록
- `backend/app/services/logging_store/store.py`
  - 이벤트 로그 저장

---

### 2.3 UI (Next.js) 상세

- `ui/app/ask/page.tsx`
  - 질문 입력/데모 실행 화면
- `ui/app/review/[qid]/page.tsx`
  - SQL 검토 + 실행 동의 화면
- `ui/app/results/[qid]/page.tsx`
  - 결과 표시 화면
- `ui/app/admin/page.tsx`
  - 예산/RAG 상태 등 관리 화면
- `ui/app/layout.tsx`, `ui/app/globals.css`
  - 공통 레이아웃/스타일

---

### 2.4 Scripts (검증/평가) 상세

- `scripts/validate_assets.py` : 메타데이터 유효성 검사
- `scripts/validate_index.py` : RAG 인덱스 생성/검증
- `scripts/validate_examples.py` : SQL 예시 검증
- `scripts/pregen_demo_cache.py` : 데모 캐시 생성
- `scripts/eval_questions.py` : 질문→SQL→결과 비교 평가
- `scripts/eval_report_summary.py` : 평가 요약 + CSV 출력
- `scripts/test_oracle_connection.py` : Oracle 연결 확인

---

### 2.5 Deploy (Docker) 상세

- `deploy/compose/docker-compose.yml`
  - API/UI 컨테이너 구성 및 포트 매핑
- `deploy/docker/Dockerfile.api`
  - API 이미지 빌드
- `deploy/docker/Dockerfile.ui`
  - UI 이미지 빌드

---

### 2.6 var/ 디렉토리 (런타임 데이터)

- `var/metadata/` : 스키마/예시/용어집/템플릿 등 RAG 문서
- `var/cache/` : Demo 캐시
- `var/logs/` : 실행 로그, 비용, 평가 결과 등
- `var/rag/` : SimpleStore 저장소 (Mongo 설정이 없을 때 사용)
- `var/mongo/` : Docker Compose MongoDB 데이터 디렉터리

> 이 디렉토리는 **런타임 생성 데이터**가 많으므로 보통 git에서 제외합니다.

---

## 3) FAQ

**Q. 모델 학습은 어디서 하나요?**
- 이 프로젝트 안에서 학습은 하지 않습니다. 외부 LLM(API)을 사용합니다.

**Q. 성능을 높이려면 무엇을 바꿔야 하나요?**
- `var/metadata/sql_examples.jsonl`, `glossary_docs.jsonl`를 보강하고
- `sql_postprocess.py`에 보정 규칙을 추가하는 것이 가장 효과적입니다.

---

## 4) 참고 문서

- `docs/TECHNICAL.md` : 기술 문서(운영/구성 상세)
- `docs/MODEL_GUIDE_KO.md` : 모델 사용/학습 방식 설명서
