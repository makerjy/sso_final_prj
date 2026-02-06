# 모델/구성 설명서

이 문서는 **Text-to-SQL 데모**에서 어떤 모델을 쓰는지, 학습은 어떻게 하는지, 그리고 전체 구성이 어떻게 동작하는지를 **처음 보는 사람도 이해하기 쉽게** 정리합니다. 이 문서는 코드 변경 없이 참고용입니다.

---

## 1) 한 줄 요약
이 시스템은 **외부 LLM(API)**을 사용해서 SQL을 생성하고, 내부의 **룰 기반 안전장치 + RAG 문서**로 결과를 보정하는 구조입니다. **여기서 모델을 직접 학습시키지는 않습니다.**

---

## 2) 어떤 모델을 사용하나요?

`backend/app/core/config.py`에서 기본값을 확인할 수 있습니다.

- **ENGINEER_MODEL** (기본: `gpt-4o`)
  - 질문 + RAG 컨텍스트를 받아 **초안을 생성**합니다.
- **EXPERT_MODEL** (기본: `gpt-4o-mini`)
  - 위험도가 높으면 **초안을 재검토/수정**합니다.
- **INTENT_MODEL** (기본: `local`)
  - 현재는 **실제로 사용되지 않음**. (의도/위험도는 룰 기반)

실제로 호출되는 곳:
- `backend/app/services/agents/sql_engineer.py`
- `backend/app/services/agents/sql_expert.py`
- `backend/app/services/agents/llm_client.py`

LLM 호출은 OpenAI 호환 API 형식이며, 아래 환경 변수를 사용합니다.
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `OPENAI_ORG`

---

## 3) 모델은 어떻게 학습시키나요?

**이 프로젝트 안에서 모델 학습은 하지 않습니다.**
모델은 외부 LLM(API)으로 제공됩니다.

대신 아래를 통해 “모델의 성능”을 간접적으로 개선합니다.

1) **RAG 문서 보강**
   - 스키마, 용어집, SQL 예시 등을 `var/metadata/`에 추가
2) **프롬프트 개선**
   - `backend/app/services/agents/prompts.py`
3) **후처리 규칙 개선**
   - `backend/app/services/agents/sql_postprocess.py`

즉, **학습(파인튜닝)**이 아니라 **문서/규칙/프롬프트 개선**으로 품질을 올립니다.

---

## 4) 전체 동작 흐름 

1) **질문 입력**
   - UI `/ask`에서 질문

2) **Demo 모드인지 확인**
   - `DEMO_MODE=true`이면 데모 캐시에서 즉시 반환
   - 캐시는 `var/cache/demo_cache.json`

3) **RAG 컨텍스트 만들기**
   - 스키마, 예시, 템플릿, 용어집에서 관련 문서를 검색
   - `backend/app/services/rag/retrieval.py`

4) **SQL 초안 생성 (ENGINEER 모델)**
   - 질문 + RAG 컨텍스트로 SQL 생성

5) **위험도 평가 + EXPERT 모델(조건부)**
   - 룰 기반 위험도 평가 후, 위험 점수 높으면 EXPERT 모델로 재검토

6) **후처리 규칙 적용**
   - Oracle 문법 보정, 컬럼/테이블 매핑, row cap 등
   - `backend/app/services/agents/sql_postprocess.py`

7) **정책 검사(PolicyGate)**
   - SELECT만 허용, WHERE 필수, JOIN 제한 등

8) **실행 + 결과 반환**
   - Oracle에서 실행 후 결과 출력

---

## 5) 모델 구성 요소 요약

| 구성 요소 | 역할 | 위치 |
|---|---|---|
| ENGINEER_MODEL | 1차 SQL 생성 | `sql_engineer.py` |
| EXPERT_MODEL | 위험 시 재검토 | `sql_expert.py` |
| INTENT/RISK | 룰 기반 위험 판별 | `runtime/risk_classifier.py` |
| RAG 검색 | 스키마/예시/템플릿/용어집 검색 | `rag/retrieval.py` |
| 후처리 규칙 | Oracle 문법/매핑/보정 | `sql_postprocess.py` |

---

## 6) 초보자용 설정 가이드

`.env`에서 다음만 바꿔도 기본 구성이 동작합니다.

```
OPENAI_API_KEY=...
ENGINEER_MODEL=gpt-4o
EXPERT_MODEL=gpt-4o-mini
ORACLE_DSN=host:1521/service
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DEFAULT_SCHEMA=SSO
```

모델을 바꾸려면 `ENGINEER_MODEL`, `EXPERT_MODEL` 값만 변경하면 됩니다.

---

## 7) 모델 성능을 올리는 가장 쉬운 방법

1) **sql_examples.jsonl에 예시 추가**
   - `var/metadata/sql_examples.jsonl`
2) **glossary_docs.jsonl에 동의어/용어 추가**
   - `var/metadata/glossary_docs.jsonl`
3) **후처리 규칙 추가**
   - `backend/app/services/agents/sql_postprocess.py`

이 3가지가 실제 체감 성능 개선에 가장 영향이 큽니다.

---

## 8) 자주 묻는 질문

### Q. 모델을 직접 학습시키나요?
A. 아니요. 외부 LLM(API)을 사용하며, 학습은 하지 않습니다.

### Q. “학습” 대신 무엇을 하나요?
A. RAG 문서와 SQL 예시를 늘리고, 후처리 규칙을 강화합니다.

### Q. 로컬 모델로 바꿀 수 있나요?
A. OpenAI 호환 API를 제공하는 로컬 서버를 연결하면 가능합니다.
   (`OPENAI_BASE_URL` 변경)

---

## 9) 관련 파일 위치

- 모델 설정: `backend/app/core/config.py`
- 프롬프트: `backend/app/services/agents/prompts.py`
- LLM 호출: `backend/app/services/agents/llm_client.py`
- SQL 생성: `backend/app/services/agents/sql_engineer.py`
- SQL 리뷰: `backend/app/services/agents/sql_expert.py`
- 후처리: `backend/app/services/agents/sql_postprocess.py`
- RAG 검색: `backend/app/services/rag/retrieval.py`

