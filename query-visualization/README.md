# Query Visualization

`query-visualization`는 `text-to-sql` 결과(`user_query`, `sql`, `rows`)를 입력받아
의도 추출, 룰 기반 검증, 차트 생성, 인사이트 요약을 수행하는 API 서비스입니다.

## Quick Start

```bash
docker compose -f query-visualization/deploy/compose/docker-compose.yml up -d --build
```

- API: `http://localhost:8080`
- UI: `http://localhost:3001`

## API Contract

### Request

`POST /visualize`

| field | type | note |
| --- | --- | --- |
| `user_query` | `string` | 1~2000 chars |
| `sql` | `string` | 1~2000 chars |
| `rows` | `array<object>` | max 10,000 rows |

### Response

| field | type | note |
| --- | --- | --- |
| `sql` | `string` | original sql |
| `table_preview` | `array<object>` | first 20 rows |
| `analyses` | `array<object>` | chart recommendation cards |
| `insight` | `string` | llm/fallback insight |
| `fallback_used` | `boolean` | whether relaxed retry was used |
| `fallback_stage` | `string \| null` | retry stage name |
| `failure_reasons` | `array<string>` | failure reasons collected during pipeline |
| `attempt_count` | `number` | 1(normal), 2(relaxed retry) |
| `request_id` | `string \| null` | trace id |
| `total_latency_ms` | `number \| null` | end-to-end latency |
| `stage_latency_ms` | `object` | per-stage latency map |

### Error Codes

| code | http | description |
| --- | --- | --- |
| `ROWS_LIMIT_EXCEEDED` | 413 | `rows` exceeds 10,000 |
| `INVALID_ROWS` | 422 | `rows` is not `array<object>` |
| `ANALYSIS_IMPORT_ERROR` | 501 | analysis module import error |
| `DB_TEST_FAILED` | 500 | db-test endpoint failure |

## Pipeline

1. `rows -> DataFrame` 변환
2. elapsed 컬럼 파생(`elapsed_icu_days`, `elapsed_admit_days`)
3. 스키마 요약 생성
4. RAG 검색(점수/버전 필터 포함)
5. 의도 추출(LLM + fallback)
6. 룰 엔진 플랜 생성(normal)
7. 차트 생성
8. 실패 시 relaxed 모드 재시도
9. 인사이트 생성(LLM 실패 시 통계 fallback)

## Recent Hardening (1~8)

1. 인코딩/문구 깨짐 수정  
   - `ui/public/index.html` 전면 정리  
   - `src/db/schema_introspect.py` 재작성
2. 룰엔진 분할  
   - `src/agent/rule_engine_postprocess.py`로 plan 후처리/우선순위 분리
3. 평가지표 코드화  
   - `src/metrics/evaluator.py`, `scripts/evaluate_pipeline.py`
4. 테스트 보강  
   - `tests/test_api_server.py` (payload 제한)  
   - `tests/test_analysis_agent.py` 빈 결과 fallback 케이스  
   - `tests/test_rule_engine.py` 식별자 group 차단 케이스
5. 관측성 강화  
   - `src/utils/logging.py` 구조화 JSON 로그  
   - request 단위 `request_id` 추적
6. API 계약 강화  
   - 길이/행수 제한, 표준 에러코드
7. RAG 품질 가드  
   - `RAG_MIN_SCORE`, `RAG_DOC_VERSION`, `RAG_CONTEXT_MAX_CHARS`
8. 포트폴리오 증거화  
   - 응답에 stage latency/fallback 정보 노출

## Evaluation Metrics

평가 스크립트:

```bash
cd query-visualization
python scripts/evaluate_pipeline.py
```

기본 산출 지표:

- `render_success_rate_pct`: 차트 생성 성공률
- `fallback_rate_pct`: fallback 사용 비율
- `failure_free_rate_pct`: failure reason 없는 비율
- `avg_latency_ms`: 평균 응답 지연시간
- `max_latency_ms`: 최대 응답 지연시간

출력 예시 형식:

```json
{
  "case_count": 3,
  "render_success_rate_pct": 66.67,
  "fallback_rate_pct": 33.33,
  "failure_free_rate_pct": 0.0,
  "avg_latency_ms": 202.35,
  "max_latency_ms": 519.7,
  "results": []
}
```

## Important Environment Variables

| name | default | note |
| --- | --- | --- |
| `OPENAI_API_KEY` | - | required for llm/embedding |
| `OPENAI_EMBEDDING_MODEL` | `text-embedding-3-small` | embedding model |
| `MONGODB_URI` | - | atlas connection |
| `RAG_TOP_K` | `6` | top-k retrieval |
| `RAG_MIN_SCORE` | `0.2` | retrieval score threshold |
| `RAG_CONTEXT_MAX_CHARS` | `4000` | max context length |
| `RAG_DOC_VERSION` | `v1` | document version filter |
