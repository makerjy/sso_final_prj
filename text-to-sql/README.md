# Text-to-SQL 데모 (RAG + MongoDB + Oracle)

자연어 질문을 안전한 Oracle SQL로 변환하는 데모 스택입니다. RAG 컨텍스트, 정책 게이팅, 예산 추적을 포함하며, Demo/Advanced 흐름을 제공하는 간단한 UI가 있습니다.

## 프로젝트 구조

- `backend/` FastAPI API (RAG, Oracle, 정책, 예산)
- `ui/` Next.js UI
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

## UI 흐름

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
