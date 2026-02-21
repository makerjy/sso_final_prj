# sso_final_prj

## Quick Start (Local)

```bash
cd /Users/ijaeyong/projects/Oracle_team9_prj/final_pdfllm
./scripts/bootstrap_dev.sh
./scripts/smoke_local.sh
```

- `bootstrap_dev.sh`
  - `query-visualization/.venv` 생성 + 의존성 설치
  - `text-to-sql/backend/.venv` 생성 + 의존성 설치
  - `ui` 의존성(`npm ci`) 설치
  - 누락된 `.env` 파일이 있으면 `*.env.example`에서 생성
- `smoke_local.sh`
  - `text-to-sql` API 실행 후 `GET /health` 확인
  - `query-visualization` API 실행 후 `GET /health` 확인
  - `POST /visualize` 샘플 요청으로 차트 응답(`figure_json`) 검증

## Quick Start (Docker)

```bash
cd /Users/ijaeyong/projects/Oracle_team9_prj/final_pdfllm
docker compose up -d --build
docker compose ps
```

또는 Make 타깃을 사용할 수 있습니다.

```bash
make bootstrap
make smoke-local
make up
make down
```
