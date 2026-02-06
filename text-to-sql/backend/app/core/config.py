from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _str(value: str | None, default: str = "") -> str:
    if value is None:
        return default
    return value


BASE_DIR = Path(__file__).resolve().parents[3]
env_path = BASE_DIR / ".env"
if env_path.is_file():
    _load_dotenv(env_path)


@dataclass(frozen=True)
class Settings:
    demo_mode: bool
    budget_limit_krw: int
    cost_alert_threshold_krw: int
    llm_cost_per_1k_tokens_krw: int
    sql_run_cost_krw: int

    engineer_model: str
    expert_model: str
    intent_model: str

    context_token_budget: int
    examples_per_query: int
    templates_per_query: int
    llm_max_output_tokens: int
    llm_timeout_sec: int
    translate_ko_to_en: bool
    demo_cache_always: bool

    max_retry_attempts: int
    expert_trigger_mode: str
    expert_score_threshold: int

    max_db_joins: int
    row_cap: int
    db_timeout_sec: int

    oracle_dsn: str
    oracle_user: str
    oracle_password: str
    oracle_default_schema: str
    oracle_pool_min: int
    oracle_pool_max: int
    oracle_pool_inc: int
    oracle_pool_timeout_sec: int

    rag_persist_dir: str
    rag_top_k: int
    rag_embedding_dim: int
    rag_multi_query: bool
    mongo_uri: str
    mongo_db: str
    mongo_collection: str
    mongo_vector_index: str

    events_log_path: str
    cost_state_path: str
    budget_config_path: str
    demo_cache_path: str

    openai_api_key: str
    openai_base_url: str
    openai_org: str


def load_settings() -> Settings:
    return Settings(
        demo_mode=_bool(os.getenv("DEMO_MODE"), True),
        budget_limit_krw=_int(os.getenv("BUDGET_LIMIT_KRW"), 10000),
        cost_alert_threshold_krw=_int(os.getenv("COST_ALERT_THRESHOLD_KRW"), 8000),
        llm_cost_per_1k_tokens_krw=_int(os.getenv("LLM_COST_PER_1K_TOKENS_KRW"), 1),
        sql_run_cost_krw=_int(os.getenv("SQL_RUN_COST_KRW"), 1),
        engineer_model=_str(os.getenv("ENGINEER_MODEL"), "gpt-4o"),
        expert_model=_str(os.getenv("EXPERT_MODEL"), "gpt-4o-mini"),
        intent_model=_str(os.getenv("INTENT_MODEL"), "local"),
        context_token_budget=_int(os.getenv("CONTEXT_TOKEN_BUDGET"), 2000),
        examples_per_query=_int(os.getenv("EXAMPLES_PER_QUERY"), 2),
        templates_per_query=_int(os.getenv("TEMPLATES_PER_QUERY"), 1),
        llm_max_output_tokens=_int(os.getenv("LLM_MAX_OUTPUT_TOKENS"), 500),
        llm_timeout_sec=_int(os.getenv("LLM_TIMEOUT_SEC"), 30),
        translate_ko_to_en=_bool(os.getenv("TRANSLATE_KO_TO_EN"), True),
        demo_cache_always=_bool(os.getenv("DEMO_CACHE_ALWAYS"), False),
        max_retry_attempts=_int(os.getenv("MAX_RETRY_ATTEMPTS"), 1),
        expert_trigger_mode=_str(os.getenv("EXPERT_TRIGGER_MODE"), "score"),
        expert_score_threshold=_int(os.getenv("EXPERT_SCORE_THRESHOLD"), 3),
        max_db_joins=_int(os.getenv("MAX_DB_JOINS"), 3),
        row_cap=_int(os.getenv("ROW_CAP"), 5000),
        db_timeout_sec=_int(os.getenv("DB_TIMEOUT_SEC"), 15),
        oracle_dsn=_str(os.getenv("ORACLE_DSN"), ""),
        oracle_user=_str(os.getenv("ORACLE_USER"), ""),
        oracle_password=_str(os.getenv("ORACLE_PASSWORD"), ""),
        oracle_default_schema=_str(os.getenv("ORACLE_DEFAULT_SCHEMA"), ""),
        oracle_pool_min=_int(os.getenv("ORACLE_POOL_MIN"), 1),
        oracle_pool_max=_int(os.getenv("ORACLE_POOL_MAX"), 4),
        oracle_pool_inc=_int(os.getenv("ORACLE_POOL_INC"), 1),
        oracle_pool_timeout_sec=_int(os.getenv("ORACLE_POOL_TIMEOUT_SEC"), 10),
        rag_persist_dir=_str(os.getenv("RAG_PERSIST_DIR"), "var/rag"),
        rag_top_k=_int(os.getenv("RAG_TOP_K"), 5),
        rag_embedding_dim=_int(os.getenv("RAG_EMBEDDING_DIM"), 128),
        rag_multi_query=_bool(os.getenv("RAG_MULTI_QUERY"), True),
        mongo_uri=_str(os.getenv("MONGO_URI"), ""),
        mongo_db=_str(os.getenv("MONGO_DB"), "text_to_sql"),
        mongo_collection=_str(os.getenv("MONGO_COLLECTION"), "rag_docs"),
        mongo_vector_index=_str(os.getenv("MONGO_VECTOR_INDEX"), ""),
        events_log_path=_str(os.getenv("EVENTS_LOG_PATH"), "var/logs/events.jsonl"),
        cost_state_path=_str(os.getenv("COST_STATE_PATH"), "var/logs/cost_state.json"),
        budget_config_path=_str(os.getenv("BUDGET_CONFIG_PATH"), "var/logs/budget_config.json"),
        demo_cache_path=_str(os.getenv("DEMO_CACHE_PATH"), "var/cache/demo_cache.json"),
        openai_api_key=_str(os.getenv("OPENAI_API_KEY"), ""),
        openai_base_url=_str(os.getenv("OPENAI_BASE_URL"), ""),
        openai_org=_str(os.getenv("OPENAI_ORG"), ""),
    )


_SETTINGS: Settings | None = None


def get_settings() -> Settings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = load_settings()
    return _SETTINGS
