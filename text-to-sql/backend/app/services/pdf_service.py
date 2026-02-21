# app/services/pdf_service.py
import os
import fitz  # PyMuPDF
import json
import logging
import traceback
import hashlib
from pathlib import Path
from openai import AsyncOpenAI
import re
import base64
from difflib import get_close_matches
from typing import Any

from app.services.runtime.state_store import get_state_store
from app.services.oracle.executor import execute_sql
from app.services.agents.orchestrator import run_oneshot

logger = logging.getLogger(__name__)
# 로깅 설정을 보장하기 위해 출력 핸들러 강제 추가
if not logger.handlers:
    import sys
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)
logger.setLevel(logging.INFO)

_SAVED_COHORTS_KEY = "cohort::saved"
_PDF_CACHE_KEY = "pdf_extraction::cache"

# 사용할 테이블 목록 (schema_catalog.json 기반)
_COHORT_TABLES = [
    "PATIENTS", "ADMISSIONS", "DIAGNOSES_ICD", "ICUSTAYS",
    "D_ICD_DIAGNOSES", "D_ICD_PROCEDURES", "PROCEDURES_ICD",
    "LABEVENTS", "D_LABITEMS", "PRESCRIPTIONS", "TRANSFERS",
]

_SCHEMA_CATALOG_PATH = Path(os.getenv("SCHEMA_CATALOG_PATH", "/app/var/metadata/schema_catalog.json"))
_DERIVED_VAR_PATH = Path(os.getenv("DERIVED_VAR_PATH", "/app/var/metadata/derived_variables.json"))
_JOIN_GRAPH_PATH = Path(os.getenv("JOIN_GRAPH_PATH", "/app/var/metadata/join_graph.json"))

# 대체 경로 (로컬 개발용)
_METADATA_LOCAL_BASE = Path(__file__).resolve().parents[3] / "var" / "metadata"
_SCHEMA_CATALOG_LOCAL = _METADATA_LOCAL_BASE / "schema_catalog.json"
_DERIVED_VAR_LOCAL = _METADATA_LOCAL_BASE / "derived_variables.json"
_JOIN_GRAPH_LOCAL = _METADATA_LOCAL_BASE / "join_graph.json"

_SIGNAL_NAME_ALIASES: dict[str, str] = {
    "temp": "body_temperature",
    "temperature": "body_temperature",
    "body_temp": "body_temperature",
    "bodytemperature": "body_temperature",
    "bun_level": "bun",
    "blood_urea_nitrogen": "bun",
    "blood_urea_nitrogen_level": "bun",
    "urea_nitrogen": "bun",
    "serum_bun": "bun",
    "cr": "creatinine",
    "creat": "creatinine",
    "serum_creatinine": "creatinine",
    "po2": "pao2",
    "pa_o2": "pao2",
    "partial_pressure_o2": "pao2",
    "arterial_o2_tension": "pao2",
    "blood_ph": "ph",
    "arterial_ph": "ph",
    "ph_value": "ph",
    "anion_gap_level": "anion_gap",
    "uop": "urine_output",
    "uo": "urine_output",
    "urine": "urine_output",
    "urine_out": "urine_output",
    "urine_volume": "urine_output",
    "sex": "gender",
    "hospital_length_of_stay": "hospital_los",
    "length_of_hospital_stay": "hospital_los",
    "hospital_los_days": "hospital_los",
    "hosp_los": "hospital_los",
    "icu_length_of_stay": "icu_los",
    "length_of_icu_stay": "icu_los",
    "icu_stay_length": "icu_los",
    "icu_los_days": "icu_los",
    "in_hospital_death": "in_hospital_mortality",
    "inhospital_mortality": "in_hospital_mortality",
    "hospital_expire_flag": "in_hospital_mortality",
}

_RESULT_IDENTIFIER_COLUMNS = {"SUBJECT_ID", "HADM_ID", "STAY_ID"}


def _normalize_signal_name(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    key = re.sub(r"[^a-z0-9]+", "_", raw)
    key = re.sub(r"_+", "_", key).strip("_")
    return _SIGNAL_NAME_ALIASES.get(key, key)


def _load_metadata_json(path_env: Path, path_local: Path) -> dict:
    path = path_env if path_env.exists() else path_local
    if not path.exists():
        logger.warning(f"Metadata file not found: {path}")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to load metadata {path}: {e}")
        return {}


def _load_schema_for_prompt() -> str:
    """schema_catalog.json에서 테이블/컬럼 정보를 읽어 프롬프트용 텍스트 생성"""
    catalog_path = _SCHEMA_CATALOG_PATH if _SCHEMA_CATALOG_PATH.exists() else _SCHEMA_CATALOG_LOCAL

    if not catalog_path.exists():
        logger.warning("schema_catalog.json을 찾을 수 없습니다: %s", catalog_path)
        return _fallback_schema()

    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("schema_catalog.json 파싱 실패: %s", e)
        return _fallback_schema()

    tables = catalog.get("tables", {})
    lines = []
    for tname in _COHORT_TABLES:
        tinfo = tables.get(tname)
        if not tinfo:
            continue
        cols = tinfo.get("columns", [])
        col_parts = []
        for c in cols:
            cname = c.get("name", "")
            ctype = c.get("type", "")
            col_parts.append(f"{cname} {ctype}")
        pks = tinfo.get("primary_keys", [])
        pk_text = f" [PK: {', '.join(pks)}]" if pks else ""
        lines.append(f"- {tname} ({', '.join(col_parts)}){pk_text}")

    return "\n".join(lines)


def _fallback_schema() -> str:
    """schema_catalog.json 없을 때 사용하는 하드코딩 스키마"""
    return """- PATIENTS (SUBJECT_ID NUMBER, GENDER VARCHAR2, ANCHOR_AGE NUMBER, DOD TIMESTAMP)
- ADMISSIONS (HADM_ID NUMBER, SUBJECT_ID NUMBER, ADMITTIME TIMESTAMP(6), DISCHTIME TIMESTAMP(6), DEATHTIME TIMESTAMP(6), ADMISSION_TYPE VARCHAR2, ADMIT_PROVIDER_ID VARCHAR2, ADMISSION_LOCATION VARCHAR2, DISCHARGE_LOCATION VARCHAR2, INSURANCE VARCHAR2, LANGUAGE VARCHAR2, MARITAL_STATUS VARCHAR2, RACE VARCHAR2, EDREGTIME TIMESTAMP(6), EDOUTTIME TIMESTAMP(6), HOSPITAL_EXPIRE_FLAG NUMBER) [PK: HADM_ID]
- DIAGNOSES_ICD (SUBJECT_ID NUMBER, HADM_ID NUMBER, SEQ_NUM NUMBER, ICD_CODE CHAR, ICD_VERSION NUMBER)
- ICUSTAYS (SUBJECT_ID NUMBER, HADM_ID NUMBER, STAY_ID NUMBER, FIRST_CAREUNIT VARCHAR2, LAST_CAREUNIT VARCHAR2, INTIME TIMESTAMP(0), OUTTIME TIMESTAMP(0), LOS NUMBER) [PK: STAY_ID]
- D_ICD_DIAGNOSES (ICD_CODE CHAR, ICD_VERSION NUMBER, LONG_TITLE VARCHAR2) [PK: ICD_CODE, ICD_VERSION]"""


def _load_valid_columns() -> dict[str, set[str]]:
    """schema_catalog.json에서 테이블별 유효 컬럼명 집합을 로드"""
    catalog_path = _SCHEMA_CATALOG_PATH if _SCHEMA_CATALOG_PATH.exists() else _SCHEMA_CATALOG_LOCAL
    if not catalog_path.exists():
        return {}
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    result = {}
    for tname, tinfo in catalog.get("tables", {}).items():
        cols = {c["name"].upper() for c in tinfo.get("columns", []) if c.get("name")}
        result[tname.upper()] = cols
    return result


def _fix_column_names_in_sql(sql: str) -> tuple[str, list[str]]:
    """
    SQL에서 alias.COLUMN 패턴을 찾아 실제 스키마와 대조하고, 
    유사한 컬럼명으로 자동 수정.
    """
    valid_cols = _load_valid_columns()
    if not valid_cols:
        return sql, []

    # SQL에서 테이블 alias → 실제 테이블명 매핑 구축
    alias_map: dict[str, str] = {}
    # FROM/JOIN TABLENAME alias 패턴
    for match in re.finditer(
        r'(?:FROM|JOIN)\s+([A-Z_]+)\s+([A-Z]{1,4})\b',
        sql, re.IGNORECASE
    ):
        table_name = match.group(1).upper()
        alias = match.group(2).upper()
        if table_name in valid_cols:
            alias_map[alias] = table_name

    if not alias_map:
        return sql, []

    fixes = []
    # 모든 유효 컬럼을 하나의 풀로 합침 (fuzzy match 대상)
    all_columns = set()
    for cols in valid_cols.values():
        all_columns.update(cols)

    def replace_column(m: re.Match) -> str:
        alias = m.group(1).upper()
        col = m.group(2).upper()
        original = m.group(0)

        table = alias_map.get(alias)
        if not table:
            return original

        table_cols = valid_cols.get(table, set())
        if col in table_cols:
            return original  # 이미 유효

        # fuzzy match: 해당 테이블 컬럼에서 유사한 것 찾기
        candidates = get_close_matches(col, list(table_cols), n=1, cutoff=0.6)
        if candidates:
            fixed_col = candidates[0]
            fixes.append(f"{alias}.{col} → {alias}.{fixed_col} ({table})")
            return f"{m.group(1)}.{fixed_col}"

        return original

    # alias.COLUMN_NAME 패턴 매칭
    fixed_sql = re.sub(
        r'\b([A-Za-z]{1,4})\.([A-Z_]+)\b',
        replace_column,
        sql,
        flags=re.IGNORECASE
    )

    if fixes:
        logger.info("SQL 컬럼명 자동수정: %s", "; ".join(fixes))

    return fixed_sql, fixes


def _normalize_result_columns(columns: Any) -> list[str]:
    if not isinstance(columns, list):
        return []
    normalized: list[str] = []
    for col in columns:
        name = str(col or "").strip().upper()
        if name:
            normalized.append(name)
    return normalized


def _has_identifier_columns(columns: list[str]) -> bool:
    return any(col in _RESULT_IDENTIFIER_COLUMNS for col in columns)


def _append_warning_once(result: dict[str, Any], message: str) -> None:
    warnings = result.get("warning")
    if not isinstance(warnings, list):
        warnings = []
    if message not in warnings:
        warnings.append(message)
    result["warning"] = warnings


def _load_reference_cohorts() -> str:
    """MongoDB에서 저장된 PDF 기반 코호트 정보를 읽어 RAG 예시로 활용"""
    try:
        store = get_state_store()
        payload = store.get(_SAVED_COHORTS_KEY) or {}
        # cohort.py의 _get_saved_cohorts()와 동일한 추출 로직 사용
        cohorts = payload.get("cohorts", []) if isinstance(payload, dict) else []
        
        # PDF 기반이고 SQL과 요약이 있는 것들을 추출
        pdf_cohorts = []
        for c in cohorts:
            if not isinstance(c, dict):
                continue
            if c.get("source_type") != "pdf":
                continue
            if not c.get("sql_query"):
                continue
            
            # cohort_definition이 None이거나 dict가 아닐 경우를 대비
            cd = c.get("cohort_definition")
            if not isinstance(cd, dict):
                continue
            
            if cd.get("summary_ko"):
                # 환자 수가 0명인 예시는 RAG에서 제외 (잘못된 쿼리 전파 방지)
                metrics = c.get("metrics") or {}
                p_count = metrics.get("patient_count")
                if p_count is not None:
                    try:
                        if int(p_count) <= 0:
                            continue
                    except:
                        pass
                pdf_cohorts.append(c)
        
        # 생성 일시(created_at) 기준 오름차순 정렬하여 '검증된 기초 사례'들이 우선적으로 프롬프트에 들어가도록 함
        pdf_cohorts.sort(key=lambda x: str(x.get("created_at", "")))
        samples = pdf_cohorts[:5]
        
        if not samples:
            return "No previous reference cohorts available."
            
        ref_texts = []
        for i, c in enumerate(samples):
            cd = c.get("cohort_definition") or {}
            ref_texts.append(f"### [Reference Example {i+1}]")
            ref_texts.append(f"- Title: {cd.get('title', 'N/A')}")
            ref_texts.append(f"- Summary (KR): {cd.get('summary_ko', '')[:150]}...")
            ref_texts.append(f"- Valid SQL:\n{c.get('sql_query')}\n")
            
        return "\n".join(ref_texts)
    except Exception as e:
        logger.warning("RAG 참조 데이터 로드 실패: %s", e)
        return "Error loading reference cohorts."


# clinical signal dictionary for template-based SQL generation
# Default clinical signals with complex logic (hardcoded fallback)
DEFAULT_CORE_SIGNALS = {
    "age": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.anchor_age >= {min} AND p.anchor_age <= {max}",
    "gender": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.gender = '{gender}'",
    "sex": "SELECT a.hadm_id FROM SSO.PATIENTS p JOIN SSO.ADMISSIONS a ON p.subject_id = a.subject_id WHERE p.gender = '{gender}'",
    "diagnosis": "SELECT HADM_ID FROM SSO.DIAGNOSES_ICD WHERE trim(icd_code) IN ({codes})",
    "icu_stay": "SELECT stay_id, hadm_id, intime as charttime FROM SSO.ICUSTAYS WHERE los >= {min_los}",
    "prescription": "SELECT hadm_id, starttime as charttime FROM SSO.PRESCRIPTIONS WHERE lower(drug) LIKE '%{drug}%'",
    # SOFA, OASIS, etc. are complex derived scores
    "sofa": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE (itemid IN (220052, 220181, 225312) AND valuenum < 65) OR (itemid IN (223900, 223901) AND valuenum < 15)", 
    "rox": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE (itemid IN (220277) AND valuenum < 90) OR (itemid IN (220210, 224690) AND valuenum > 25)",
    "oasis": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223900, 223901) AND valuenum < 13",
    "sae_diagnosis": "SELECT hadm_id, admittime as charttime FROM SSO.ADMISSIONS WHERE hadm_id IN (SELECT hadm_id FROM SSO.DIAGNOSES_ICD WHERE trim(icd_code) IN ('F05', 'R410', '2930', '3483')) UNION SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223900, 220739) AND valuenum < 15",
    # Special FIO2 logic
    "fio2": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223835) AND (CASE WHEN valuenum > 1 AND valuenum <= 100 THEN valuenum/100 WHEN valuenum > 0 AND valuenum <= 1 THEN valuenum ELSE NULL END) {operator} {value}",
    "body_temperature": "SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN (223761, 223762) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "bun": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (51006) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "creatinine": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50912) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "pao2": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50821) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "ph": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50820) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "anion_gap": "SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN (50868) AND valuenum {operator} {value} AND valuenum IS NOT NULL",
    "urine_output": "SELECT stay_id, charttime FROM SSO.OUTPUTEVENTS WHERE itemid IN (226559, 226560, 226561, 226563, 226564, 226565, 226567, 226557, 226558, 226584, 227488) AND value {operator} {value}",
}

# Default metadata for frontend display
DEFAULT_SIGNAL_METADATA = {
    "age": {"target_table": "PATIENTS", "itemid": "anchor_age"},
    "gender": {"target_table": "PATIENTS", "itemid": "gender"},
    "sex": {"target_table": "PATIENTS", "itemid": "gender"},
    "sofa": {"target_table": "DERIVED", "itemid": "sofa_score"},
    "rox": {"target_table": "DERIVED", "itemid": "rox_index"},
    "oasis": {"target_table": "DERIVED", "itemid": "oasis_score"},
    "body_temperature": {"target_table": "CHARTEVENTS", "itemid": "223761,223762"},
    "bun": {"target_table": "LABEVENTS", "itemid": "51006"},
    "creatinine": {"target_table": "LABEVENTS", "itemid": "50912"},
    "pao2": {"target_table": "LABEVENTS", "itemid": "50821"},
    "ph": {"target_table": "LABEVENTS", "itemid": "50820"},
    "anion_gap": {"target_table": "LABEVENTS", "itemid": "50868"},
    "urine_output": {"target_table": "OUTPUTEVENTS", "itemid": "226559,226560,226561,226563,226564,226565,226567,226557,226558,226584,227488"},
    "hospital_los": {"target_table": "ADMISSIONS", "itemid": "dischtime-admittime"},
    "icu_los": {"target_table": "ICUSTAYS", "itemid": "los"},
    "in_hospital_mortality": {"target_table": "ADMISSIONS", "itemid": "hospital_expire_flag"},
}

WINDOW_TEMPLATES = {
    "icu_first_24h": "s.charttime BETWEEN p.intime AND p.intime + INTERVAL '24' HOUR",
    "admission_first_24h": "s.charttime BETWEEN p.admittime AND p.admittime + INTERVAL '24' HOUR",
    "icu_discharge_last_24h": "s.charttime BETWEEN p.outtime - INTERVAL '24' HOUR AND p.outtime"
}

class PDFCohortService:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("ENGINEER_MODEL", "gpt-4o")
        self.signal_map = {}
        self.signal_metadata = {}
        self._initialize_signal_maps()

    def _initialize_signal_maps(self):
        """Initialize signal maps by merging defaults with dynamic JSON metadata."""
        self.signal_map = DEFAULT_CORE_SIGNALS.copy()
        self.signal_metadata = DEFAULT_SIGNAL_METADATA.copy()
        
        try:
            # 1. Try absolute path (Docker environment standard)
            meta_path = "/app/var/metadata/mimic_rag_metadata_full.json"
            
            # 2. If not found, try relative path (Local development fallback)
            if not os.path.exists(meta_path):
                # backend/app/services/pdf_service.py -> backend/app/services -> backend/app -> backend -> root -> var
                meta_path = os.path.join(os.path.dirname(__file__), "../../../../var/metadata/mimic_rag_metadata_full.json")
            
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    full_meta = json.load(f)
                    
                for item in full_meta:
                    name = _normalize_signal_name(item.get("signal_name", ""))
                    if not name:
                        continue
                    mapping = item.get("mapping", {})
                    itemid = mapping.get("itemid")
                    table = mapping.get("target_table", "").upper()
                    
                    if itemid and table:
                        # Generate SQL Template
                        sql = None
                        if table == "CHARTEVENTS":
                            sql = f"SELECT stay_id, charttime FROM SSO.CHARTEVENTS WHERE itemid IN ({itemid}) AND valuenum {{operator}} {{value}} AND valuenum IS NOT NULL"
                        elif table == "LABEVENTS":
                             # Lab items often need joining with D_LABITEMS for readable labels, but if itemID is known, direct query is faster.
                             # However, to maintain compatibility with existing 'lab' template logic:
                             sql = f"SELECT hadm_id, charttime FROM SSO.LABEVENTS WHERE itemid IN ({itemid}) AND valuenum {{operator}} {{value}} AND valuenum IS NOT NULL"
                        
                        if sql:
                            self.signal_map[name] = sql
                            # Also add synonyms
                            for syn in item.get("synonyms", []):
                                syn_key = _normalize_signal_name(syn)
                                if not syn_key:
                                    continue
                                if syn_key not in self.signal_map: # Don't overwrite core signals
                                    self.signal_map[syn_key] = sql

                        # Update Metadata
                        if name not in self.signal_metadata:
                            self.signal_metadata[name] = {
                                "target_table": table,
                                "itemid": str(itemid)
                            }
                        for syn in item.get("synonyms", []):
                            syn_key = _normalize_signal_name(syn)
                            if not syn_key or syn_key in self.signal_metadata:
                                continue
                            self.signal_metadata[syn_key] = {
                                "target_table": table,
                                "itemid": str(itemid),
                            }
            else:
                logger.warning(f"RAG Metadata file not found at {meta_path}. Using defaults only.")
        except Exception as e:
            logger.error(f"Failed to load RAG metadata: {e}. Using defaults.")

    async def _extract_pdf_content_async(self, file_content: bytes) -> dict:
        """PDF 텍스트 및 자산(표, 이미지) 추출을 비동기로 실행"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._extract_pdf_content, file_content)

    def _extract_pdf_content(self, file_content: bytes) -> dict:
        """PDF에서 구조화된 텍스트와 핵심 자산(표, 이미지)을 추출"""
        logger.info("PDF 정밀 분석 시작: %d bytes", len(file_content))
        try:
            doc = fitz.open(stream=file_content, filetype="pdf")
        except Exception as e:
            logger.error("PDF 열기 실패: %s", traceback.format_exc())
            raise RuntimeError(f"PDF 파일을 열 수 없습니다: {e}") from e

        triggers = [
            'eligibility criteria', 'figure', 'flowchart', 'table', 
            'inclusion', 'exclusion', 'missing data', '24 hours before discharge',
            'study population', 'participant selection'
        ]

        text_parts = []
        assets = {"figures": [], "tables": []}
        max_pages = min(7, len(doc))
        
        for i in range(max_pages):
            page_no = i + 1
            page = doc[i]
            page_text = page.get_text("text")
            
            # 텍스트 섹션 추가
            text_parts.append(f"\n=== PAGE {page_no} ===")
            blocks = page.get_text("blocks")
            for b in blocks:
                block_text = b[4].strip()
                if block_text:
                    text_parts.append(f"[Page {page_no}, Block {b[5]}] {block_text}")

            # 키워드 기반 자산 추출 (비용 및 성능 최적화)
            lower_text = page_text.lower()
            if any(kw in lower_text for kw in triggers):
                logger.info(f"Page {page_no}에서 트리거 키워드 감지. 자산 추출 시작.")
                
                # 표 추출
                try:
                    tabs = page.find_tables()
                    for j, tab in enumerate(tabs.tables):
                        content = tab.extract()
                        if content and len(content) > 1: # 의미 있는 표만
                            assets["tables"].append({
                                "page": page_no,
                                "content": content
                            })
                except Exception as e:
                    logger.warning(f"Page {page_no} 표 추출 실패: {e}")

                # 이미지 추출
                try:
                    image_list = page.get_images(full=True)
                    for img_index, img in enumerate(image_list[:3]): # 페이지당 최대 3개
                        xref = img[0]
                        base_image = doc.extract_image(xref)
                        assets["figures"].append({
                            "page": page_no,
                            "image_bytes": base_image["image"],
                            "ext": base_image["ext"]
                        })
                except Exception as e:
                    logger.warning(f"Page {page_no} 이미지 추출 실패: {e}")

        doc.close()
        return {
            "full_text": "\n".join(text_parts).strip(),
            "assets": assets
        }

    def _canonicalize_text(self, text: str) -> str:
        """텍스트 정규화: 기호 제거 및 공백 통합을 통해 해시 견고성 확보"""
        # 1. 소문자화
        t = text.lower()
        # 2. [Page X, Block Y] 마커 및 페이지 구분자 제거
        t = re.sub(r'\[page \d+, block \d+\]', '', t)
        t = re.sub(r'=== page \d+ ===', '', t)
        # 3. 특수문자 제거 (알파벳, 숫자, 한글만 남김)
        t = re.sub(r'[^a-z0-9가-힣]', ' ', t)
        # 4. 공백 통합
        t = re.sub(r'\s+', ' ', t).strip()
        return t

    async def _describe_image(self, image_bytes: bytes, page_no: int) -> str:
        """Vision 모델을 사용하여 이미지를 요약 설명합니다."""
        try:
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini", # 비용 효율을 위해 mini 사용
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": f"이 이미지는 논문의 {page_no}페이지에서 추출되었습니다. 코호트 선정 기준(Flowchart, Inclusion/Exclusion)이나 환자 특성(Baseline)과 관련된 내용이 있다면 핵심만 요약해 주세요. 관련 없다면 'No clinical relevance'라고 답하세요."},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                            },
                        ],
                    }
                ],
                max_tokens=300,
            )
            summary = response.choices[0].message.content.strip()
            return summary if "No clinical relevance" not in summary else ""
        except Exception as e:
            logger.error(f"이미지 분석 실패: {e}")
            return ""

    async def _get_assets_summary(self, assets: dict) -> str:
        """추출된 표와 이미지 요약을 프롬프트용 텍스트로 변환"""
        summaries = []
        
        if assets.get("tables"):
            summaries.append("\n## EXTRACTED TABLES (RAW)")
            for t in assets["tables"]:
                table_str = "\n".join([" | ".join([str(cell or "").strip() for cell in row]) for row in t["content"]])
                summaries.append(f"### [Page {t['page']}] Table\n{table_str}")

        if assets.get("figures"):
            summaries.append("\n## EXTRACTED FIGURES (ANALYSIS SKIPPED)")
            # 그림 분석(LLM)은 속도를 위해 생략하고 개수만 표시
            summaries.append(f"Total {len(assets['figures'])} figures detected in this PDF.")
                    
        return "\n".join(summaries)

    def _load_rag_metadata(self, detected_vars: list = None) -> str:
        """RAG 프롬프트 강화를 위해 var/metadata의 핵심 정의들을 로드합니다."""
        meta_dir = Path("var/metadata")
        context_parts = []

        # 1. Derived Variables (SOFA, ROX, OASIS 등)
        try:
            dv_path = meta_dir / "derived_variables.json"
            if dv_path.exists():
                dv_data = json.loads(dv_path.read_text(encoding="utf-8"))
                defs = []
                for dv in dv_data.get("derived_variables", []):
                    derived_name = str(dv.get("derived_name") or "").strip()
                    if not derived_name:
                        continue
                    description = str(dv.get("description") or dv.get("definition") or "").strip()
                    sql_pattern = (
                        str(dv.get("sql_pattern") or "").strip()
                        or str((dv.get("oracle_template") or {}).get("strategy") or "").strip()
                        or "Complex logic"
                    )
                    defs.append(f"- {derived_name}: {description} (SQL: {sql_pattern})")
                context_parts.append("\n## DERIVED CLINICAL SCORES (USE THESE PATTERNS):\n" + "\n".join(defs))
        except Exception as e:
            logger.warning(f"Failed to load derived_variables.json: {e}")

        # 2. Comorbidity Specs (Charlson Index 등)
        try:
            cm_path = meta_dir / "cohort_comorbidity_specs.json"
            if cm_path.exists():
                cm_data = json.loads(cm_path.read_text(encoding="utf-8"))
                specs = []
                for cm in cm_data:
                    specs.append(f"- {cm['group_key']} ({cm['group_label']}): {cm.get('map_terms', [])}")
                context_parts.append("\n## COMORBIDITY MAPPING (ICD GROUPS):\n" + "\n".join(specs))
        except Exception as e:
            logger.warning(f"Failed to load cohort_comorbidity_specs.json: {e}")

        # 3. Schema Hints (Postprocess Rules)
        try:
            pp_path = meta_dir / "sql_postprocess_schema_hints.json"
            if pp_path.exists():
                pp_data = json.loads(pp_path.read_text(encoding="utf-8"))
                hints = []
                for table, cols in pp_data.get("tables", {}).items():
                    hints.append(f"- {table}: {', '.join(cols)}")
                context_parts.append("\n## KEY SCHEMA HINTS (TABLE COLUMNS):\n" + "\n".join(hints))
        except Exception as e:
            logger.warning(f"Failed to load sql_postprocess_schema_hints.json: {e}")

        # 4. [NEW] Detailed Variable Metadata (from mimic_rag_metadata_full.json)
        if detected_vars:
            try:
                # Docker path first, then local fallback (relative from this file)
                full_path_str = "/app/var/metadata/mimic_rag_metadata_full.json"
                if not os.path.exists(full_path_str):
                     full_path_str = os.path.join(os.path.dirname(__file__), "../../../../var/metadata/mimic_rag_metadata_full.json")
                
                full_path = Path(full_path_str)
                if full_path.exists():
                    full_data = json.loads(full_path.read_text(encoding="utf-8"))
                    var_hints = []
                    
                    # Create a lookup set for efficiency (normalize names)
                    detected_names = {_normalize_signal_name(v.get("signal_name", "")) for v in detected_vars}
                    
                    for item in full_data:
                        s_name = _normalize_signal_name(item.get("signal_name", ""))
                        # Check if this variable is relevant
                        if s_name in detected_names:
                            desc = item.get("description", "")
                            mapping = item.get("mapping", {})
                            hint = f"- **{item['signal_name']}**: {desc} (Table: {mapping.get('target_table')}, ItemID: {mapping.get('itemid')})"
                            var_hints.append(hint)
                            
                    if var_hints:
                        context_parts.append("\n## DETAILED VARIABLE SPECS (RELEVANT):\n" + "\n".join(var_hints))
            except Exception as e:
                logger.warning(f"Failed to load mimic_rag_metadata_full.json for RAG context: {e}")

        return "\n".join(context_parts)




    async def _extract_conditions(self, full_text: str, assets_summary: str = "", deterministic: bool = True) -> dict:
        """1단계: PDF 텍스트 및 자산 요약에서 코호트 선정 조건을 정규화된 JSON으로 추출"""
        rag_context = ""
        if not deterministic:
            rag_context = f"\n## REFERENCE COHORT EXAMPLES (RAG)\n{_load_reference_cohorts()}\n"

        prompt = f"""당신은 세계 최고의 임상 연구 정보 추출 전문가입니다.
제공된 논문 텍스트와 추출된 시각적 자산(표, 그림 요약)에서 '코호트 선정 조건(Eligibility/Inclusion/Exclusion)'을 누락 없이 정밀하게 추출하세요.

{rag_context}

[필수 요구사항]
1. **신속 정확한 추출**: 긴 설명보다는 JSON 필드를 정확히 채우는 데 집중하세요.
2. **시각적 정보 우선**: 텍스트와 표/그림의 수치가 다를 경우 표/그림(Flowchart)을 따르세요.
3. **핵심 요약**: `summary_ko`와 `criteria_summary_ko`는 각각 3문장 내외로 핵심만 요약하세요. (속도 최적화)
4. **논리적 분해**: 각 조건을 DB 필터링 로직 위주로 설명하세요.
5. **임상 변수 추출**: 주요 수치형 임상 변수를 찾아 `variables` 리스트에 담고, 반드시 단위(Unit)를 함께 명시하세요.

## 추출 대상 정보
### 1. TEXT CONTENT
{full_text}

### 2. VISUAL ASSETS (TABLES & FIGURES)
{assets_summary if assets_summary else "No additional assets extracted."}

[주의 사항 (Medical Guardrails)]
- **ICD 코드 변환**: 진단 조건이 나오면 질환명(Text)을 그대로 쓰지 말고, 반드시 상응하는 **ICD-9/10 코드(예: 850, 486)**를 찾아서 `variables`의 `codes` 파라미터에 배열로 담으세요. (예: `["850", "851"]`)
- **임상적 상식**: SOFA 점수는 패혈증 진단 시 통상 **2점 이상**을 의미합니다. 문맥 없이 0점이나 비상식적인 수치를 추출하지 마세요. 불명확하면 `is_mandatory: false`로 설정하세요.
- **원천 데이터 우선**: 복합 점수(SOFA, ROX)보다는 측정 가능한 원천 변수(혈압, 의식 수준, 호흡수) 추출에 집중하세요.

## 출력 JSON 스키마
{{
  "cohort_definition": {{
    "title": "논문 제목",
    "description": "Short description (English)",
    "summary_ko": "상세 연구 요약 (500자 이상)",
    "criteria_summary_ko": "상세 선정/제외 기준 요약 (500자 이상)",
    "extraction_details": {{
      "study_context": {{
        "data_source": "데이터 출처",
        "database_version": "버전",
        "study_period": "연구 기간",
        "setting": "설정"
      }},
      "cohort_criteria": {{
        "population": [
          {{
            "criterion": "선정/제외 기준 텍스트",
            "type": "inclusion|exclusion",
            "operational_definition": "DB 구현 로직 설명",
            "evidence": "[Source] 인용 원문 또는 요약",
            "evidence_source": {{
              "type": "text|figure|table",
              "page": "페이지 번호 (예: 1)"
            }}
          }}
        ],
        "index_unit": "patient|icu_stay",
        "first_stay_only": "Yes|No"
      }},
      "diagnosis_criteria": {{
        "coding_system": "ICD-10|ICD-9",
        "codes": ["코드 리스트"],
        "evidence": "[Source] 인용 원문",
        "evidence_source": {{
          "type": "text|figure|table",
          "page": "페이지 번호"
        }}
      }}
    }},
    "methods_summary": {{
      "structured_summary": {{
        "study_design_setting": "연구 설계",
        "data_source": "데이터 원천",
        "population_selection": "대상자 선정",
        "variables": "주요 변수",
        "outcomes": "결과 지표"
      }}
    }},
    "variables": [
      {{
        "signal_name": "변수명 (예: heart_rate)",
        "description": "변동성 설명 (예: Heart rate measured hourly during ICU stay)"
      }}
    ]
  }}
}}
"""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "의료 논문 데이터 추출 전문가입니다. 반드시 유효한 JSON만 반환하세요."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0,
            seed=42
        )
        return json.loads(response.choices[0].message.content)

    async def _generate_sql_from_conditions(self, conditions_json: dict, relax_mode: bool = False, deterministic: bool = True) -> dict:
        """2단계: 추출된 코호트 조건(JSON)을 바탕으로 'Intent JSON'을 생성하고 SQL로 컴파일"""
        derived_meta = _load_metadata_json(_DERIVED_VAR_PATH, _DERIVED_VAR_LOCAL)
        derived_names = [v.get("derived_name") for v in derived_meta.get("derived_variables", [])]
        
        prompt = f"""당신은 MIMIC-IV 데이터베이스 전문가입니다.
제공된 코호트 정의를 바탕으로, SQL을 직접 쓰지 말고 아래 규칙에 따라 'Cohort Intent JSON'을 생성하세요.

## 규칙
1. **시그널 매핑 강제 (Guardrail)**: 너의 상식으로 itemid를 추측하지 마세요. 반드시 제공된 SIGNAL_MAP의 키워드만 사용하세요.
2. **타입 엄격 적용**: Vital Signs(HR, SBP, SpO2 등)은 `vital` 타입을, Lab 결과는 `lab` 타입을 사용하세요.
3. **파생 지표 토큰화**: SOFA, ROX 등 복잡한 지표는 `derived` 타입의 `name` 파라미터에 표준 토큰(sofa, rox, oasis)을 입력하세요.
4. **시간창(Window) 엄격 적용**: 날짜 계산을 직접 하지 말고, `window` 필드에 지정된 템플릿 이름(`icu_first_24h` 등)을 정확히 기입하세요.
5. **제외 로직 명시 (Exclusion)**: 제외 기준(Exclusion)에 해당하는 단계는 `"is_exclusion": true` 속성을 반드시 부여하세요.
6. **필수/권장 여부 (Relaxation)**: 연구의 핵심이 아닌 보조적 조건(예: 특정 Lab 수치 범위 등)은 `"is_mandatory": false`로 설정하여, 0명일 때 자동 완화될 수 있게 하세요.
7. **논리**: 신호들은 기본적으로 AND로 결합됩니다.
8. **ICU 체류시간 규칙 (중요)**:
   - "ICU stay < 24h 제외" 문구는 반드시 `type: "icu_stay"`, `params: {{"min_los": 1}}`, `is_exclusion: true`로 표현하세요.
   - `min_los`는 0보다 큰 값으로 넣으세요(일 단위, 24h=1).

## 출력 JSON 형식
{{
  "steps": [
    {{ 
      "name": "단계 이름 (영어)", 
      "type": "age|gender|diagnosis|lab|icu_stay|vital|derived", 
      "params": {{ ... }},
      "window": "icu_first_24h|admission_first_24h|icu_discharge_last_24h",
      "is_exclusion": true/false,
      "is_mandatory": true/false
    }}
  ]
}}

COHORT JSON:
{json.dumps(conditions_json, ensure_ascii=False, indent=2)}
"""
        # 속도 최적화를 위해 Intent 생성 단계는 빠르고 정형화된 gpt-4o-mini 모델 사용
        mini_model = "gpt-4o-mini"
        response = await self.client.chat.completions.create(
            model=mini_model,
            messages=[
                {"role": "system", "content": "MIMIC-IV 코호트 설계 전문가입니다. 인텐트 기반 JSON만 반환하세요."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0,
            seed=42
        )
        intent = json.loads(response.choices[0].message.content)
        
        # 0명 발생 시 완화 로직 (Relax Mode)
        if relax_mode:
            # 필수 조건만 남기거나 범위를 넓히는 로직 (현재는 LLM 가이드에 is_optional 위임)
            logger.info("완화 모드 활성화됨: 선택적 조건 필터링 검토")

        return self.compile_oracle_sql(intent)

    def _get_best_join_key(self, s_type, s_params) -> str:
        """가이드라인 3: 테이블 성격에 맞는 최적의 조인 키 선택"""
        # [Hospital Level Tables] -> hadm_id 사용
        # lab, diagnosis, prescription, microbiology, inputevents(일부), admissions
        hospital_tables = ["lab", "diagnosis", "prescription", "microbiology", "admissions", "procedures"]
        
        if s_type in hospital_tables:
            return "hadm_id"
            
        # [Stay Level Tables] -> stay_id 사용 (기본값)
        # vital, derived, icu_stay, chartevents, outputevents
        return "stay_id"

    def _sanitize_step_slug(self, value: Any) -> str:
        slug = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug or "unknown"

    def _extract_select_keys(self, sql: str) -> set[str]:
        sql_text = str(sql or "")
        m = re.search(r"select\s+(.*?)\s+from\b", sql_text, flags=re.IGNORECASE | re.DOTALL)
        select_part = m.group(1).lower() if m else sql_text.lower()
        if "*" in select_part:
            return {"subject_id", "hadm_id", "stay_id"}
        available: set[str] = set()
        for key in ("subject_id", "hadm_id", "stay_id"):
            if re.search(rf"\b{key}\b", select_part):
                available.add(key)
        return available

    def _resolve_join_key(self, preferred_key: str, signal_sql: str) -> str | None:
        available = self._extract_select_keys(signal_sql)
        if not available:
            return None
        if preferred_key in available:
            return preferred_key
        for fallback in ("hadm_id", "stay_id", "subject_id"):
            if fallback in available:
                return fallback
        return next(iter(available))

    def compile_oracle_sql(self, intent: dict) -> dict:
        """Intent JSON을 바탕으로 실제 Oracle SQL(MIMIC-IV)을 조립합니다. (CTE 단계 누적)"""
        steps = intent.get("steps", [])
        ctes = []
        step_labels = []
        step_refs = []
        
        # 가이드라인 2 & 3: First-Stay 원칙 및 최소 재원 시간(24h) 필터 적용
        ctes.append("""population AS (
    SELECT subject_id, hadm_id, stay_id, intime, outtime, admittime
    FROM (
        SELECT a.subject_id, a.hadm_id, i.stay_id, i.intime, i.outtime, a.admittime,
               ROW_NUMBER() OVER (PARTITION BY a.subject_id ORDER BY i.intime) as rn
        FROM SSO.ADMISSIONS a
        JOIN SSO.ICUSTAYS i ON a.hadm_id = i.hadm_id
        WHERE (CAST(i.outtime AS DATE) - CAST(i.intime AS DATE)) >= 1
    )
    WHERE rn = 1
)""")
        step_labels.append("Initial Population (First ICU Stay & >24h)")
        step_refs.append("population")
        
        current_prev = "population"
        
        for i, step in enumerate(steps):
            s_type = step.get("type")
            s_params = step.get("params", {})
            window_key = step.get("window")
            s_name = f"step_{i+1}_{self._sanitize_step_slug(s_type)}"
            is_exclusion = bool(step.get("is_exclusion", False))
            
            # Safe SQL Formatting with defaults (fixed KeyError by adding 'label')
            defaults = {"min": 0, "max": 150, "operator": "=", "value": 0, "min_los": 0, "drug": "", "gender": "all", "codes": "''", "label": ""}
            safe_params = {**defaults, **s_params}
            
            # 가드레일: Vital은 ChartEvents 우선
            if s_type == "vital":
                v_signal = s_params.get("signal")
                if v_signal in self.signal_map:
                    raw_sql = self.signal_map[v_signal]
                    signal_sql = raw_sql.format(**safe_params)
                else:
                    logger.warning(f"Unknown vital signal: {v_signal}")
                    continue
            elif s_type == "derived":
                d_name = s_params.get("name", "").lower()
                if d_name in self.signal_map:
                    signal_sql = self.signal_map[d_name].format(**safe_params)
                else:
                    # 유효하지 않은 derived 변수의 경우, SSO 스키마를 붙여 admissions 조인으로 우회 (에러 방지)
                    logger.warning(f"Unknown derived signal: {d_name}. Falling back to admissions.")
                    # Fallback to ICUSTAYS for derived scores to prevent ORA-00942 (Missing Table)
                    signal_sql = "SELECT stay_id, intime as charttime FROM SSO.ICUSTAYS WHERE stay_id IS NOT NULL"
            elif s_type in self.signal_map:
                raw_sql = self.signal_map[s_type]
                if s_type == "icu_stay":
                    min_los_raw = s_params.get("min_los", safe_params.get("min_los", 0))
                    try:
                        min_los = float(str(min_los_raw).strip())
                    except (TypeError, ValueError):
                        min_los = 0.0

                    # Exclusion with los<=0 degenerates to "exclude everyone".
                    # Use a safe default (24h == 1 day) when threshold is missing/invalid.
                    if is_exclusion and min_los <= 0:
                        min_los = 1.0
                        logger.warning(
                            "Step '%s': exclusion icu_stay min_los is invalid (%s). Defaulting to 1 day.",
                            s_name,
                            min_los_raw,
                        )

                    if is_exclusion:
                        signal_sql = (
                            "SELECT stay_id, hadm_id, intime as charttime "
                            f"FROM SSO.ICUSTAYS WHERE los < {min_los:g}"
                        )
                    else:
                        safe_params["min_los"] = min_los
                        signal_sql = raw_sql.format(**safe_params)
                elif s_type == "diagnosis":
                    raw_codes = s_params.get("codes", [])
                    if isinstance(raw_codes, list):
                        code_candidates = raw_codes
                    else:
                        raw_text = str(raw_codes or "")
                        code_candidates = raw_text.split(",") if "," in raw_text else [raw_text]

                    cleaned_codes: list[str] = []
                    for code in code_candidates:
                        normalized = re.sub(r"[^A-Za-z0-9]+", "", str(code or "")).upper().strip()
                        if normalized and normalized not in cleaned_codes:
                            cleaned_codes.append(normalized)

                    if not cleaned_codes:
                        logger.warning(
                            "Step '%s': diagnosis codes are empty. Skipping step to avoid invalid IN () SQL.",
                            s_name,
                        )
                        continue

                    code_val = ", ".join([f"'{code}'" for code in cleaned_codes])
                    signal_sql = raw_sql.format(codes=code_val)
                else:
                    signal_sql = raw_sql.format(**safe_params)
            else:
                continue



            # 가이드라인 3: 동적 조인 키 적용 및 무결성 검사
            preferred_key = self._get_best_join_key(s_type, s_params)
            join_key = self._resolve_join_key(preferred_key, signal_sql)
            if not join_key:
                logger.warning(
                    "Step '%s': no identifier key (subject_id/hadm_id/stay_id) in SELECT list. Skipping step.",
                    s_name,
                )
                continue
            if join_key != preferred_key:
                logger.info(
                    "Step '%s': join key adjusted from '%s' to '%s' based on projected columns.",
                    s_name,
                    preferred_key,
                    join_key,
                )

            # 제외(Exclusion) 여부
            operator_exists = "NOT EXISTS" if is_exclusion else "EXISTS"

            # 가이드라인 2 & 3: EXISTS 기반의 정교한 시간창 비교 및 조인
            condition_parts = [f"s.{join_key} = p.{join_key}"]
            
            # 시간 정보(charttime)가 있는 경우에만 윈도우 필터 적용 (가드 로직)
            if window_key and window_key in WINDOW_TEMPLATES:
                if "charttime" in signal_sql.lower():
                    condition_parts.append(WINDOW_TEMPLATES[window_key])
                else:
                    logger.info(f"Step '{s_name}' skipped window filter: No 'charttime' in SQL.")
            
            where_clause = " AND ".join(condition_parts)

            cte_query = f"""SELECT p.* 
FROM {current_prev} p
WHERE {operator_exists} (
    SELECT 1 FROM ({signal_sql}) s
    WHERE {where_clause}
)"""
            ctes.append(f"{s_name} AS ({cte_query})")
            step_labels.append(step.get("name") or s_name)
            step_refs.append(s_name)
            current_prev = s_name

        # 최종 쿼리 조립
        cte_str = ",\n".join(ctes)
        
        cohort_sql = f"WITH {cte_str}\nSELECT * FROM {current_prev} FETCH FIRST 100 ROWS ONLY"
        count_sql = f"WITH {cte_str}\nSELECT count(*) as patient_count FROM {current_prev}"
        
        # Funnel SQL (Step Counts)
        debug_parts = []
        for label, cte_ref in zip(step_labels, step_refs):
            safe_label = str(label or "").replace("'", "''")
            debug_parts.append(f"SELECT '{safe_label}' as step_name, count(*) as cnt FROM {cte_ref}")
        debug_parts.append(f"SELECT 'Final Cohort' as step_name, count(*) as cnt FROM {current_prev}")
        
        debug_count_sql = f"WITH {cte_str}\n" + " UNION ALL ".join(debug_parts)

        return {
            "cohort_sql": cohort_sql,
            "count_sql": count_sql,
            "debug_count_sql": debug_count_sql
        }

    def _map_clinical_variables(self, extracted_vars: list) -> list:
        """추출된 임상 변수들을 self.signal_metadata와 대조하여 실제 DB 매칭 정보 추가"""
        mapped_vars = []
        derived_meta = _load_metadata_json(_DERIVED_VAR_PATH, _DERIVED_VAR_LOCAL)
        derived_vars = derived_meta.get("derived_variables", [])
        
        for v in extracted_vars:
            raw_signal_name = str(v.get("signal_name", ""))
            signal_name = _normalize_signal_name(raw_signal_name)
            if not signal_name:
                continue
            # 1. self.signal_metadata 직접 매핑 확인
            mapping = self.signal_metadata.get(signal_name)
            
            # 2. 파생 변수(Derived Variables) 메타데이터 확인
            if not mapping:
                matched_d = next(
                    (
                        d for d in derived_vars
                        if _normalize_signal_name(d.get("derived_name", "")) == signal_name
                        or signal_name in {_normalize_signal_name(a) for a in d.get("aliases", [])}
                    ),
                    None,
                )
                if matched_d:
                    mapping = {"target_table": "DERIVED", "itemid": f"Score: {matched_d['derived_name']}"}
            
            # 3. 근접 매칭 (퍼지) - 단순 구현
            if not mapping:
                matches = get_close_matches(signal_name, self.signal_metadata.keys(), n=1, cutoff=0.7)
                if matches:
                    mapping = self.signal_metadata[matches[0]]

            if mapping:
                v["mapping"] = {
                    "target_table": str(mapping.get("target_table") or "Unknown"),
                    "itemid": str(mapping.get("itemid") or "N/A"),
                }
            else:
                v["mapping"] = {"target_table": "Unknown", "itemid": "N/A"}
            mapped_vars.append(v)

        def _sort_key(item: dict[str, Any]) -> tuple[int, str, str, str]:
            mapping = item.get("mapping") if isinstance(item.get("mapping"), dict) else {}
            table_name = str(mapping.get("target_table") or "Unknown").strip()
            signal_name = str(item.get("signal_name") or "").strip()
            item_id = str(mapping.get("itemid") or "N/A").strip()
            unknown_rank = 1 if table_name.lower() in {"", "unknown", "n/a"} else 0
            return (unknown_rank, table_name.lower(), signal_name.lower(), item_id.lower())

        mapped_vars.sort(key=_sort_key)

        logger.info(f"Clinical variables mapped: {len(mapped_vars)} items found.")
        return mapped_vars

    def _build_features(self, mapped_variables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """프론트엔드 배너/카드용 features 배열 생성 및 정렬"""
        features: list[dict[str, Any]] = []
        for item in mapped_variables:
            if not isinstance(item, dict):
                continue

            signal_name = str(item.get("signal_name") or "").strip()
            if not signal_name:
                continue

            description = str(item.get("description") or "").strip()
            mapping = item.get("mapping") if isinstance(item.get("mapping"), dict) else {}
            table_name = str(mapping.get("target_table") or "Unknown").strip() or "Unknown"
            item_id = str(mapping.get("itemid") or "N/A").strip() or "N/A"
            features.append({
                "name": signal_name,
                "description": description,
                "table_name": table_name,
                "itemid": item_id,
            })

        features.sort(
            key=lambda row: (
                1 if str(row.get("table_name") or "").lower() in {"", "unknown", "n/a"} else 0,
                str(row.get("table_name") or "").lower(),
                str(row.get("name") or "").lower(),
                str(row.get("itemid") or "").lower(),
            )
        )
        return features


    def _calculate_prompt_hash(self, relax_mode: bool, deterministic: bool) -> str:
        """프롬프트 지시문의 해시를 계산하여 로직 변경 여부를 추적"""
        # 실제 텍스트가 아닌 '지침(Instruction)' 부분만 취합하여 해싱
        instructions = [
            "당신은 세계 최고의 임상 연구 정보 추출 전문가입니다.",
            "3. 엄격한 준수: 논문에 명시된 조건을 절대 변경하지 마세요." if not relax_mode else "3. 결과 보장: 조건을 유연하게 적용하세요.",
            "Deterministic" if deterministic else "RAG-enabled",
            _load_schema_for_prompt()[:100] # 스키마 일부 포함
        ]
        return hashlib.sha256("".join(instructions).encode("utf-8")).hexdigest()[:12]

    async def verify_sql_integrity(self, sql: str) -> tuple[bool, str]:
        """가이드라인 5: schema_catalog 기반 사후 검증"""
        catalog = _load_metadata_json(_SCHEMA_CATALOG_PATH, _SCHEMA_CATALOG_LOCAL)
        if not catalog:
            return True, "No catalog found for verification"

        tables_meta = catalog.get("tables", {})
        
        # 간단한 정규식으로 사용하는 테이블명 추출 (SSO.TABLE_NAME)
        found_tables = re.findall(r'SSO\.([A-Za-z0-9_]+)', sql.upper())
        for tname in set(found_tables):
            if tname not in tables_meta:
                return False, f"Table '{tname}' does not exist in schema_catalog."
            
            # 해당 테이블의 컬럼 존재 확인 (기본적인 것만 체크)
            # 여기서는 쿼리 전체에서 컬럼명을 추출하기 어려우므로 테이블 존재 여부를 우선으로 함
            
        return True, "Integrity check passed"

    async def analyze_and_generate_sql(
        self,
        file_content: bytes,
        *,
        filename: str | None = None,
        user_id: str | None = None,
        relax_mode: bool = False,
        deterministic: bool = True,
        reuse_existing: bool = True,
    ) -> dict:
        """
        PDF 분석을 2단계(추출 -> SQL 생성)로 수행하고 결과 집계.
        reuse_existing=True이면 동일 환경(Version/Mode/Hash)의 캐시를 즉시 반환합니다.
        False이면 캐시를 무시하고 강제로 재생성하여 업데이트합니다.
        """
        pipeline_version = "v55"
        file_hash = hashlib.sha256(file_content).hexdigest()
        model_name = self.model
        prompt_hash = self._calculate_prompt_hash(relax_mode, deterministic)
        
        store = get_state_store()
        
        # 1-0. 최종 확정 데이터(pdf_confirmed_cohorts) 확인 - 최우선 순위
        if store:
            from app.services.runtime.state_store import AppStateStore
            confirmed_store = AppStateStore(collection_name="pdf_confirmed_cohorts")
            confirmed = confirmed_store.get(file_hash)
            if confirmed and confirmed.get("status") == "confirmed":
                logger.info(f"최종 확정된 코호트 데이터 발견 및 반환 (File Hash: {file_hash})")
                confirmed["pdf_hash"] = file_hash # 보장
                return confirmed

        # PK 생성: pdf_hash + relax_mode + deterministic + pipeline_version
        cache_key = f"pdf_analysis::{pipeline_version}::{file_hash}::{relax_mode}::{deterministic}"
        
        # 1-1. Primary Cache 확인 (reuse_existing=True일 때만)
        if store and reuse_existing:
            cached = store.get(cache_key)
            if cached:
                logger.info(f"PDF 분석 결과 임시 캐시 적중 (File Hash: {file_hash}, Version: {pipeline_version})")
                cached["pdf_hash"] = file_hash # 보장
                return cached

        # 0 & 1-3. 병렬 처리 시작: 텍스트 추출과 시각적 자산 요약을 동시에 대기
        logger.info("병렬 작업 시작: 텍스트 추출 및 자산 요약 대기")
        import asyncio
        
        # 0. 텍스트 및 자산 기본 추출 (File IO/Parsing)
        extracted_task = self._extract_pdf_content_async(file_content)
        
        # 1-2. Secondary Cache 확인 (Canonical Hash 기반 - reuse_existing=True일 때만)
        # 이 부분은 extracted_task가 완료되어 full_text를 얻어야 canonical_text를 만들 수 있으므로,
        # 병렬 처리 후 또는 extracted_task 완료 직후에 수행해야 합니다.
        # 현재 구조에서는 extracted_task 완료 후 진행하는 것이 자연스럽습니다.
        
        extracted = await extracted_task # Wait for extraction to complete
        full_text = extracted["full_text"]
        assets = extracted["assets"]
        
        canonical_text = self._canonicalize_text(full_text)
        canonical_hash = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()

        # 1-2. Secondary Cache 확인 (Canonical Hash 기반 - reuse_existing=True일 때만)
        if store and reuse_existing:
            matched = store.find_one({
                "value.canonical_hash": canonical_hash,
                "value.relax_mode": relax_mode,
                "value.deterministic": deterministic,
                "value.pipeline_version": pipeline_version
            })
            if matched:
                logger.info(f"PDF 분석 결과 Canonical 캐시 적중 (Canonical Hash: {canonical_hash})")
                result = matched.get("value", {})
                result["pdf_hash"] = file_hash
                store.set(cache_key, result)
                return result

        # 1-3. 시각적 자산 요약 생성 (비동기 처리)
        logger.info("자산(표/그림) 요약 생성 시작")
        assets_summary_task = self._get_assets_summary(assets)
        
        if not reuse_existing:
            logger.info(f"PDF 신규 분석 강제 실행 (reuse_existing=False, File Hash: {file_hash})")
            
        # Wait for assets_summary_task to complete
        try:
            assets_summary = await assets_summary_task
        except Exception as e:
            logger.warning(f"자산 요약 생성 실패 (Text-only fallback 실행): {e}")
            assets_summary = "" # 실패 시 빈 문자열로 유지하여 텍스트 분석으로 진행

        # 1단계: 코호트 조건 추출
        logger.info(f"1단계: 코호트 조건 추출 시작 (Deterministic: {deterministic})")
        conditions = await self._extract_conditions(full_text, assets_summary=assets_summary, deterministic=deterministic)
        
        # 2단계: SQL 생성
        logger.info(f"2단계: SQL 생성 시작 (Relax Mode: {relax_mode}, Deterministic: {deterministic})")
        sql_result = await self._generate_sql_from_conditions(conditions, relax_mode=relax_mode, deterministic=deterministic)
        
        # 3. SQL 정제 및 최적화 힌트 추가
        for key in ["cohort_sql", "count_sql", "debug_count_sql"]:
            if key in sql_result:
                sql = str(sql_result[key]).strip().rstrip(";").replace("`", "")
                # 대량 데이터 처리를 위한 Parallel 힌트 강제 삽입 (Oracle 최적화)
                if "SELECT" in sql and "/*+" not in sql:
                    sql = sql.replace("SELECT", "SELECT /*+ PARALLEL(4) */", 1)
                sql = re.sub(r'"([A-Za-z_]+)"', r'\1', sql)
                sql_result[key] = sql

        # 3.5 SQL Integrity Verification (Guideline 5)
        if "cohort_sql" in sql_result:
            is_valid, msg = await self.verify_sql_integrity(sql_result["cohort_sql"])
            if not is_valid:
                logger.warning(f"SQL Integrity Warning: {msg}")
                # 에러 메시지를 결과에 포함시켜 UI에서 인지 가능하게 함
                if not sql_result.get("warning"):
                    sql_result["warning"] = []
                sql_result["warning"].append(msg)

        # 4. DB 실행 (Auto-Relaxation Loop applied)
        logger.info("3단계: SQL 실행 및 결과 집계 (Auto-Relaxation)")
        db_result = {
            "columns": [],
            "rows": [],
            "step_counts": [],
            "row_count": 0,
            "total_count": None,
            "error": None,
            "warning": sql_result.get("warning", []),
        }
        
        # Max retries for relaxation
        max_retries = 3
        current_intent = conditions # Actually we need intent json, but it is inside _generate_sql. 
        # Refactoring: _generate_sql_from_conditions returns sql_dict, but we need intent object to modify steps.
        # For v43 hotfix, we cannot modify intent structure easily here without larger refactor.
        # Instead, we will simulate relaxation by checking row count.
        
        # 1st Attempt
        try:
            main_res = await asyncio.to_thread(execute_sql, sql_result["cohort_sql"])
            
            # 0명인 경우 & Relax Mode가 아닌 경우에도, 시스템적으로 자동 완화 시도
            if (not main_res.get("rows")) and (len(main_res.get("rows", [])) == 0):
                logger.info("결과 0건 감지: Auto-Relaxation 시도")
                # 여기서 Intent를 다시 생성하는 것은 비효율적이므로, 
                # 추출된 intent 내에서 is_mandatory=False인 스텝을 제외한 SQL을 다시 compile하는 것이 이상적.
                # 하지만 현재 구조 제한상, 사용자에게 "조건을 완화해보세요" 경고를 주는 것으로 1차 대응.
                _append_warning_once(db_result, "검색된 환자가 0명입니다. '완화 모드'를 켜거나 일부 조건을 제외해 보세요.")
            
            if "error" in main_res:
                db_result["error"] = main_res["error"]
            else:
                db_result["columns"] = main_res.get("columns", [])
                db_result["rows"] = main_res.get("rows", [])[:100]
                db_result["row_count"] = int(main_res.get("row_count") or 0)
                db_result["total_count"] = main_res.get("total_count")

            # 단계별 카운트 조회
            debug_res = await asyncio.to_thread(execute_sql, sql_result["debug_count_sql"])
            if "error" not in debug_res:
                db_result["step_counts"] = debug_res.get("rows", [])
        except Exception as e:
            logger.error(f"DB 실행 중 오류: {e}")
            db_result["error"] = str(e)

        # === 4.5 AI RAG 고도화 (Automatic) ===
        logger.info("4.5단계: AI RAG 쿼리 고도화 자동 실행")
        try:
            summary_ko = conditions.get("cohort_definition", {}).get("summary_ko", "")
            criteria_summary = conditions.get("cohort_definition", {}).get("criteria_summary_ko", "")
            
            # Load mapped variables from Step 1 (Prioritized for RAG context)
            mapped_vars = self._map_clinical_variables(conditions.get("cohort_definition", {}).get("variables", []))
            mapped_str = "\n".join([f"- {v['signal_name']}: {v.get('description', '')} (Mapped: {v.get('mapping', {}).get('target_table')} / {v.get('mapping', {}).get('itemid')})" for v in mapped_vars])

            # Load rich metadata for RAG context (Using detected mapped_vars)
            rag_hints = self._load_rag_metadata(mapped_vars)

            question = (
                f"**[ESSENTIAL SQL RULES]**\n"
                f"1. **ID Propagation (CRITICAL)**: In every CTE, SELECT ALL identifiers (`subject_id`, `hadm_id`, `stay_id`). Even if unused, carry them forward.\n"
                f"2. **Strict Join-Key Mapping**:\n"
                f"   - HOSPITAL Tables (ADMISSIONS, LAB, DIAGNOSIS): Use `hadm_id`.\n"
                f"   - ICU Tables (ICUSTAY, CHART): Use `stay_id`.\n"
                f"   - To bridge, ensure CTEs have both IDs.\n"
                f"3. **Research Guidelines (Apply ONLY if consistent with summary)**:\n"
                f"   - **First-Stay**: Apply `rn=1` filtering ONLY IF the text explicitly mentions 'first admission/stay'. Otherwise, allow all stays.\n"
                f"   - **Minimal Stay**: Apply `los >= 1` (24h) ONLY IF text mentions time criteria.\n"
                f"   - **Age Filter**: Apply `anchor_age` limits based on text. If vague, assume adult (>=18).\n"
                f"4. **Syntax Rules**:\n"
                f"   - `anchor_age` is in `PATIENTS` (p.anchor_age), `hospital_expire_flag` is in `ADMISSIONS`.\n"
                f"   - Use `NOT EXISTS` for exclusions.\n"
                f"   - **Diagnosis Codes**: ALWAYS use `LIKE '123%'` or `IN` for broader matching. Do not use strict `=` for ICD codes.\n"
                f"   - **Window Functions**: MUST use `OVER (PARTITION BY ... ORDER BY ...)`.\n"
                f"     - CORRECT: `ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY charttime ASC)`\n"
                f"     - WRONG: `ROW_NUMBER() OVER (ORDER BY charttime)` (Missing PARTITION BY in strict mode causes ORA-00924)\n\n"
                f"5. **Output Shape (CRITICAL)**:\n"
                f"   - Final SELECT must return patient-level rows including `subject_id`, `hadm_id`, `stay_id`.\n"
                f"   - Do NOT return aggregate-only metrics (COUNT/AVG/RATE only).\n\n"
                f"## REFERENCE KNOWLEDGE (METADATA):\n{rag_hints}\n\n"
                f"## DETECTED CLINICAL SIGNALS (FROM PDF):\n{mapped_str}\n\n"
                f"연구 요약: {summary_ko}\n"
                f"선정 및 제외 기준: {criteria_summary}\n\n"
                f"위 연구 디자인을 SQL 쿼리로 변환해줘. "
                f"MIMIC-IV 스키마를 사용하고, 단계별로 환자가 필터링되는 Funnel 형태의 CTE 구조를 만들어줘."
            )
            
            import asyncio
            loop = asyncio.get_running_loop()
            rag_payload = await loop.run_in_executor(
                None, 
                lambda: run_oneshot(question, translate=False, rag_multi=True, enable_clarification=False)
            )
            
            rag_final_sql = ""
            if "final" in rag_payload:
                rag_final_sql = rag_payload["final"].get("final_sql", "")
            elif "draft" in rag_payload:
                rag_final_sql = rag_payload["draft"].get("final_sql", "")
                
            if rag_final_sql:
                logger.info("RAG 고도화 SQL 생성 성공")
                candidate_sql = rag_final_sql
                candidate_count_sql = f"SELECT COUNT(*) FROM ({candidate_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"

                # DB 재실행 (고도화된 쿼리로)
                rag_db_res = await asyncio.to_thread(execute_sql, candidate_sql)
                
                # [Error Recovery Logic] If RAG SQL fails, try to auto-repair
                if "error" in rag_db_res:
                    logger.warning(f"RAG SQL Execution Error: {rag_db_res['error']}. Attempting auto-repair...")
                    fixed_sql = await self.fix_sql_with_error_async(candidate_sql, rag_db_res["error"])
                    if fixed_sql:
                        logger.info(f"Auto-Repaired SQL: {fixed_sql[:100]}...")
                        # 재실행
                        retry_res = await asyncio.to_thread(execute_sql, fixed_sql)
                        if "error" not in retry_res:
                            candidate_sql = fixed_sql
                            candidate_count_sql = f"SELECT COUNT(*) FROM ({fixed_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                            rag_db_res = retry_res
                        else:
                            logger.error(f"Repair Failed: {retry_res['error']}")
                            
                # [Zero-Result Relaxation Logic]
                # If result is 0 rows (and no error), user likely wants broader criteria.
                if "error" not in rag_db_res and "rows" in rag_db_res and len(rag_db_res["rows"]) == 0:
                     logger.info("RAG SQL returned 0 rows. Attempting relaxation (removing strict filters)...")
                     relax_prompt = (
                         f"The previous SQL executed successfully but returned 0 rows. This is too strict.\n"
                         f"Please RELAX the constraints:\n"
                         f"1. Remove `rn=1` (First-Stay) filter.\n"
                         f"2. Use broader ICD code matching (e.g. `LIKE '850%'` instead of specific codes).\n"
                         f"3. Remove non-essential lab value filters.\n"
                         f"4. Keep ID propagation and Join Keys correct.\n"
                         f"Rewrite the SQL to be more inclusive."
                     )
                     # Reuse repair function for relaxation as it handles SQL generation
                     relaxed_sql = await self.fix_sql_with_error_async(candidate_sql, relax_prompt)
                     if relaxed_sql:
                         logger.info("Executing Relaxed SQL...")
                         relaxed_res = await asyncio.to_thread(execute_sql, relaxed_sql)
                         if "error" not in relaxed_res and len(relaxed_res.get("rows", [])) > 0:
                             candidate_sql = relaxed_sql
                             candidate_count_sql = f"SELECT COUNT(*) FROM ({relaxed_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                             rag_db_res = relaxed_res
                         elif "rows" in relaxed_res and len(relaxed_res["rows"]) == 0:
                             logger.warning("Relaxed SQL also returned 0 rows.")


                if "error" in rag_db_res:
                    logger.warning("RAG SQL 실행 오류로 인해 기본 코호트 SQL 결과를 유지합니다.")
                    _append_warning_once(db_result, "RAG SQL 실행 오류로 기본 코호트 결과를 유지했습니다.")
                else:
                    rag_columns = _normalize_result_columns(rag_db_res.get("columns", []))

                    if not _has_identifier_columns(rag_columns):
                        logger.warning("RAG SQL이 집계형 결과를 반환했습니다. 환자 단위 출력으로 자동 재작성 시도.")
                        mapped_signal_names = sorted({
                            _normalize_signal_name(v.get("signal_name"))
                            for v in mapped_vars
                            if isinstance(v, dict) and str(v.get("signal_name") or "").strip()
                        })
                        mapped_signal_names = [name for name in mapped_signal_names if name]
                        rewrite_prompt = (
                            "The previous SQL returned aggregate-only metrics without patient identifiers.\n"
                            "Rewrite it to patient-level cohort output.\n"
                            "Requirements:\n"
                            "1. Include subject_id, hadm_id, stay_id in final SELECT.\n"
                            "2. Do not return COUNT/AVG-only aggregate output.\n"
                            "3. Include mapped clinical variable columns when possible.\n"
                            "4. Keep Oracle-compatible SQL and use FETCH FIRST 200 ROWS ONLY."
                        )
                        if mapped_signal_names:
                            rewrite_prompt += f"\nMapped clinical variables: {', '.join(mapped_signal_names)}"

                        row_level_sql = await self.fix_sql_with_error_async(candidate_sql, rewrite_prompt)
                        if row_level_sql:
                            row_level_res = await asyncio.to_thread(execute_sql, row_level_sql)
                            row_level_columns = _normalize_result_columns(row_level_res.get("columns", []))
                            if "error" not in row_level_res and _has_identifier_columns(row_level_columns):
                                logger.info("환자 단위 SQL 재작성 성공. 결과를 환자 행 기반으로 교체합니다.")
                                candidate_sql = row_level_sql
                                candidate_count_sql = f"SELECT COUNT(*) FROM ({row_level_sql.replace('FETCH FIRST 100 ROWS ONLY', '')})"
                                rag_db_res = row_level_res
                                rag_columns = row_level_columns

                        if not _has_identifier_columns(rag_columns):
                            logger.warning("환자 식별자 컬럼이 없는 집계형 SQL로 판단되어 기본 코호트 결과를 유지합니다.")
                            _append_warning_once(db_result, "집계형 SQL이 생성되어 환자 단위 기본 코호트 결과를 유지했습니다.")
                            rag_db_res = {}

                    if rag_db_res:
                        sql_result["cohort_sql"] = candidate_sql
                        sql_result["count_sql"] = candidate_count_sql
                        db_result["error"] = None
                        db_result["columns"] = rag_db_res.get("columns", [])
                        db_result["rows"] = rag_db_res.get("rows", [])[:100]
                        db_result["row_count"] = int(rag_db_res.get("row_count") or 0)
                        db_result["total_count"] = rag_db_res.get("total_count")
                        # step_counts는 파싱하기 어려울 수 있으므로 비움
                        db_result["step_counts"] = []
            else:
                logger.warning("RAG 고도화 SQL 생성 실패 (빈 결과)")
        except Exception as e:
            logger.error(f"RAG 고도화 중 오류 발생 (기존 템플릿 결과 유지): {e}")

        # 5. 임상 변수 리스트 매핑 강화
        extracted_vars = (conditions.get("cohort_definition") or {}).get("variables") or []
        mapped_variables = self._map_clinical_variables(extracted_vars)
        features = self._build_features(mapped_variables)
        
        # 6. 프론트엔드용 최종 스키마 조립 (메타데이터 포함)
        final_response = {
            "pdf_hash": file_hash,
            "canonical_hash": canonical_hash,
            "filename": str(filename or "").strip() or "uploaded.pdf",
            "user_id": str(user_id or "").strip() or None,
            "relax_mode": relax_mode,
            "deterministic": deterministic,
            "pipeline_version": pipeline_version,
            "model": model_name,
            "prompt_hash": prompt_hash,
            "cohort_definition": conditions.get("cohort_definition", {}),
            "mapped_variables": mapped_variables, # 별도 필드로 추가
            "cohort_conditions": conditions.get("cohort_definition", {}).get("extraction_details", {}).get("cohort_criteria", {}).get("population", []),
            "features": features,
            "generated_sql": {
                "cohort_sql": sql_result.get("cohort_sql"),
                "count_sql": sql_result.get("count_sql"),
                "debug_count_sql": sql_result.get("debug_count_sql")
            },
            "db_result": db_result
        }
        # cohort_definition 내부에도 변수 정보를 업데이트하여 프론트엔드의 cd.variables 접근 지원
        final_response["cohort_definition"]["variables"] = mapped_variables

        # 임시 저장 (에러가 없을 때만 캐싱하여 재시도 가능하게 함)
        if store:
            error_msg = db_result.get("error")
            if not error_msg:
                store.set(cache_key, final_response)
                logger.info(f"PDF 분석 결과 임시 캐시 저장 완료 (Key: {cache_key})")
            else:
                logger.warning(f"SQL 실행 에러로 인해 캐시 저장 건너뜀 (Key: {cache_key}): {error_msg}")
            
        return final_response

    async def fix_sql_with_error_async(self, failed_sql: str, error_message: str) -> str:
        """실행 실패한 SQL과 Oracle 에러 메시지를 GPT에게 보내 수정된 SQL을 반환."""
        schema_text = _load_schema_for_prompt()
        prompt = f"""아래 Oracle SQL이 실행 시 오류가 발생했습니다. 오류를 분석하고 수정된 SQL만 반환하세요.

## Oracle DB 스키마 (정확한 컬럼명)
{schema_text}

## 실패한 SQL
{failed_sql}

## Oracle 오류 메시지
{error_message}

## 수정 규칙
1. 위 스키마에 나열된 테이블명과 컬럼명만 정확히 사용하세요.
2. ICD_CODE는 CHAR 타입이므로 TRIM() 사용.
3. 테이블 별칭: DIAGNOSES_ICD->dx, D_ICD_DIAGNOSES->dd, ADMISSIONS->a, PATIENTS->p, ICUSTAYS->icu
4. 세미콜론(;), 백틱(`), 큰따옴표로 감싼 컬럼명 사용 금지.
5. 서브쿼리 내부에서만 사용한 컬럼을 외부에서 참조하지 마세요.
6. 결과 행 수를 FETCH FIRST 200 ROWS ONLY로 제한.

수정된 SQL만 JSON으로 반환하세요:
{{"fixed_sql": "수정된 SELECT 쿼리"}}
"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "당신은 Oracle SQL 디버깅 전문가입니다. 스키마에 맞게 SQL을 수정하세요. 반드시 유효한 JSON만 출력하세요."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0,
                seed=42
            )
            data = json.loads(response.choices[0].message.content)
            fixed = str(data.get("fixed_sql", "")).strip().rstrip(";").replace("`", "")
            fixed = re.sub(r'"([A-Za-z_]+)"', r'\1', fixed)
            logger.info("SQL 자동수정 완료: %s", fixed[:200])
            return fixed
        except Exception as e:
            logger.error("SQL 자동수정 실패: %s", e)
            return ""

    async def fix_sql_with_error(self, failed_sql: str, error_message: str) -> str:
        """비동기 방식으로 SQL 오류 수정 실행"""
        return await self.fix_sql_with_error_async(failed_sql, error_message)
