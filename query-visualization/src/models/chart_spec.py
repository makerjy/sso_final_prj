"""시각화 결과 타입 정의."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ChartSpec(BaseModel):
    # 차트 타입 (line, bar, hist, scatter, box 등)
    chart_type: str
    # 축/그룹/집계 정보
    x: Optional[str] = None
    y: Optional[str] = None
    group: Optional[str] = None
    agg: Optional[str] = None


class AnalysisCard(BaseModel):
    # 차트 스펙
    chart_spec: ChartSpec
    # 왜 이 차트를 추천했는지
    reason: Optional[str] = None
    # Plotly figure JSON
    figure_json: Optional[Dict[str, Any]] = None
    # 코드 요약(디버깅용)
    code: Optional[str] = None
    # 자연어 요약(선택)
    summary: Optional[str] = None


class VisualizationResponse(BaseModel):
    # 원본 SQL
    sql: str
    # 테이블 미리보기
    table_preview: List[Dict[str, Any]]
    # 추천 시각화 카드 목록
    analyses: List[AnalysisCard]
