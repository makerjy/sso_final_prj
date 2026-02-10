from __future__ import annotations

import pandas as pd

from src.agent.code_generator import generate_chart


def test_generate_chart_bar_with_agg() -> None:
    df = pd.DataFrame(
        {
            "year": [2020, 2020, 2021, 2021],
            "rate": [0.1, 0.3, 0.2, 0.4],
        }
    )
    chart_spec = {"chart_type": "bar", "x": "year", "y": "rate", "agg": "avg"}

    result = generate_chart(chart_spec, df)

    assert result["figure_json"] is not None
    y_values = result["figure_json"]["data"][0]["y"]
    assert len(y_values) == 2
