from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from fastapi import HTTPException

# Ensure backend is importable when running from repo root
ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.append(str(ROOT / "backend"))

from app.services.agents.orchestrator import run_oneshot
from app.services.oracle.executor import execute_sql


def load_examples(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("question") and obj.get("sql"):
            items.append(obj)
    return items


def safe_execute(sql: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return execute_sql(sql), None
    except HTTPException as exc:
        return None, str(exc.detail)
    except Exception as exc:
        return None, str(exc)


def normalize_rows(rows: list[list[Any]], ignore_order: bool) -> list[list[Any]]:
    if not ignore_order:
        return rows
    # Sort rows as strings for deterministic comparison
    return sorted(rows, key=lambda r: json.dumps(r, ensure_ascii=True, default=str))


def compare_results(
    expected: dict[str, Any],
    generated: dict[str, Any],
    ignore_order: bool,
) -> tuple[bool, dict[str, Any]]:
    exp_cols = expected.get("columns", [])
    gen_cols = generated.get("columns", [])
    exp_rows = expected.get("rows", [])
    gen_rows = generated.get("rows", [])

    def _is_count_col(col: str) -> bool:
        name = str(col).strip().upper()
        return name in {"CNT", "COUNT"} or "COUNT(" in name

    same_cols = exp_cols == gen_cols
    exp_norm = normalize_rows(exp_rows, ignore_order)
    gen_norm = normalize_rows(gen_rows, ignore_order)
    same_rows = exp_norm == gen_norm

    if not same_cols and same_rows and len(exp_cols) == len(gen_cols) == 1:
        if _is_count_col(exp_cols[0]) and _is_count_col(gen_cols[0]):
            same_cols = True

    return (same_cols and same_rows), {
        "same_cols": same_cols,
        "same_rows": same_rows,
        "expected_row_count": expected.get("row_count"),
        "generated_row_count": generated.get("row_count"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate text-to-SQL accuracy against expected SQL.")
    parser.add_argument(
        "--input",
        default="var/metadata/sql_examples.jsonl",
        help="Path to jsonl with {question, sql}.",
    )
    parser.add_argument(
        "--output",
        default="var/logs/eval_report.jsonl",
        help="Report output jsonl path.",
    )
    parser.add_argument("--max", type=int, default=0, help="Max number of examples (0 = all).")
    parser.add_argument("--ignore-order", action="store_true", help="Ignore row order in comparison.")
    parser.add_argument("--skip-policy", action="store_true", help="Skip PolicyGate precheck in generation.")
    parser.add_argument(
        "--require-advanced",
        action="store_true",
        help="Fail if demo cache is used (requires DEMO_MODE=false).",
    )
    args = parser.parse_args()

    examples = load_examples(Path(args.input))
    if not examples:
        print("No examples found.")
        return 1

    if args.max and args.max > 0:
        examples = examples[: args.max]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = len(examples)
    gen_ok = 0
    exec_ok = 0
    match_ok = 0
    demo_used = 0

    with out_path.open("w", encoding="utf-8") as report:
        for idx, item in enumerate(examples, 1):
            question = item["question"]
            expected_sql = item["sql"]

            payload = run_oneshot(question, skip_policy=args.skip_policy)
            if payload.get("mode") == "demo":
                demo_used += 1
                if args.require_advanced:
                    status = {
                        "idx": idx,
                        "question": question,
                        "status": "demo_used",
                    }
                    report.write(json.dumps(status, ensure_ascii=True) + "\n")
                    continue

            final = payload.get("final", {})
            generated_sql = final.get("final_sql") or payload.get("draft", {}).get("final_sql")

            if not generated_sql:
                status = {
                    "idx": idx,
                    "question": question,
                    "status": "no_generated_sql",
                }
                report.write(json.dumps(status, ensure_ascii=True) + "\n")
                continue

            gen_ok += 1

            exp_result, exp_err = safe_execute(expected_sql)
            gen_result, gen_err = safe_execute(generated_sql)

            status: dict[str, Any] = {
                "idx": idx,
                "question": question,
                "expected_sql": expected_sql,
                "generated_sql": generated_sql,
                "expected_error": exp_err,
                "generated_error": gen_err,
            }

            if exp_result and gen_result:
                exec_ok += 1
                matched, detail = compare_results(exp_result, gen_result, args.ignore_order)
                if matched:
                    match_ok += 1
                    status["status"] = "match"
                else:
                    status["status"] = "mismatch"
                status["compare"] = detail
            else:
                status["status"] = "exec_error"

            report.write(json.dumps(status, ensure_ascii=True, default=str) + "\n")

    summary = {
        "total": total,
        "generated_sql": gen_ok,
        "executed_both": exec_ok,
        "matched": match_ok,
        "demo_used": demo_used,
        "output": str(out_path),
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
