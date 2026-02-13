from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.services.oracle.connection import acquire_connection


def extract_metadata(owner: str, output_dir: str = "var/metadata") -> dict[str, Any]:
    owner = owner.upper()
    schema_catalog: dict[str, Any] = {"owner": owner, "tables": {}}
    join_graph: dict[str, Any] = {"owner": owner, "edges": []}

    conn = acquire_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM all_tables
        WHERE owner = :owner
    """, owner=owner)
    tables = [row[0] for row in cur.fetchall()]

    cur.execute("""
        SELECT table_name, column_name, data_type, nullable
        FROM all_tab_columns
        WHERE owner = :owner
        ORDER BY table_name, column_id
    """, owner=owner)
    for table_name, column_name, data_type, nullable in cur.fetchall():
        table_entry = schema_catalog["tables"].setdefault(
            table_name,
            {"columns": [], "primary_keys": []},
        )
        table_entry["columns"].append({
            "name": column_name,
            "type": data_type,
            "nullable": nullable == "Y",
        })

    cur.execute("""
        SELECT acc.table_name, acc.column_name, ac.constraint_type, ac.constraint_name,
               ac.r_owner, ac.r_constraint_name
        FROM all_cons_columns acc
        JOIN all_constraints ac
          ON acc.owner = ac.owner AND acc.constraint_name = ac.constraint_name
        WHERE ac.owner = :owner
          AND ac.constraint_type IN ('P', 'R')
    """, owner=owner)

    pk_by_constraint: dict[str, list[tuple[str, str]]] = {}
    fk_rows: list[tuple[str, str, str, str]] = []

    for table_name, column_name, ctype, cname, r_owner, r_cname in cur.fetchall():
        if ctype == "P":
            pk_by_constraint.setdefault(cname, []).append((table_name, column_name))
            table_entry = schema_catalog["tables"].setdefault(
                table_name,
                {"columns": [], "primary_keys": []},
            )
            table_entry["primary_keys"].append(column_name)
        elif ctype == "R" and r_owner == owner:
            fk_rows.append((table_name, column_name, r_owner, r_cname))

    for fk_table, fk_column, r_owner, r_cname in fk_rows:
        pk_cols = pk_by_constraint.get(r_cname, [])
        for pk_table, pk_column in pk_cols:
            join_graph["edges"].append({
                "from_table": fk_table,
                "from_column": fk_column,
                "to_table": pk_table,
                "to_column": pk_column,
                "type": "FK",
            })

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "schema_catalog.json").write_text(
        json.dumps(schema_catalog, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (output_path / "join_graph.json").write_text(
        json.dumps(join_graph, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    cur.close()
    conn.close()

    return {"schema_catalog": schema_catalog, "join_graph": join_graph, "tables": len(tables)}
