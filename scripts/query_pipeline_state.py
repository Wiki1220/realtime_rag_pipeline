#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.state_manager import StateManager


DEFAULT_DB_PATH = "logs/pipeline_states.db"
DEFAULT_PIPELINE = "configs/pipeline_legacy_enhanced.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query preset4 pipeline runs from the local SQLite state database.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-id", help="Query a single run by run_id.")
    mode.add_argument("--latest", type=int, help="Query the latest N runs.")
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"SQLite DB path. Default: {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--pipeline",
        default="",
        help=(
            "Pipeline config name used to derive the state table. "
            "If omitted, the script auto-searches matching state tables."
        ),
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden runs.",
    )
    return parser.parse_args()


def load_rows(
    db_path: str,
    pipeline_name: str,
    run_id: str = "",
    latest: int = 0,
    include_hidden: bool = False,
) -> List[sqlite3.Row]:
    manager = StateManager(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        table_names = list_state_tables(cursor)
        if not table_names:
            raise SystemExit("No state_* tables found in the SQLite database.")

        if pipeline_name:
            table_name = manager._get_table_name(pipeline_name)
            if table_name not in table_names:
                raise SystemExit(f"State table not found: {table_name}")
            manager.ensure_management_columns(pipeline_name)
            table_names = [table_name]

        if run_id:
            for table_name in table_names:
                cursor.execute(f"SELECT * FROM {table_name} WHERE run_id = ?", (run_id,))
                row = cursor.fetchone()
                if row:
                    row_dict = dict(row)
                    row_dict["_table_name"] = table_name
                    return [row_dict]
            raise SystemExit(f"Run not found: {run_id}")

        rows: List[Dict[str, Any]] = []
        for table_name in table_names:
            hidden_clause = "" if include_hidden else "WHERE COALESCE(is_hidden, 0) = 0"
            cursor.execute(
                f"""
                SELECT * FROM {table_name}
                {hidden_clause}
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (latest,),
            )
            for row in cursor.fetchall():
                row_dict = dict(row)
                row_dict["_table_name"] = table_name
                rows.append(row_dict)
        rows.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
        return rows[:latest]


def list_state_tables(cursor: sqlite3.Cursor) -> List[str]:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'state_%' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def parse_metadata(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {"_raw": raw}
    return parsed if isinstance(parsed, dict) else {"_raw": parsed}


def parse_error_message(raw: Any) -> Any:
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return raw


def build_processor_states(row_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    for key, value in row_dict.items():
        if not key.endswith("_status") or key == "global_status":
            continue
        proc_name = key[:-7]
        states[proc_name] = {
            "status": value or "pending",
            "cost": row_dict.get(f"{proc_name}_cost", 0.0) or 0.0,
        }
    return dict(sorted(states.items(), key=lambda item: item[0]))


def serialize_row(row: Any) -> Dict[str, Any]:
    row_dict = dict(row)
    return {
        "table_name": row_dict.get("_table_name"),
        "run_id": row_dict.get("run_id"),
        "doc_id": row_dict.get("doc_id"),
        "source_file": row_dict.get("source_file"),
        "global_status": row_dict.get("global_status"),
        "created_at": row_dict.get("created_at"),
        "updated_at": row_dict.get("updated_at"),
        "error_message": parse_error_message(row_dict.get("error_message")),
        "processor_states": build_processor_states(row_dict),
        "run_metadata": parse_metadata(row_dict.get("run_metadata_json")),
    }


def main() -> None:
    args = parse_args()
    db_path = str(Path(args.db_path))
    rows = load_rows(
        db_path=db_path,
        pipeline_name=args.pipeline,
        run_id=args.run_id or "",
        latest=args.latest or 0,
        include_hidden=args.include_hidden,
    )
    payload = {
        "db_path": db_path,
        "pipeline": args.pipeline or DEFAULT_PIPELINE,
        "count": len(rows),
        "items": [serialize_row(row) for row in rows],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
