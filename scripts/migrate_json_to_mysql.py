import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pymysql


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and (k not in os.environ or not os.environ.get(k)):
            os.environ[k] = v


def safe_float(v: Any) -> Optional[float]:
    try:
        if v in ("", None):
            return None
        return float(v)
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    try:
        if v in ("", None):
            return None
        return int(v)
    except Exception:
        return None


def run_summary_of(actions: List[Dict[str, Any]]) -> Dict[str, Any]:
    sig = {"BUY": 0, "HOLD": 0, "REDUCE": 0}
    dis = {"FOUND": 0, "SKIP": 0}
    for a in actions:
        s = str(a.get("signal") or "")
        if s in sig:
            sig[s] += 1
        label = str(a.get("label") or "")
        if label.startswith("[DISCOVERY]") and "FOUND" in label:
            dis["FOUND"] += 1
        elif label.startswith("[DISCOVERY]") and "SKIP" in label:
            dis["SKIP"] += 1
    return {"total_rows": len(actions), "signal_summary": sig, "discovery_summary": dis}


def upsert_run_actions(
    conn,
    generated_at: str,
    generated_at_iso: str,
    date_key: str,
    actions: List[Dict[str, Any]],
    run_summary: Dict[str, Any],
    source_file: str,
    progress: Optional[Dict[str, Any]] = None,
    payload_json: Optional[str] = None,
) -> int:
    phase = str((progress or {}).get("stage") or "") if isinstance(progress, dict) else ""
    run_uid = f"{generated_at_iso}|{source_file or 'latest'}|{phase or 'final'}"
    sig = run_summary.get("signal_summary", {}) if isinstance(run_summary, dict) else {}
    dis = run_summary.get("discovery_summary", {}) if isinstance(run_summary, dict) else {}
    progress = progress if isinstance(progress, dict) else {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_runs (
              run_uid, generated_at_iso, generated_at_local, date_key, source_file,
              total_rows, buy_count, hold_count, reduce_count, discovery_found, discovery_skip,
              progress_stage, progress_city, progress_city_index, progress_total_cities, payload_json
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON DUPLICATE KEY UPDATE
              generated_at_local=VALUES(generated_at_local),
              date_key=VALUES(date_key),
              source_file=VALUES(source_file),
              total_rows=VALUES(total_rows),
              buy_count=VALUES(buy_count),
              hold_count=VALUES(hold_count),
              reduce_count=VALUES(reduce_count),
              discovery_found=VALUES(discovery_found),
              discovery_skip=VALUES(discovery_skip),
              progress_stage=VALUES(progress_stage),
              progress_city=VALUES(progress_city),
              progress_city_index=VALUES(progress_city_index),
              progress_total_cities=VALUES(progress_total_cities),
              payload_json=VALUES(payload_json)
            """,
            (
                run_uid,
                generated_at_iso,
                generated_at,
                date_key,
                source_file,
                safe_int(run_summary.get("total_rows") if isinstance(run_summary, dict) else None),
                safe_int(sig.get("BUY") if isinstance(sig, dict) else None),
                safe_int(sig.get("HOLD") if isinstance(sig, dict) else None),
                safe_int(sig.get("REDUCE") if isinstance(sig, dict) else None),
                safe_int(dis.get("FOUND") if isinstance(dis, dict) else None),
                safe_int(dis.get("SKIP") if isinstance(dis, dict) else None),
                str(progress.get("stage") or "") or None,
                str(progress.get("city") or "") or None,
                safe_int(progress.get("city_index")),
                safe_int(progress.get("total_cities")),
                payload_json,
            ),
        )
        cur.execute("SELECT id FROM bot_runs WHERE run_uid=%s LIMIT 1", (run_uid,))
        row = cur.fetchone()
        run_id = int(row[0])
        cur.execute("DELETE FROM bot_actions WHERE run_id=%s", (run_id,))
        if actions:
            rows = []
            sql = """
              INSERT INTO bot_actions (
                run_id, action_index, city, action_date, date_label, action_signal, action_side, label,
                token_id, opposite_token_id, condition_id, question,
                market_price, fair_prob, edge, edge_ratio, hold_reason, exit_reason, raw_json
              ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for idx, act in enumerate(actions):
                if not isinstance(act, dict):
                    continue
                rows.append(
                    (
                        run_id,
                        idx,
                        str(act.get("city") or "") or None,
                        str(act.get("date") or "") or None,
                        str(act.get("date_label") or "") or None,
                        str(act.get("signal") or "") or None,
                        str(act.get("side") or "") or None,
                        str(act.get("label") or "") or None,
                        str(act.get("token_id") or "") or None,
                        str(act.get("opposite_token_id") or "") or None,
                        str(act.get("condition_id") or "") or None,
                        str(act.get("question") or "") or None,
                        safe_float(act.get("market_price")),
                        safe_float(act.get("fair_prob")),
                        safe_float(act.get("edge")),
                        safe_float(act.get("edge_ratio")),
                        str(act.get("hold_reason") or "") or None,
                        str(act.get("exit_reason") or "") or None,
                        json.dumps(act, ensure_ascii=False),
                    )
                )
            if rows:
                cur.executemany(sql, rows)
    return len(actions)


def upsert_diagnostics(conn, payload: Dict[str, Any]) -> None:
    generated_at_iso = str(payload.get("generated_at_iso") or "")
    if not generated_at_iso:
        return
    generated_at = str(payload.get("generated_at") or "")
    mode = str(payload.get("mode") or "")
    sig = payload.get("signal_summary", {}) if isinstance(payload.get("signal_summary"), dict) else {}
    rows = payload.get("rows", [])
    diag_uid = f"{generated_at_iso}|{mode or 'unknown'}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_diagnostics (
              diag_uid, generated_at_iso, generated_at_local, mode,
              buy_count, hold_count, reduce_count, rows_count, payload_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              generated_at_local=VALUES(generated_at_local),
              mode=VALUES(mode),
              buy_count=VALUES(buy_count),
              hold_count=VALUES(hold_count),
              reduce_count=VALUES(reduce_count),
              rows_count=VALUES(rows_count),
              payload_json=VALUES(payload_json)
            """,
            (
                diag_uid,
                generated_at_iso,
                generated_at,
                mode,
                safe_int(sig.get("BUY") if isinstance(sig, dict) else None),
                safe_int(sig.get("HOLD") if isinstance(sig, dict) else None),
                safe_int(sig.get("REDUCE") if isinstance(sig, dict) else None),
                len(rows) if isinstance(rows, list) else 0,
                json.dumps(payload, ensure_ascii=False),
            ),
        )


def truncate_text(v: Any, max_len: int) -> Optional[str]:
    s = str(v or "").strip()
    if not s:
        return None
    if len(s) > max_len:
        return s[:max_len]
    return s


def insert_flat_actions(conn, actions: List[Dict[str, Any]]) -> int:
    if not actions:
        return 0
    rows = []
    for act in actions:
        if not isinstance(act, dict):
            continue
        try:
            rows.append(
                (
                    truncate_text(act.get("city"), 50),
                    truncate_text(act.get("date_label"), 50),
                    truncate_text(act.get("condition_id"), 255),
                    truncate_text(act.get("token_id"), 100),
                    truncate_text(act.get("signal"), 20),
                    safe_float(act.get("market_price")),
                    safe_float(act.get("fair_prob")),
                    safe_float(act.get("edge")),
                    safe_float(act.get("dynamic_buy_usdc")),
                    truncate_text(act.get("hold_reason"), 65535),
                )
            )
        except Exception:
            continue
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO fact_bot_actions (
              city, date_label, condition_id, token_id, trade_signal,
              market_price, fair_prob, edge, dynamic_buy_usdc, hold_reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    return len(rows)


def insert_flat_diagnostics(conn, payload: Dict[str, Any]) -> int:
    if not isinstance(payload, dict):
        return 0
    raw_rows = payload.get("rows", [])
    if not isinstance(raw_rows, list) or not raw_rows:
        return 0
    rows = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        try:
            yes = item.get("yes", {}) if isinstance(item.get("yes"), dict) else {}
            no = item.get("no", {}) if isinstance(item.get("no"), dict) else {}
            rows.append(
                (
                    truncate_text(item.get("city"), 50),
                    truncate_text(item.get("date_label"), 50),
                    safe_float(item.get("forecast_max")),
                    safe_float(item.get("confidence_score")),
                    safe_float(item.get("disagreement_index")),
                    truncate_text(yes.get("token_id"), 100),
                    safe_float(yes.get("market_price")),
                    safe_float(yes.get("fair_prob")),
                    safe_float(yes.get("edge")),
                    truncate_text(no.get("token_id"), 100),
                    safe_float(no.get("market_price")),
                    safe_float(no.get("fair_prob")),
                    safe_float(no.get("edge")),
                )
            )
        except Exception:
            continue
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_bot_diagnostics (
              city, date_label, forecast_max, confidence_score, disagreement_index,
              yes_token_id, yes_market_price, yes_fair_prob, yes_edge,
              no_token_id, no_market_price, no_fair_prob, no_edge
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    return len(rows)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    load_env_file(root / ".env")
    reports = root / "reports"

    host = os.getenv("POLY_DB_HOST", "127.0.0.1")
    port = int(os.getenv("POLY_DB_PORT", "3306") or "3306")
    user = os.getenv("POLY_DB_USER", "root")
    password = os.getenv("POLY_DB_PASSWORD", "root")
    database = os.getenv("POLY_DB_NAME", "quantify")
    timeout = int(os.getenv("POLY_DB_CONNECT_TIMEOUT_S", "5") or "5")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connect_timeout=timeout,
        autocommit=True,
    )

    run_count = 0
    action_count = 0
    flat_action_count = 0
    flat_diag_count = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM fact_bot_actions")
            cur.execute("DELETE FROM dim_bot_diagnostics")

        history_dir = reports / "history"
        for p in sorted(history_dir.glob("*.json")):
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
                actions = payload.get("actions", []) if isinstance(payload.get("actions"), list) else []
                run_summary = payload.get("run_summary") if isinstance(payload.get("run_summary"), dict) else run_summary_of(actions)
                generated_at = str(payload.get("generated_at") or "")
                generated_at_iso = str(payload.get("generated_at_iso") or "")
                date_key = str(payload.get("date_key") or (generated_at_iso[:10] if generated_at_iso else ""))
                if not generated_at_iso:
                    continue
                source_file = f"history/{p.name}"
                c = upsert_run_actions(
                    conn=conn,
                    generated_at=generated_at,
                    generated_at_iso=generated_at_iso,
                    date_key=date_key,
                    actions=actions,
                    run_summary=run_summary,
                    source_file=source_file,
                    progress=None,
                    payload_json=json.dumps(payload, ensure_ascii=False),
                )
                run_count += 1
                action_count += c
                flat_action_count += insert_flat_actions(conn, actions)
            except Exception as exc:
                print(f"[WARN] skip history file {p.name}: {exc}")

        latest = reports / "latest_actions.json"
        if latest.exists():
            try:
                payload = json.loads(latest.read_text(encoding="utf-8"))
                actions = payload.get("actions", []) if isinstance(payload.get("actions"), list) else []
                run_summary = payload.get("run_summary") if isinstance(payload.get("run_summary"), dict) else run_summary_of(actions)
                generated_at = str(payload.get("generated_at") or "")
                generated_at_iso = str(payload.get("generated_at_iso") or "")
                date_key = generated_at_iso[:10] if generated_at_iso else ""
                progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else None
                if generated_at_iso:
                    c = upsert_run_actions(
                        conn=conn,
                        generated_at=generated_at,
                        generated_at_iso=generated_at_iso,
                        date_key=date_key,
                        actions=actions,
                        run_summary=run_summary,
                        source_file="latest_actions.json",
                        progress=progress,
                        payload_json=json.dumps(payload, ensure_ascii=False),
                    )
                    run_count += 1
                    action_count += c
                    flat_action_count += insert_flat_actions(conn, actions)
            except Exception as exc:
                print(f"[WARN] skip latest_actions.json: {exc}")

        diag = reports / "diagnostics.json"
        if diag.exists():
            try:
                payload = json.loads(diag.read_text(encoding="utf-8"))
                upsert_diagnostics(conn, payload)
                flat_diag_count += insert_flat_diagnostics(conn, payload)
            except Exception as exc:
                print(f"[WARN] skip diagnostics.json: {exc}")

    print(
        f"[DONE] migrated runs={run_count}, actions={action_count}, "
        f"fact_actions={flat_action_count}, dim_diagnostics={flat_diag_count}"
    )


if __name__ == "__main__":
    main()
