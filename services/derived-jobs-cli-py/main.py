import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Tuple

import psycopg
from psycopg.types.json import Json


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def log(message: str) -> None:
    print(f"[derived-jobs] {message}")


def parse_checkpoint(raw: Any) -> Dict[str, str]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    return {}


def default_checkpoint() -> Dict[str, str]:
    return {
        "normalized_at": "1970-01-01T00:00:00+00:00",
        "record_id": "00000000-0000-0000-0000-000000000000",
    }


def checkpoint_tuple(checkpoint: Dict[str, str]) -> Tuple[str, str]:
    normalized_at = checkpoint.get("normalized_at")
    record_id = checkpoint.get("record_id")
    if not normalized_at or not record_id:
        defaults = default_checkpoint()
        return defaults["normalized_at"], defaults["record_id"]
    return normalized_at, record_id


def fetch_checkpoint(conn: psycopg.Connection, job_name: str) -> Dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT checkpoint FROM job_checkpoints WHERE job_name = %s",
            (job_name,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        return parse_checkpoint(row[0])


def insert_job_run(
    conn: psycopg.Connection,
    job_name: str,
    params: Dict[str, Any],
    progress: Dict[str, Any],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_runs (job_name, status, params, progress)
            VALUES (%s, 'running', %s, %s)
            RETURNING job_run_id
            """,
            (job_name, Json(params), Json(progress)),
        )
        return int(cur.fetchone()[0])


def update_job_run_progress(
    conn: psycopg.Connection,
    job_run_id: int,
    progress: Dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE job_runs SET progress = %s WHERE job_run_id = %s",
            (Json(progress), job_run_id),
        )


def finalize_job_run(
    conn: psycopg.Connection,
    job_run_id: int,
    status: str,
    error: Optional[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_runs
            SET status = %s,
                ended_at = now(),
                error = %s
            WHERE job_run_id = %s
            """,
            (status, error, job_run_id),
        )


def load_batch_for_dry_run(
    conn: psycopg.Connection,
    last_normalized_at: str,
    last_record_id: str,
    batch_size: int,
) -> Tuple[int, Optional[Tuple[str, str]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT normalized_at, record_id
            FROM canonical_records
            WHERE (
                   (normalized_at > %s)
                OR (normalized_at = %s AND record_id > %s)
                  )
              AND event_version IS NOT NULL
              AND record_kind IS NOT NULL
              AND normalized ? 'patient_token'
              AND normalized->>'patient_token' <> ''
            ORDER BY normalized_at ASC, record_id ASC
            LIMIT %s
            """,
            (last_normalized_at, last_normalized_at, last_record_id, batch_size),
        )
        rows = cur.fetchall()
        if not rows:
            return 0, None
        last_row = rows[-1]
        return len(rows), (str(last_row[0]), str(last_row[1]))


def verify_patient_index(conn: psycopg.Connection) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM canonical_records
            WHERE event_version IS NOT NULL
              AND record_kind IS NOT NULL
              AND normalized ? 'patient_token'
              AND normalized->>'patient_token' <> ''
            """
        )
        canonical_total_eligible = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM derived_patient_record_index")
        derived_total = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(DISTINCT record_id) FROM derived_patient_record_index")
        distinct_record_ids = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(*)
            FROM canonical_records c
            WHERE c.event_version IS NOT NULL
              AND c.record_kind IS NOT NULL
              AND c.normalized ? 'patient_token'
              AND c.normalized->>'patient_token' <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM derived_patient_record_index d
                  WHERE d.record_id = c.record_id
                    AND d.patient_token = c.normalized->>'patient_token'
              )
            """
        )
        missing_count = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT c.record_id
            FROM canonical_records c
            WHERE c.event_version IS NOT NULL
              AND c.record_kind IS NOT NULL
              AND c.normalized ? 'patient_token'
              AND c.normalized->>'patient_token' <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM derived_patient_record_index d
                  WHERE d.record_id = c.record_id
                    AND d.patient_token = c.normalized->>'patient_token'
              )
            ORDER BY c.normalized_at ASC, c.record_id ASC
            LIMIT 10
            """
        )
        sample_missing = [str(row[0]) for row in cur.fetchall()]

    results = {
        "mode": "verify",
        "canonical_total_eligible": canonical_total_eligible,
        "derived_total": derived_total,
        "distinct_record_ids_in_derived": distinct_record_ids,
        "missing_count": missing_count,
        "sample_missing": sample_missing,
    }

    log(f"canonical_total_eligible={canonical_total_eligible}")
    log(f"derived_total={derived_total}")
    log(f"distinct_record_ids_in_derived={distinct_record_ids}")
    log(f"missing_count={missing_count}")
    if sample_missing:
        log(f"sample_missing={sample_missing}")
    return results


def materialize_patient_index(args: argparse.Namespace) -> int:
    job_name = "materialize_patient_index"
    postgres_dsn = get_env("POSTGRES_DSN")

    params = {
        "batch_size": args.batch_size,
        "dry_run": args.dry_run,
        "verify": args.verify,
    }
    progress: Dict[str, Any] = {
        "rows_scanned": 0,
        "rows_written": 0,
        "checkpoint": {},
    }

    conn = psycopg.connect(postgres_dsn)
    job_run_id = None
    try:
        if args.verify:
            with conn.transaction():
                job_run_id = insert_job_run(conn, job_name, params, {"mode": "verify"})
            log(f"started job_run_id={job_run_id}")
            with conn.transaction():
                results = verify_patient_index(conn)
            missing_count = int(results.get("missing_count") or 0)
            with conn.transaction():
                update_job_run_progress(conn, job_run_id, results)
                if missing_count == 0:
                    finalize_job_run(conn, job_run_id, "succeeded", None)
                else:
                    finalize_job_run(conn, job_run_id, "failed", f"missing_count={missing_count}")
            return 0 if missing_count == 0 else 2
        with conn.transaction():
            checkpoint = fetch_checkpoint(conn, job_name)
            progress["checkpoint"] = checkpoint or {}
            job_run_id = insert_job_run(conn, job_name, params, progress)
        log(f"started job_run_id={job_run_id}")

        last_checkpoint = checkpoint or {}
        rows_scanned = 0
        rows_written = 0

        while True:
            last_normalized_at, last_record_id = checkpoint_tuple(last_checkpoint)

            if args.dry_run:
                with conn.transaction():
                    batch_count, last_row = load_batch_for_dry_run(
                        conn,
                        last_normalized_at,
                        last_record_id,
                        args.batch_size,
                    )
                if batch_count == 0:
                    break
                rows_scanned += batch_count
                if last_row:
                    last_checkpoint = {
                        "normalized_at": last_row[0],
                        "record_id": last_row[1],
                    }
                progress = {
                    "rows_scanned": rows_scanned,
                    "rows_written": rows_written,
                    "checkpoint": last_checkpoint,
                }
                with conn.transaction():
                    update_job_run_progress(conn, job_run_id, progress)
                log(
                    "dry_run batch_size=%s rows_scanned=%s last_normalized_at=%s last_record_id=%s"
                    % (
                        batch_count,
                        rows_scanned,
                        last_checkpoint.get("normalized_at"),
                        last_checkpoint.get("record_id"),
                    )
                )
                continue

            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH batch AS (
                            SELECT
                                normalized_at,
                                record_id,
                                source,
                                event_version,
                                record_kind,
                                schema_hint,
                                ingested_at,
                                normalized->>'patient_token' AS patient_token
                            FROM canonical_records
                            WHERE (
                                   (normalized_at > %s)
                                OR (normalized_at = %s AND record_id > %s)
                                  )
                              AND event_version IS NOT NULL
                              AND record_kind IS NOT NULL
                              AND normalized ? 'patient_token'
                              AND normalized->>'patient_token' <> ''
                            ORDER BY normalized_at ASC, record_id ASC
                            LIMIT %s
                        ),
                        insert_rows AS (
                            INSERT INTO derived_patient_record_index (
                                patient_token,
                                record_id,
                                source,
                                event_version,
                                record_kind,
                                schema_hint,
                                ingested_at,
                                normalized_at
                            )
                            SELECT
                                patient_token,
                                record_id,
                                source,
                                event_version,
                                record_kind,
                                schema_hint,
                                ingested_at,
                                normalized_at
                            FROM batch
                            ON CONFLICT (patient_token, record_id) DO NOTHING
                            RETURNING 1
                        ),
                        last_row AS (
                            SELECT normalized_at, record_id
                            FROM batch
                            ORDER BY normalized_at DESC, record_id DESC
                            LIMIT 1
                        )
                        SELECT
                            (SELECT COUNT(*) FROM batch) AS batch_count,
                            (SELECT COUNT(*) FROM insert_rows) AS insert_count,
                            (SELECT normalized_at FROM last_row) AS last_normalized_at,
                            (SELECT record_id FROM last_row) AS last_record_id
                        """,
                        (last_normalized_at, last_normalized_at, last_record_id, args.batch_size),
                    )
                    result = cur.fetchone()
                    if not result or result[0] == 0:
                        batch_count = 0
                        insert_count = 0
                        last_row = None
                    else:
                        batch_count = int(result[0])
                        insert_count = int(result[1])
                        last_row = (str(result[2]), str(result[3]))

                if batch_count == 0:
                    break

                rows_scanned += batch_count
                rows_written += insert_count
                last_checkpoint = {
                    "normalized_at": last_row[0],
                    "record_id": last_row[1],
                }

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO job_checkpoints (job_name, checkpoint, updated_at)
                        VALUES (%s, %s, now())
                        ON CONFLICT (job_name)
                        DO UPDATE SET checkpoint = EXCLUDED.checkpoint, updated_at = now()
                        """,
                        (job_name, Json(last_checkpoint)),
                    )

                progress = {
                    "rows_scanned": rows_scanned,
                    "rows_written": rows_written,
                    "checkpoint": last_checkpoint,
                }
                update_job_run_progress(conn, job_run_id, progress)

            log(
                "batch complete batch_size=%s rows_scanned=%s rows_written=%s last_normalized_at=%s last_record_id=%s"
                % (
                    batch_count,
                    rows_scanned,
                    rows_written,
                    last_checkpoint.get("normalized_at"),
                    last_checkpoint.get("record_id"),
                )
            )

        with conn.transaction():
            update_job_run_progress(conn, job_run_id, progress)
            finalize_job_run(conn, job_run_id, "succeeded", None)
        log(
            "finished job_run_id=%s rows_scanned=%s rows_written=%s"
            % (job_run_id, rows_scanned, rows_written)
        )
        return 0
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        try:
            if job_run_id is not None:
                with conn.transaction():
                    finalize_job_run(conn, job_run_id, "failed", error)
        except Exception:
            pass
        log(f"failed error={error}")
        return 1
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="derived-jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize = subparsers.add_parser(
        "materialize-patient-index",
        help="Build derived_patient_record_index from canonical records",
    )
    materialize.add_argument("--batch-size", type=int, default=500)
    materialize.add_argument("--dry-run", action="store_true")
    materialize.add_argument("--verify", action="store_true")
    materialize.set_defaults(func=materialize_patient_index)

    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
