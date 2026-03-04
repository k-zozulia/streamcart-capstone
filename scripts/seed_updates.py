"""
Simulates ongoing e-commerce activity in the source PostgreSQL database.

Each run:
  - Inserts  50–200 new orders (~90% clean, ~10% intentionally dirty)
  - Updates  10–30 existing orders

Dirty rows exist so the ETL pipeline has something to quarantine:
  - negative quantity
  - negative unit_price
  - invalid status

Usage:
    # Single run
    python scripts/seed_updates.py

    # Continuous loop (every 60 seconds)
    python scripts/seed_updates.py --loop --interval 60

    # Custom counts
    python scripts/seed_updates.py --min-inserts 100 --max-inserts 200 --min-updates 20 --max-updates 30
"""

import argparse
import logging
import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB connection — reads from env vars
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", "5433")),
    "dbname": os.getenv("SOURCE_DB_NAME", "ecommerce"),
    "user": os.getenv("SOURCE_DB_USER", "ecommerce_user"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "ecommerce_pass"),
}

# ---------------------------------------------------------------------------
# Status transition map
# ---------------------------------------------------------------------------
VALID_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

STATUS_TRANSITIONS = {
    "pending": ["processing", "cancelled"],
    "processing": ["shipped", "cancelled"],
    "shipped": ["delivered"],
    "delivered": [],
    "cancelled": [],
}

# New orders always start as pending or processing
NEW_ORDER_STATUSES = ["pending", "pending", "processing"]

# Invalid statuses
BAD_STATUSES = ["done", "complete", "PENDING", "Shipped", "unknown"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_connection():
    """Open and return a psycopg2 connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def build_rows(count: int) -> list:
    """
    Build a list of row tuples to insert.
    ~90% clean rows, ~10% dirty rows (3 types, rotated evenly).

    Returns list of tuples:
        (customer_id, product_id, quantity, unit_price, status, created_at, updated_at)
    """
    rows = []
    now = datetime.now(timezone.utc)

    n_dirty = max(1, count // 10)
    n_clean = count - n_dirty

    # Clean rows
    for _ in range(n_clean):
        rows.append(
            (
                random.randint(1, 500),
                random.randint(1, 200),
                random.randint(1, 20),
                round(random.uniform(5.0, 499.99), 2),
                random.choice(NEW_ORDER_STATUSES),
                now,
                now,
            )
        )

    # Dirty rows
    dirty_types = ["neg_qty", "neg_price", "bad_status"]

    for i in range(n_dirty):
        dtype = dirty_types[i % 3]

        if dtype == "neg_qty":
            rows.append(
                (
                    random.randint(1, 500),
                    random.randint(1, 200),
                    -random.randint(1, 10),
                    round(random.uniform(5.0, 99.99), 2),
                    "pending",
                    now,
                    now,
                )
            )

        elif dtype == "neg_price":
            rows.append(
                (
                    random.randint(1, 500),
                    random.randint(1, 200),
                    random.randint(1, 10),
                    -round(random.uniform(0.01, 49.99), 2),
                    "processing",
                    now,
                    now,
                )
            )

        else:
            rows.append(
                (
                    random.randint(1, 500),
                    random.randint(1, 200),
                    random.randint(1, 10),
                    round(random.uniform(5.0, 99.99), 2),
                    random.choice(BAD_STATUSES),
                    now,
                    now,
                )
            )

    return rows


def insert_new_orders(cur, count: int) -> dict:
    """
    Insert `count` new orders (mix of clean and dirty).
    Returns counts breakdown: {total, clean, dirty}.
    """
    rows = build_rows(count)

    n_dirty = max(1, count // 10)
    n_clean = count - n_dirty

    execute_values(
        cur,
        """
        INSERT INTO orders
            (customer_id, product_id, quantity, unit_price, status, created_at, updated_at)
        VALUES %s
        """,
        rows,
    )

    return {"total": count, "clean": n_clean, "dirty": n_dirty}


def update_existing_orders(cur, count: int) -> int:
    """
    Pick `count` non-terminal orders at random and advance their status.

    Only updates rows with valid statuses from STATUS_TRANSITIONS —
    dirty 'bad_status' rows are intentionally skipped (no valid transition).

    Returns number of rows actually updated.
    """
    cur.execute(
        """
        SELECT order_id, status
        FROM   orders
        WHERE  status IN ('pending', 'processing', 'shipped')
        ORDER  BY RANDOM()
        LIMIT  %s
        """,
        (count,),
    )
    rows = cur.fetchall()

    if not rows:
        log.warning("No non-terminal orders found to update.")
        return 0

    updated = 0
    for order_id, current_status in rows:
        next_statuses = STATUS_TRANSITIONS.get(current_status, [])
        if not next_statuses:
            continue

        new_status = random.choice(next_statuses)

        cur.execute(
            """
            UPDATE orders
            SET    status     = %s,
                   updated_at = NOW()
            WHERE  order_id   = %s
            """,
            (new_status, order_id),
        )
        updated += 1

    return updated


def print_status_breakdown(cur) -> None:
    """Log current status distribution across all orders."""
    cur.execute(
        "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status"
    )
    rows = cur.fetchall()
    log.info("  Status breakdown:")
    for status, cnt in rows:
        log.info(f"    {status:<14} {cnt:>6}")


def run_once(
    min_inserts: int, max_inserts: int, min_updates: int, max_updates: int
) -> None:
    """Execute one round of inserts + updates."""
    n_inserts = random.randint(min_inserts, max_inserts)
    n_updates = random.randint(min_updates, max_updates)

    log.info("=" * 60)
    log.info("seed_updates.py — starting run")
    log.info(f"  Planned inserts : {n_inserts}  (~{n_inserts // 10} dirty)")
    log.info(f"  Planned updates : {n_updates}")

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:

                # Inserts
                insert_stats = insert_new_orders(cur, n_inserts)
                log.info(
                    f"  Inserted        : {insert_stats['total']} rows "
                    f"({insert_stats['clean']} clean, {insert_stats['dirty']} dirty)"
                )

                # Updates
                updated = update_existing_orders(cur, n_updates)
                log.info(f"  Updated         : {updated} rows")

                # Quick stats
                cur.execute("SELECT COUNT(*) FROM orders")
                total = cur.fetchone()[0]
                log.info(f"  Total rows in DB: {total}")

                print_status_breakdown(cur)

        log.info("Run complete ✓")

    except Exception as exc:
        conn.rollback()
        log.error(f"Run failed: {exc}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulate StreamCart order activity in PostgreSQL."
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run in a continuous loop (useful for Airflow demos)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between runs in loop mode (default: 60)",
    )
    parser.add_argument("--min-inserts", type=int, default=50)
    parser.add_argument("--max-inserts", type=int, default=200)
    parser.add_argument("--min-updates", type=int, default=10)
    parser.add_argument("--max-updates", type=int, default=30)
    return parser.parse_args()


def main():
    args = parse_args()

    if args.loop:
        log.info(f"Loop mode: running every {args.interval}s. Press Ctrl+C to stop.")
        try:
            while True:
                run_once(
                    args.min_inserts,
                    args.max_inserts,
                    args.min_updates,
                    args.max_updates,
                )
                log.info(f"Sleeping {args.interval}s …")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
    else:
        run_once(
            args.min_inserts,
            args.max_inserts,
            args.min_updates,
            args.max_updates,
        )


if __name__ == "__main__":
    main()
