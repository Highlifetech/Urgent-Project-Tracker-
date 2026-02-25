"""
Lark Project Due Date Tracker Bot — Main Entry Point

Automatically discovers all tables (boards) in your Lark Base,
checks each project's due date, and sends warnings to your group chat at:
  - 3 weeks before due date
  - 2 weeks before due date
  - 1 week before due date

Runs twice a day (8am and 8pm) via GitHub Actions.
Projects marked as "Shipped" are automatically skipped.

Usage:
    python main.py            # Run once (live)
    python main.py --dry-run  # Print what would be sent without messaging
"""
import sys
import logging
from datetime import datetime, timezone

from config import (
    DONE_STATUS,
    WARNING_DAYS,
    WARNING_LABELS,
)
from lark_client import LarkClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def days_until_due(due_date_ms: int) -> int:
    """Return number of whole days from now until the due date."""
    now_ms  = datetime.now(timezone.utc).timestamp() * 1000
    diff_ms = due_date_ms - now_ms
    return int(diff_ms / (1000 * 60 * 60 * 24))


def format_date(ms: int) -> str:
    """Format a millisecond timestamp as 'Mon, Feb 25 2026'."""
    if not ms:
        return "N/A"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%a, %b %-d %Y")


def urgency_emoji(days: int) -> str:
    if days <= 7:
        return "🔴"
    elif days <= 14:
        return "🟠"
    else:
        return "🟡"


def build_message(warnings: list) -> str:
    """Build the group chat card message, grouped by warning threshold."""
    by_threshold = {}
    for w in warnings:
        by_threshold.setdefault(w["threshold"], []).append(w)

    lines = ["**📋 HLT Project Due Date Reminder**"]
    lines.append(f"*{datetime.now(timezone.utc).strftime('%A, %B %-d %Y')}*")
    lines.append("")

    for days in sorted(by_threshold.keys()):
        label = WARNING_LABELS[days]
        items = by_threshold[days]
        emoji = urgency_emoji(days)
        count = len(items)
        lines.append(f"**{emoji} Due in {label} ({count} project{'s' if count != 1 else ''})**")

        for w in items:
            due_str = format_date(w["due_date_ms"])
            order   = w["order_num"]   or "—"
            desc    = w["description"] or "—"
            qty     = w["qty_ordered"] or "—"
            status  = w["status"]      or "—"
            days_left = w["days_left"]

            lines.append(
                f"• **{order}** — {desc}\n"
                f"  Due: {due_str} ({days_left} days left) | Qty: {qty} | Status: {status}"
            )
        lines.append("")

    return "\n".join(lines).strip()


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        logger.info("=== DRY RUN — no messages will be sent ===")

    lark = LarkClient()

    # Auto-discover all tables — no need to configure them manually
    try:
        table_ids = lark.get_all_table_ids()
    except Exception as e:
        logger.error(f"Failed to discover tables: {e}")
        sys.exit(1)

    if not table_ids:
        logger.error("No tables found in Base. Check LARK_BASE_APP_TOKEN.")
        sys.exit(1)

    warnings = []

    for table_id in table_ids:
        logger.info(f"Reading table: {table_id}")
        try:
            records = lark.get_table_records(table_id)
        except Exception as e:
            logger.error(f"  Failed to read table {table_id}: {e}")
            continue

        for raw in records:
            project = lark.parse_record(raw)

            # Skip shipped / done projects
            if project["status"].lower() == DONE_STATUS.lower():
                logger.info(f"  Skipping {project['order_num']} — status: {project['status']}")
                continue

            due_ms = project["due_date_ms"]
            if not due_ms:
                logger.info(f"  Skipping {project['order_num']} — no due date set")
                continue

            days_left = days_until_due(due_ms)
            logger.info(f"  {project['order_num']} — {days_left} days until due")

            # Check if this run falls within a warning window
            # Window is threshold-1 < days_left <= threshold to avoid double-firing
            for threshold in WARNING_DAYS:
                if threshold - 1 < days_left <= threshold:
                    logger.info(f"    → Sending {WARNING_LABELS[threshold]} warning")
                    warnings.append({
                        **project,
                        "days_left": days_left,
                        "threshold": threshold,
                    })
                    break

    logger.info(f"Total warnings to send: {len(warnings)}")

    if not warnings:
        logger.info("No projects hitting a warning threshold right now. No message sent.")
        return

    message = build_message(warnings)

    if dry_run:
        logger.info("Message that would be sent:")
        print(message)
    else:
        try:
            lark.send_group_message(message)
            logger.info("Warning message sent to group chat")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
