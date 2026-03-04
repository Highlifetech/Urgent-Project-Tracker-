—→→→→🟡🟠🔴—•"""
Lark Project Due Date Tracker — Main Entry Point

Runs twice daily (8am + 8pm EST via GitHub Actions cron).
Scans all tables in the Lark Base, checks due dates, and sends
warnings to the appropriate Lark group chat:
  - Projects in Hannah's tables → PRODUCTION (HANNAH) chat
  - Projects in Lucy's tables   → PRODUCTION (LUCY) chat
  - Unknown tables              → both chats

Warning windows (to avoid double-firing at 8am and 8pm):
  3 weeks: days_left in (14, 21]
  2 weeks: days_left in (7,  14]
  1 week:  days_left in (0,   7]

Projects with status "Shipped" are skipped.
"""

import sys
import logging
from datetime import date, datetime

from config import (
    LARK_BASE_APP_TOKEN,
    LARK_CHAT_ID_HANNAH,
    LARK_CHAT_ID_LUCY,
    WARNING_DAYS,
    WARNING_LABELS,
    DONE_STATUS,
    FIELD_DUE_DATE,
    FIELD_STATUS,
    FIELD_ORDER_NUM,
    FIELD_DESCRIPTION,
    CHAT_ROUTING,
)
from lark_client import LarkClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def days_until(due_date_str: str) -> int | None:
    """Return days from today until due_date_str, or None if unparseable."""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            due = datetime.strptime(due_date_str.strip()[:10], fmt).date()
            return (due - date.today()).days
        except (ValueError, TypeError):
            continue
    return None


def in_warning_window(days_left: int, threshold: int) -> bool:
    """
    Return True if days_left falls in the half-open window
    (threshold-7, threshold] so each threshold fires exactly once
    across the two daily runs.
    """
    return (threshold - 7) < days_left <= threshold


def route_chat_ids(table_name: str) -> list:
    """Return list of chat IDs to notify based on table name."""
    name_lower = table_name.lower()
    for keyword, chat_id in CHAT_ROUTING.items():
        if keyword in name_lower and chat_id:
            return [chat_id]
    # Unknown table — send to both
    chat_ids = []
    if LARK_CHAT_ID_HANNAH:
        chat_ids.append(LARK_CHAT_ID_HANNAH)
    if LARK_CHAT_ID_LUCY:
        chat_ids.append(LARK_CHAT_ID_LUCY)
    return chat_ids


def build_warning_message(warnings: dict) -> str:
    """
    Build the warning message text.
    warnings = {threshold: [(table_name, record), ...]}
    """
    lines = ["**HLT Project Due Date Tracker**"]
    color = {21: "🟡", 14: "🟠", 7: "🔴"}

    for threshold in sorted(warnings.keys(), reverse=True):
        items = warnings[threshold]
        if not items:
            continue
        label = WARNING_LABELS[threshold]
        lines.append(f"\n{color[threshold]} **Due in {label}:**")
        for table_name, record in items:
            fields = record.get("fields", {})
            order_num   = fields.get(FIELD_ORDER_NUM, "N/A")
            description = fields.get(FIELD_DESCRIPTION, "")
            due_date    = fields.get(FIELD_DUE_DATE, "")
            if isinstance(due_date, dict):
                # Lark sometimes returns date as {"timestamp": ...}
                ts = due_date.get("timestamp", 0)
                due_date = datetime.utcfromtimestamp(int(ts)/1000).strftime("%Y-%m-%d") if ts else ""
            desc_part = f" — {description}" if description else ""
            lines.append(f"  • **{order_num}**{desc_part} (due {due_date}) [{table_name}]")

    return "\n".join(lines)


def main():
    lark = LarkClient()

    logger.info("Discovering all tables in Lark Base...")
    tables = lark.get_all_tables(LARK_BASE_APP_TOKEN)
    logger.info(f"Found {len(tables)} tables")

    # warnings_by_chat = {chat_id: {threshold: [items]}}
    warnings_by_chat: dict = {}

    for table in tables:
        table_id   = table["table_id"]
        table_name = table["name"]
        logger.info(f"Scanning table: {table_name} ({table_id})")

        records = lark.get_all_records(LARK_BASE_APP_TOKEN, table_id)
        logger.info(f"  {len(records)} records")

        target_chat_ids = route_chat_ids(table_name)

        for record in records:
            fields = record.get("fields", {})
            status = str(fields.get(FIELD_STATUS, "") or "").strip()
            if status.lower() == DONE_STATUS.lower():
                continue

            due_raw = fields.get(FIELD_DUE_DATE, "")
            if isinstance(due_raw, dict):
                ts = due_raw.get("timestamp", 0)
                due_str = datetime.utcfromtimestamp(int(ts)/1000).strftime("%Y-%m-%d") if ts else ""
            else:
                due_str = str(due_raw or "").strip()

            if not due_str:
                continue

            days_left = days_until(due_str)
            if days_left is None:
                continue

            for threshold in WARNING_DAYS:
                if in_warning_window(days_left, threshold):
                    for chat_id in target_chat_ids:
                        if chat_id not in warnings_by_chat:
                            warnings_by_chat[chat_id] = {t: [] for t in WARNING_DAYS}
                        warnings_by_chat[chat_id][threshold].append((table_name, record))

    if not warnings_by_chat:
        logger.info("No warnings to send today.")
        return

    for chat_id, warnings in warnings_by_chat.items():
        if any(warnings[t] for t in WARNING_DAYS):
            msg = build_warning_message(warnings)
            lark.send_group_message(msg, chat_id=chat_id)
            logger.info(f"Sent warnings to chat {chat_id}")

    logger.info("Done!")


if __name__ == "__main__":
    main()
