"""
HLT Inbound Shipment Tracker

Runs 3x daily (8am, 1pm, 8pm EST via GitHub Actions).
Reads the shipment tracking table from Lark Base, checks for
shipments needing attention (exceptions, customs holds, etc.),
and sends ONE alert per issue to the HLT INBOUND DELIVERIES chat.

Deduplication: Each shipment issue is tracked by writing the current
status to the "Alerted Status" field on the Lark Base record.
Once alerted for a given status, it will NOT alert again unless
the shipment status changes to a different alert-worthy status.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

from lark_client import LarkClient
from config import (
    LARK_BASE_APP_TOKEN,
    LARK_CHAT_ID_HLT_INBOUND,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Field names in the Lark Base shipment table (adjust if yours differ)
FIELD_TRACKING_NUM      = "Tracking Number"
FIELD_CARRIER           = "Carrier"
FIELD_SHIPMENT_STATUS   = "Shipment Status"
FIELD_CLIENT            = "Client"
FIELD_BOXES             = "Boxes"
FIELD_EXPECTED_DELIVERY = "Expected Delivery"
FIELD_ALERTED_STATUS    = "Alerted Status"
FIELD_MONTH             = "Month"

# Optional: set LARK_SHIPMENT_TABLE_ID as a GitHub secret to skip keyword search
LARK_SHIPMENT_TABLE_ID = os.environ.get("LARK_SHIPMENT_TABLE_ID", "")
LARK_SHIPMENT_TABLE_NAME = os.environ.get("LARK_SHIPMENT_TABLE_NAME", "")

# Statuses that indicate a shipment needs attention
ALERT_STATUSES = [
    "exception",
    "shipment exception",
    "customs hold",
    "customs delay",
    "delivery exception",
    "returned",
    "failed delivery",
    "address issue",
    "damaged",
    "lost",
    "held",
    "alert",
]

# Target chat for shipment alerts
INBOUND_CHAT_ID = LARK_CHAT_ID_HLT_INBOUND

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def field_to_text(val):
    """Convert a Lark field value to plain text."""
    if isinstance(val, list):
        parts = []
        for item in val:
            if isinstance(item, dict):
                parts.append(item.get("text", item.get("name", str(item))))
            else:
                parts.append(str(item))
        return ", ".join(parts)
    if isinstance(val, dict):
        return val.get("text", val.get("name", str(val)))
    return str(val) if val is not None else ""


def needs_attention(status_text):
    """Return True if the shipment status indicates an issue."""
    if not status_text:
        return False
    lower = status_text.strip().lower()
    for alert_kw in ALERT_STATUSES:
        if alert_kw in lower:
            return True
    return False


def is_already_alerted(record):
    """Check if we already sent an alert for this exact status.

    Returns True if the 'Alerted Status' field matches the current
    'Shipment Status', meaning we already notified about this issue.
    """
    fields = record.get("fields", {})
    alerted = field_to_text(fields.get(FIELD_ALERTED_STATUS, "")).strip().lower()
    current = field_to_text(fields.get(FIELD_SHIPMENT_STATUS, "")).strip().lower()

    if not alerted:
        return False  # never alerted before

    return alerted == current


def mark_as_alerted(lark, table_id, record_id, status_text):
    """Write the current status to the Alerted Status field so we don't re-alert."""
    try:
        lark.update_record_fields(table_id, record_id, {
            FIELD_ALERTED_STATUS: status_text
        })
        logger.info(f"Marked record {record_id} as alerted for: {status_text}")
        return True
    except Exception as e:
        logger.error(f"Failed to mark record {record_id} as alerted: {e}")
        return False

# ---------------------------------------------------------------------------
# Find the shipment tracking table
# ---------------------------------------------------------------------------

def find_shipment_table(lark, tables):
    """Find the table that contains shipment/inbound tracking data.

    First checks the LARK_SHIPMENT_TABLE_ID environment variable.
    Falls back to searching by keyword in table names.
    """
    # Option 1: Use explicit table ID from env var
    if LARK_SHIPMENT_TABLE_ID:
        for table in tables:
            if table["table_id"] == LARK_SHIPMENT_TABLE_ID:
                logger.info(f"Using configured shipment table: {table['name']} ({table['table_id']})")
                return table
        # If not found in list, create a minimal record with just the ID
        name = LARK_SHIPMENT_TABLE_NAME or LARK_SHIPMENT_TABLE_ID
        logger.info(f"Using configured shipment table ID: {LARK_SHIPMENT_TABLE_ID}")
        return {"table_id": LARK_SHIPMENT_TABLE_ID, "name": name}

    # Option 2: Use explicit table name from env var
    if LARK_SHIPMENT_TABLE_NAME:
        name_lower = LARK_SHIPMENT_TABLE_NAME.lower()
        for table in tables:
            if table.get("name", "").lower() == name_lower:
                logger.info(f"Found table by name: {table['name']} ({table['table_id']})")
                return table

    # Option 3: Keyword search in table names
    shipment_keywords = ["shipment", "inbound", "tracking", "delivery", "deliveries"]
    for table in tables:
        name_lower = table.get("name", "").lower()
        for kw in shipment_keywords:
            if kw in name_lower:
                logger.info(f"Found shipment table by keyword: {table['name']} ({table['table_id']})")
                return table

    logger.warning("No shipment table found. Available tables:")
    for t in tables:
        logger.warning(f"  - {t['name']} ({t['table_id']})")
    logger.warning("Tip: Set LARK_SHIPMENT_TABLE_ID or LARK_SHIPMENT_TABLE_NAME as a GitHub secret.")
    return None

# ---------------------------------------------------------------------------
# Build the status update message (all active shipments)
# ---------------------------------------------------------------------------

def build_status_message(records_by_client):
    """Build the full shipment status update message."""
    now = datetime.now(timezone.utc)
    lines = [f"**Shipment Status Update**", f"{now.strftime('%A, %B %d %Y')}", ""]

    for client, records in sorted(records_by_client.items()):
        lines.append(f"**-- {client} --**")
        lines.append("")

        by_carrier = {}
        for rec in records:
            fields = rec.get("fields", {})
            carrier = field_to_text(fields.get(FIELD_CARRIER, "Unknown")).upper()
            if carrier not in by_carrier:
                by_carrier[carrier] = []
            by_carrier[carrier].append(rec)

        for carrier, carrier_recs in sorted(by_carrier.items()):
            lines.append(f"*{carrier}*")
            for rec in carrier_recs:
                fields   = rec.get("fields", {})
                tracking = field_to_text(fields.get(FIELD_TRACKING_NUM, ""))
                boxes    = field_to_text(fields.get(FIELD_BOXES, ""))
                status   = field_to_text(fields.get(FIELD_SHIPMENT_STATUS, ""))
                expected = field_to_text(fields.get(FIELD_EXPECTED_DELIVERY, ""))

                box_part       = f" [{boxes}]" if boxes else ""
                status_part    = f" -- {status}" if status else ""
                expected_part  = (f" -- expected delivery on {expected}"
                                  if expected and "expect" not in status.lower() else "")
                exception_flag = " \u26a0\ufe0f" if needs_attention(status) else ""

                lines.append(f"{tracking}{box_part}{status_part}{expected_part}{exception_flag}")
            lines.append("")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Build the alert message (only NEW shipment issues)
# ---------------------------------------------------------------------------

def build_alert_message(alert_records):
    """Build a Shipment Alert card for issues needing attention."""
    lines = ["**HLT Shipment Alert**", "",
             "The following shipments need attention:"]
    for table_name, rec in alert_records:
        fields   = rec.get("fields", {})
        carrier  = field_to_text(fields.get(FIELD_CARRIER, "Unknown")).upper()
        tracking = field_to_text(fields.get(FIELD_TRACKING_NUM, ""))
        client   = field_to_text(fields.get(FIELD_CLIENT, "Unknown"))
        month    = field_to_text(fields.get(FIELD_MONTH, ""))
        status   = field_to_text(fields.get(FIELD_SHIPMENT_STATUS, ""))

        month_tag = f" [{month}]" if month else ""
        lines.append(f"\u2022 {carrier} | {tracking}{month_tag} | {client} | {status}")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not INBOUND_CHAT_ID:
        logger.error("LARK_CHAT_ID_HLT_INBOUND not set. Cannot send shipment alerts.")
        sys.exit(1)

    lark = LarkClient()

    logger.info("Discovering tables in Lark Base...")
    tables = lark.get_all_tables(LARK_BASE_APP_TOKEN)
    logger.info(f"Found {len(tables)} tables")

    shipment_table = find_shipment_table(lark, tables)
    if not shipment_table:
        logger.error("Could not find shipment tracking table. Exiting.")
        logger.error("Set LARK_SHIPMENT_TABLE_ID or LARK_SHIPMENT_TABLE_NAME as a GitHub secret.")
        sys.exit(1)

    table_id   = shipment_table["table_id"]
    table_name = shipment_table["name"]

    logger.info(f"Reading records from {table_name}...")
    records = lark.get_all_records(LARK_BASE_APP_TOKEN, table_id)
    logger.info(f"Found {len(records)} shipment records")

    # ----- Categorize records -----
    records_by_client = {}
    alert_records = []
    seen_tracking = set()  # prevent duplicates within this run

    for rec in records:
        fields   = rec.get("fields", {})
        status   = field_to_text(fields.get(FIELD_SHIPMENT_STATUS, "")).strip()
        tracking = field_to_text(fields.get(FIELD_TRACKING_NUM, "")).strip()

        # Skip records without tracking numbers
        if not tracking:
            continue

        # Skip fully delivered shipments
        if status.lower() == "delivered":
            continue

        # Group by client for the status update
        client = field_to_text(fields.get(FIELD_CLIENT, "Unknown")).strip()
        if client not in records_by_client:
            records_by_client[client] = []
        records_by_client[client].append(rec)

        # Check if this shipment needs attention AND hasn't been alerted yet
        if needs_attention(status) and not is_already_alerted(rec):
            # Deduplicate within this single run (same tracking number)
            dedup_key = f"{tracking}|{status.lower()}"
            if dedup_key not in seen_tracking:
                seen_tracking.add(dedup_key)
                alert_records.append((table_name, rec))

    # ----- Step 1: Mark alert records BEFORE sending -----
    # This prevents duplicates if the workflow runs again before
    # the records are updated (race condition protection).
    successfully_marked = []
    for tname, rec in alert_records:
        record_id = rec.get("record_id", "")
        status = field_to_text(rec.get("fields", {}).get(FIELD_SHIPMENT_STATUS, ""))
        if record_id:
            if mark_as_alerted(lark, table_id, record_id, status):
                successfully_marked.append((tname, rec))
        else:
            logger.warning(f"Skipping alert for {record_id} — could not mark as alerted")

    # ----- Step 2: Send the full status update -----
    if records_by_client:
        status_msg = build_status_message(records_by_client)
        try:
            lark.send_group_message(status_msg, chat_id=INBOUND_CHAT_ID)
            logger.info("Sent shipment status update")
        except Exception as e:
            logger.error(f"Failed to send status update: {e}")
    else:
        logger.info("No active shipments to report.")

    # ----- Step 3: Send alert ONLY for successfully marked records -----
    if successfully_marked:
        alert_msg = build_alert_message(successfully_marked)
        try:
            lark.send_alert_card(alert_msg, chat_id=INBOUND_CHAT_ID)
            logger.info(f"Sent alert for {len(successfully_marked)} shipment issues")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    else:
        logger.info("No new shipment issues to alert on.")

    logger.info("Done!")


if __name__ == "__main__":
    main()

