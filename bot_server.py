import os
import logging
import json
import re
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
import anthropic
import psycopg2
import psycopg2.extras
from lark_client import LarkClient
from netsuite_client import NetSuiteClient
from pipedrive_client import PipedriveClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
app = Flask(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BOT_NAME = os.environ.get("BOT_NAME", "Iron Bot")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

from config import (
    LARK_CHAT_ID_HANNAH_ARTWORK, LARK_CHAT_ID_LUCY_ARTWORK,
    FIELD_PRODUCTION_DRAWING, ARTWORK_CONFIRMED_STATUS,
    LARK_CHAT_ID_HANNAH, LARK_CHAT_ID_LUCY,
    LARK_BASE_APP_TOKEN, LARK_BASE_RECORD_URL,
    FIELD_ORDER_NUM, FIELD_STATUS, FIELD_DESCRIPTION, FIELD_CLIENT,
    FIELD_DUE_DATE, FIELD_PRODUCTION_ARTWORK, FIELD_ASSIGNED_TO,
    SKIP_STATUSES, DIGEST_EXCLUDED_BOARDS,
    LARK_CHAT_ID_DIGEST, DIGEST_SECRET,
)

FOUNDERS_CHAT = os.environ.get("LARK_CHAT_ID_FOUNDERS", "")
UPDATES_CHAT = os.environ.get("LARK_CHAT_ID_UPDATES", "")
DIGEST_CHAT = os.environ.get("LARK_CHAT_ID_DIGEST", "")
URGENT_APPROVALS_CHAT = os.environ.get("LARK_CHAT_ID_URGENT_APPROVALS", "")
HANNAH_OPEN_ID = os.environ.get("HANNAH_OPEN_ID", "ou_42c3063bcfefad67c05c615ba0088146")
LUCY_OPEN_ID = os.environ.get("LUCY_OPEN_ID", "ou_0f26700382eae7f58ea889b7e98388b4")
BRENDAN_OPEN_ID = os.environ.get("BRENDAN_OPEN_ID", "")
CARLO_OPEN_ID = os.environ.get("CARLO_OPEN_ID", "")

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
lark = LarkClient()
netsuite = NetSuiteClient()
pipedrive = PipedriveClient()

BOT_OPEN_ID = None
processed_message_ids = {}
DEDUP_TTL = 300
_projects_cache = []
_projects_cache_time = 0
PROJECTS_CACHE_TTL = 300
CONVERSATION_MAX_TURNS = 10
CONVERSATION_TTL = 3600
_memory_history = {}

# =========================================================================
# HELPERS
# =========================================================================

def record_link(table_id, record_id):
    return f"{LARK_BASE_RECORD_URL}{LARK_BASE_APP_TOKEN}?table={table_id}&view=vewUkx3tAe&record={record_id}"

def field_to_text(val):
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

def get_assigned_to(fields):
    raw = fields.get(FIELD_ASSIGNED_TO, "")
    text = field_to_text(raw).strip().lower()
    if "hannah" in text:
        return "Hannah"
    if "lucy" in text:
        return "Lucy"
    return "Brendan"

def get_assigned_from_table(table_name):
    name = table_name.lower()
    if "hannah" in name:
        return "Hannah"
    if "lucy" in name:
        return "Lucy"
    return "Brendan"

def get_image_key_from_field(fields, field_name="Production Artwork"):
    artwork = fields.get(field_name, [])
    if isinstance(artwork, list) and artwork:
        first = artwork[0]
        if isinstance(first, dict):
            return first.get("file_token", first.get("token", ""))
    return ""

def parse_date_ms(val):
    if isinstance(val, (int, float)) and val > 1000000000:
        return val if val > 1000000000000 else val * 1000
    if isinstance(val, dict):
        return val.get("timestamp", 0)
    return 0

def ms_to_date(ms):
    if not ms:
        return None
    try:
        return datetime.utcfromtimestamp(ms / 1000).date()
    except Exception:
        return None

def _is_excluded_board(table_name):
    tname = table_name.strip().lower()
    for excl in DIGEST_EXCLUDED_BOARDS:
        if tname == excl or tname.startswith(excl):
            return True
    return False

def get_user_name(open_id):
    if open_id == HANNAH_OPEN_ID:
        return "Hannah"
    if open_id == LUCY_OPEN_ID:
        return "Lucy"
    if open_id == BRENDAN_OPEN_ID:
        return "Brendan"
    if open_id == CARLO_OPEN_ID:
        return "Carlo"
    return "Unknown"

# =========================================================================
# DATABASE
# =========================================================================

def _get_db_conn():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, connect_timeout=5)
    except Exception as e:
        logger.error("DB conn error: " + str(e))
        return None

def _init_db():
    conn = _get_db_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY, chat_id TEXT NOT NULL,
                role TEXT NOT NULL, content TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW())""")
            cur.execute("""CREATE TABLE IF NOT EXISTS card_actions (
                id SERIAL PRIMARY KEY, action_id TEXT UNIQUE NOT NULL,
                clicked_by TEXT DEFAULT '', clicked_at TIMESTAMPTZ DEFAULT NOW())""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_conv_chat ON conversations (chat_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_act ON card_actions (action_id)")
        conn.commit()
        conn.close()
        logger.info("DB tables ready")
    except Exception as e:
        logger.error("DB init error: " + str(e))
        try:
            conn.close()
        except Exception:
            pass

def _is_action_clicked(action_id):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM card_actions WHERE action_id = %s", (action_id,))
            result = cur.fetchone()
        conn.close()
        return result is not None
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return False

def _mark_action_clicked(action_id, clicked_by=""):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO card_actions (action_id, clicked_by) VALUES (%s, %s) ON CONFLICT (action_id) DO NOTHING", (action_id, clicked_by))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error("Mark action error: " + str(e))
        try:
            conn.close()
        except Exception:
            pass
        return False

def _get_conversation(chat_id):
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("DELETE FROM conversations WHERE created_at < NOW() - INTERVAL '%s seconds'", (CONVERSATION_TTL,))
                cur.execute("SELECT role, content FROM conversations WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s", (chat_id, CONVERSATION_MAX_TURNS * 2))
                rows = cur.fetchall()
            conn.commit()
            conn.close()
            return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
        except Exception as e:
            logger.error("DB read: " + str(e))
            try:
                conn.close()
            except Exception:
                pass
    return _memory_history.get(chat_id, [])

def _add_to_conversation(chat_id, role, content):
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO conversations (chat_id, role, content) VALUES (%s, %s, %s)", (chat_id, role, content[:8000]))
            conn.commit()
            conn.close()
            return
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    if chat_id not in _memory_history:
        _memory_history[chat_id] = []
    _memory_history[chat_id].append({"role": role, "content": content})

# =========================================================================
# DATA FETCHING
# =========================================================================

def fetch_all_projects():
    global _projects_cache, _projects_cache_time
    now = time.time()
    if _projects_cache and (now - _projects_cache_time) < PROJECTS_CACHE_TTL:
        return _projects_cache
    try:
        tables = lark.get_all_tables()
        all_records = []
        for table in tables:
            table_id = table.get("table_id", "")
            table_name = table.get("name", table_id)
            if not table_id or _is_excluded_board(table_name):
                continue
            try:
                records = lark.get_table_records(table_id)
                for rec in records:
                    flat = dict(rec.get("fields", {}))
                    flat["__table_name__"] = table_name
                    flat["__table_id__"] = table_id
                    flat["__record_id__"] = rec.get("record_id", "")
                    all_records.append(flat)
            except Exception as e:
                logger.warning(f"Table {table_name}: {str(e)[:80]}")
        _projects_cache = all_records
        _projects_cache_time = now
        logger.info(f"Fetched {len(all_records)} records")
        return all_records
    except Exception as e:
        logger.error(f"Lark fetch error: {e}")
        return _projects_cache

def _fetch_bot_open_id():
    global BOT_OPEN_ID
    try:
        info = lark.get_bot_info()
        BOT_OPEN_ID = info.get("open_id", "")
        logger.info(f"Bot open_id: {BOT_OPEN_ID}")
    except Exception as e:
        logger.warning(f"Bot info error: {e}")

# =========================================================================
# FEATURE 1 - NOTIFY BUTTON
# =========================================================================

def build_notify_card(order_num, client, assigned_to, table_id, record_id, image_key=""):
    color = "orange" if assigned_to == "Hannah" else "red"
    link = record_link(table_id, record_id)
    action_id = f"notify_viewed_{table_id}_{record_id}"
    elements = [{"tag": "markdown", "content": f"**Sales Order:** {order_num}\n**Client:** {client}\n**Assigned To:** {assigned_to}"}]
    if image_key:
        elements.append({"tag": "img", "img_key": image_key, "alt": {"tag": "plain_text", "content": "Production Artwork"}})
    if _is_action_clicked(action_id):
        elements.append({"tag": "action", "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "Viewed \u2713"}, "type": "default", "disabled": True}]})
    else:
        elements.append({"tag": "action", "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udc41 Mark as Viewed"}, "type": "primary", "value": {"action": action_id}}]})
    elements.append({"tag": "markdown", "content": f"[Open Record]({link})"})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udce2 Notify: {order_num} - {client}"}, "template": color}, "elements": elements}

def handle_notify_button(table_id, record_id):
    try:
        record = lark.get_record(table_id, record_id)
        fields = record.get("fields", {})
        order_num = field_to_text(fields.get(FIELD_ORDER_NUM, ""))
        client = field_to_text(fields.get(FIELD_CLIENT, ""))
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            assigned_to = get_assigned_from_table("")
        image_key = get_image_key_from_field(fields)
        card = build_notify_card(order_num, client, assigned_to, table_id, record_id, image_key)
        if FOUNDERS_CHAT:
            lark.send_card(card, chat_id=FOUNDERS_CHAT)
            logger.info(f"Notify card sent for {order_num}")
        return {"status": "ok", "order": order_num}
    except Exception as e:
        logger.error(f"Notify error: {e}")
        return {"status": "error", "detail": str(e)}

# =========================================================================
# FEATURE 2 - UPDATE TEAM BUTTON
# =========================================================================

def build_update_team_card(order_num, description, assigned_to, table_id, record_id):
    link = record_link(table_id, record_id)
    action_id = f"mark_resolved_{table_id}_{record_id}"
    elements = [{"tag": "markdown", "content": f"**Sales Order:** {order_num}\n**Description:** {description}\n**Updated by:** {assigned_to}"}]
    view_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udcce View Record"}, "type": "default", "url": link}
    if _is_action_clicked(action_id):
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "Resolved \u2713"}, "type": "default", "disabled": True}
    else:
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark Resolved"}, "type": "primary", "value": {"action": action_id, "order_num": order_num, "assigned_to": assigned_to}}
    elements.append({"tag": "action", "actions": [view_btn, resolve_btn]})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udce9 Project Updated \u2014 {order_num}"}, "template": "purple"}, "elements": elements}

def build_status_request_card(order_num, assigned_to, table_id, record_id, image_key=""):
    link = record_link(table_id, record_id)
    action_id = f"mark_updated_{table_id}_{record_id}"
    elements = [{"tag": "markdown", "content": f"**\ud83d\udcca Status Update Requested**\n\n**Sales Order:** {order_num}\n**Requested by:** Brendan\n\nPlease provide an update on the production status in the comments and fill in an estimated ship date for Brendan. Please be mindful of the in-hands date and if there is any issue, notify Brendan."}]
    if image_key:
        elements.append({"tag": "img", "img_key": image_key, "alt": {"tag": "plain_text", "content": "Production Artwork"}})
    view_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udcce View Record"}, "type": "default", "url": link}
    update_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark as Updated"}, "type": "primary", "value": {"action": action_id, "order_num": order_num, "assigned_to": assigned_to, "table_id": table_id, "record_id": record_id}}
    elements.append({"tag": "action", "actions": [view_btn, update_btn]})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udcca Status Request \u2014 {order_num}"}, "template": "orange"}, "elements": elements}

def handle_update_team_button(table_id, record_id):
    try:
        record = lark.get_record(table_id, record_id)
        fields = record.get("fields", {})
        order_num = field_to_text(fields.get(FIELD_ORDER_NUM, ""))
        description = field_to_text(fields.get(FIELD_DESCRIPTION, ""))
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            tables = lark.get_all_tables()
            for t in tables:
                if t.get("table_id") == table_id:
                    assigned_to = get_assigned_from_table(t.get("name", ""))
                    break
        card = build_update_team_card(order_num, description, assigned_to, table_id, record_id)
        target = FOUNDERS_CHAT
        if target:
            lark.send_card(card, chat_id=target)
        logger.info(f"Update Team card for {order_num} to {assigned_to}")
        return {"status": "ok", "order": order_num, "routed_to": assigned_to}
    except Exception as e:
        logger.error(f"Update Team error: {e}")
        return {"status": "error", "detail": str(e)}


def handle_status_request_button(table_id, record_id):
    try:
        record = lark.get_record(table_id, record_id)
        fields = record.get("fields", {})
        order_num = field_to_text(fields.get(FIELD_ORDER_NUM, ""))
        logger.info(f"Record fields for {table_id}/{record_id}: {list(fields.keys())}")
        if not order_num:
            for alt_field in ["Sales Order", "Order Number", "SO#", "Order#", "order_num"]:
                order_num = field_to_text(fields.get(alt_field, ""))
                if order_num:
                    break
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            tables = lark.get_all_tables()
            for t in tables:
                if t.get("table_id") == table_id:
                    assigned_to = get_assigned_from_table(t.get("name", ""))
                    break
        image_key = get_image_key_from_field(fields)
        card = build_status_request_card(order_num, assigned_to, table_id, record_id, image_key)
        target = LARK_CHAT_ID_HANNAH if assigned_to == "Hannah" else (LARK_CHAT_ID_LUCY if assigned_to == "Lucy" else FOUNDERS_CHAT)
        if target:
            try:
                lark.send_card(card, chat_id=target)
            except Exception as card_err:
                logger.warning(f"Card with image failed, retrying without: {card_err}")
                card = build_status_request_card(order_num, assigned_to, table_id, record_id)
                lark.send_card(card, chat_id=target)
        logger.info(f"Status Request card for {order_num} to {assigned_to}")
        return {"status": "ok", "order": order_num, "assigned_to": assigned_to}
    except Exception as e:
        logger.error(f"Status Request error: {e}")
        return {"status": "error", "detail": str(e)}

# =========================================================================
# FEATURE 3 - MORNING DIGEST
# =========================================================================

def build_morning_digest(projects):
    today = datetime.now(timezone.utc).date()
    status_counts = {}
    waiting_art = []
    due_7 = []
    due_14 = []
    overdue = []
    seen = set()
    for p in projects:
        order_num = field_to_text(p.get(FIELD_ORDER_NUM, ""))
        if not order_num or order_num in seen:
            continue
        seen.add(order_num)
        status = field_to_text(p.get(FIELD_STATUS, "")).strip()
        if not status:
            continue
        status_upper = status.upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        if any(s in status_upper for s in ("SHIPPED", "RESOLVED", "CANCELLED")):
            continue
        client = field_to_text(p.get(FIELD_CLIENT, ""))
        tname = p.get("__table_name__", "")
        tid = p.get("__table_id__", "")
        rid = p.get("__record_id__", "")
        link = record_link(tid, rid) if tid and rid else ""
        due_ms = parse_date_ms(p.get(FIELD_DUE_DATE) or p.get("Due Date") or p.get("In Hand Date", 0))
        due_date = ms_to_date(due_ms)
        if "WAITING ART" in status_upper or "PAID/WAITING" in status_upper:
            waiting_art.append({"order": order_num, "link": link, "client": client})
        if due_date:
            days = (due_date - today).days
            entry = {"order": order_num, "client": client, "board": tname, "date": due_date, "days": days, "status": status, "link": link}
            if days < 0:
                overdue.append(entry)
            elif days <= 7:
                due_7.append(entry)
            elif days <= 14:
                due_14.append(entry)
    overdue.sort(key=lambda x: x["days"])
    due_7.sort(key=lambda x: x["days"])
    due_14.sort(key=lambda x: x["days"])
    s = []
    s.append("**Project Overview**")
    for st, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        s.append(f"  {st}: {c}")
    s.append(f"\n**Need Artwork: {len(waiting_art)} projects**")
    for w in waiting_art:
        s.append(f"  [{w['order']}]({w['link']}) - {w['client']}")
    s.append(f"\n**Overdue: {len(overdue)} projects**")
    for o in overdue:
        s.append(f"  {o['order']} - {o['client']} - {o['board']} - **{abs(o['days'])} days overdue**")
    s.append(f"\n**Due Within 7 Days: {len(due_7)} projects**")
    for d in due_7:
        s.append(f"  {d['order']} - {d['client']} - {d['board']} ({d['days']}d left)")
    s.append(f"\n**Due Within 14 Days: {len(due_14)} projects**")
    for d in due_14:
        s.append(f"  {d['order']} - {d['client']} - {d['board']} ({d['days']}d left)")
    digest = "\n".join(s)
    try:
        prompt = f"Write 2-3 sentences about what Brendan, Hannah, and Lucy should focus on today. Be concise.\n\n{digest}"
        resp = anthropic_client.messages.create(model="claude-sonnet-4-6", max_tokens=500, system="You are Iron Bot. Brief daily focus summary for HLT team.", messages=[{"role": "user", "content": prompt}])
        digest += f"\n\n**Daily Focus**\n{resp.content[0].text.strip()}"
    except Exception as e:
        logger.error(f"Summary error: {e}")
    return digest

# =========================================================================
# FEATURE 4 - DUE DATE ALERTS
# =========================================================================

def send_due_date_alerts():
    projects = fetch_all_projects()
    today = datetime.now(timezone.utc).date()
    alerts_7 = {}
    alerts_14 = {}
    seen = set()
    for p in projects:
        order_num = field_to_text(p.get(FIELD_ORDER_NUM, ""))
        if not order_num or order_num in seen:
            continue
        seen.add(order_num)
        status = field_to_text(p.get(FIELD_STATUS, "")).strip().upper()
        if any(s in status for s in ("SHIPPED", "ARTWORK CONFIRMED", "RESOLVED", "CANCELLED")):
            continue
        due_ms = parse_date_ms(p.get(FIELD_DUE_DATE) or p.get("Due Date") or p.get("In Hand Date", 0))
        due_date = ms_to_date(due_ms)
        if not due_date:
            continue
        days = (due_date - today).days
        if days < 0 or days > 14:
            continue
        tname = p.get("__table_name__", "")
        assigned = get_assigned_to(p)
        if assigned == "Brendan":
            assigned = get_assigned_from_table(tname)
        client = field_to_text(p.get(FIELD_CLIENT, ""))
        tid = p.get("__table_id__", "")
        rid = p.get("__record_id__", "")
        link = record_link(tid, rid)
        entry = {"order": order_num, "client": client, "date": due_date.strftime("%m/%d/%Y"), "days": days, "status": field_to_text(p.get(FIELD_STATUS, "")), "link": link, "tid": tid, "rid": rid}
        if days <= 7:
            alerts_7.setdefault(assigned, []).append(entry)
        else:
            alerts_14.setdefault(assigned, []).append(entry)
    for assigned, entries in alerts_7.items():
        card = _build_alert_card(entries, 7, assigned)
        target = LARK_CHAT_ID_HANNAH if assigned == "Hannah" else (LARK_CHAT_ID_LUCY if assigned == "Lucy" else FOUNDERS_CHAT)
        if target:
            lark.send_card(card, chat_id=target)
    for assigned, entries in alerts_14.items():
        card = _build_alert_card(entries, 14, assigned)
        target = LARK_CHAT_ID_HANNAH if assigned == "Hannah" else (LARK_CHAT_ID_LUCY if assigned == "Lucy" else FOUNDERS_CHAT)
        if target:
            lark.send_card(card, chat_id=target)
    logger.info(f"Due alerts sent: {len(alerts_7)} groups (7d), {len(alerts_14)} groups (14d)")

def _build_alert_card(entries, window, assigned):
    color = "yellow" if window == 7 else "orange"
    title = f"Due Within {window} Days"
    lines = []
    actions = []
    for e in entries:
        lines.append(f"**{e['order']}** - {e['client']} | In-Hand: {e['date']} | {e['days']}d left | {e['status']}")
        lines.append(f"  [View Record]({e['link']})")
        aid = f"ack_{e['tid']}_{e['rid']}"
        if _is_action_clicked(aid):
            actions.append({"tag": "button", "text": {"tag": "plain_text", "content": f"Acknowledged {e['order']}"}, "type": "default", "disabled": True})
        else:
            actions.append({"tag": "button", "text": {"tag": "plain_text", "content": f"Request Update {e['order']}"}, "type": "primary", "value": {"action": aid, "order_num": e["order"], "assigned_to": assigned, "date": e["date"], "status": e["status"]}})
    elements = [{"tag": "markdown", "content": "\n".join(lines)}]
    for i in range(0, len(actions), 3):
        elements.append({"tag": "action", "actions": actions[i:i+3]})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": title}, "template": color}, "elements": elements}

# =========================================================================
# CARD CALLBACK HANDLER
# =========================================================================

def handle_card_callback(body):
    action = body.get("action", {})
    action_value = action.get("value", {})
    action_str = action_value.get("action", "")
    operator = body.get("operator", {})
    operator_id = operator.get("open_id", "")
    operator_name = get_user_name(operator_id)
    logger.info(f"Card callback: {action_str} by {operator_name}")
    if not action_str:
        return {"toast": {"type": "info", "content": "No action"}}
    if action_str.startswith("notify_viewed_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already viewed"}}
        _mark_action_clicked(action_str, operator_name)
        return {"toast": {"type": "success", "content": f"Viewed by {operator_name}"}}
    if action_str.startswith("mark_resolved_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already resolved"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        confirm = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": "Resolved"}, "template": "green"}, "elements": [{"tag": "markdown", "content": f"**{operator_name}** marked **{order_num}** as resolved - {now_str}"}]}
        if FOUNDERS_CHAT:
            threading.Thread(target=lambda: lark.send_card(confirm, chat_id=FOUNDERS_CHAT), daemon=True).start()
        return {"toast": {"type": "success", "content": "Marked resolved"}}
    if action_str.startswith("ack_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already acknowledged"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        date_str = action_value.get("date", "")
        status = action_value.get("status", "")
        confirm = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": "Update Acknowledged"}, "template": "blue"}, "elements": [{"tag": "markdown", "content": f"**{operator_name}** acknowledged **{order_num}** - In-Hand: {date_str} - Status: {status}"}]}
        if FOUNDERS_CHAT:
            threading.Thread(target=lambda: lark.send_card(confirm, chat_id=FOUNDERS_CHAT), daemon=True).start()
        return {"toast": {"type": "success", "content": "Acknowledged"}}
    if action_str.startswith("mark_updated_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already updated"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        assigned_to = action_value.get("assigned_to", "")
        table_id = action_value.get("table_id", "")
        record_id = action_value.get("record_id", "")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        link = record_link(table_id, record_id) if table_id and record_id else ""
        respond_action_id = f"brendan_responded_{table_id}_{record_id}"
        view_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udcce View Record"}, "type": "default", "url": link} if link else None
        respond_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark as Reviewed"}, "type": "primary", "value": {"action": respond_action_id, "order_num": order_num, "assigned_to": assigned_to}}
        confirm_elements = [{"tag": "markdown", "content": f"**{operator_name}** commented on **{order_num}**, please review and respond."}]
        action_buttons = [b for b in [view_btn, respond_btn] if b]
        if action_buttons:
            confirm_elements.append({"tag": "action", "actions": action_buttons})
        confirm = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udce9 Update Received \u2014 {order_num}"}, "template": "green"}, "elements": confirm_elements}
        if FOUNDERS_CHAT:
            threading.Thread(target=lambda: lark.send_card(confirm, chat_id=FOUNDERS_CHAT), daemon=True).start()
        return {"toast": {"type": "success", "content": "Marked as updated"}}

    if action_str.startswith("comment_resolved_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already resolved"}}
        _mark_action_clicked(action_str, operator_name)
        commenter = action_value.get("commenter", "")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        confirm = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": "\u2705 Comment Resolved"}, "template": "green"},
            "elements": [{"tag": "markdown", "content": f"**{operator_name}** resolved comment from **{commenter}** - {now_str}"}],
        }
        target = URGENT_APPROVALS_CHAT or FOUNDERS_CHAT
        if target:
            threading.Thread(target=lambda: lark.send_card(confirm, chat_id=target), daemon=True).start()
        return {"toast": {"type": "success", "content": "Marked as resolved"}}

    if action_str.startswith("brendan_responded_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already responded"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        assigned_to = action_value.get("assigned_to", "")
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        notify = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\u2705 Brendan Reviewed \u2014 {order_num}"}, "template": "green"}, "elements": [{"tag": "markdown", "content": f"**Brendan** sent an update on **{order_num}** - {now_str}"}]}
        target = LARK_CHAT_ID_HANNAH if assigned_to == "Hannah" else (LARK_CHAT_ID_LUCY if assigned_to == "Lucy" else FOUNDERS_CHAT)
        if target:
            threading.Thread(target=lambda: lark.send_card(notify, chat_id=target), daemon=True).start()
        return {"toast": {"type": "success", "content": f"{assigned_to} notified"}}

    return {"toast": {"type": "info", "content": "Processed"}}

# =========================================================================
# BOT CHAT Q&A
# =========================================================================

def get_user_scope(sender_open_id):
    if sender_open_id == HANNAH_OPEN_ID:
        return "hannah"
    if sender_open_id == LUCY_OPEN_ID:
        return "lucy"
    return "brendan"

def extract_question(msg):
    try:
        content = json.loads(msg.get("content", "{}"))
        raw_text = content.get("text", "").strip()
    except Exception:
        return None
    if not raw_text:
        return None
    if msg.get("chat_type", "") == "p2p":
        return raw_text
    mentions = msg.get("mentions", [])
    bot_mentioned = False
    for mention in mentions:
        mid = mention.get("id", {})
        mention_open_id = mid.get("open_id", "")
        mention_name = mention.get("name", "")
        if BOT_OPEN_ID and mention_open_id == BOT_OPEN_ID:
            bot_mentioned = True
            break
        if BOT_NAME and BOT_NAME.lower() in mention_name.lower():
            bot_mentioned = True
            break
    if not bot_mentioned:
        return None
    clean = re.sub(r'@[^\s]+', '', raw_text).strip()
    return clean if clean else raw_text

def build_context(projects):
    lines = [f"Today is {datetime.now(timezone.utc).strftime('%A %B %d %Y')}.", f"Total records: {len(projects)}", ""]
    for p in projects[:200]:
        tname = p.get("__table_name__", "Unknown")
        parts = [f"[Board: {tname}]"]
        for key, val in p.items():
            if key.startswith("__"):
                continue
            parts.append(f"{key}: {field_to_text(val)}")
        lines.append(" | ".join(parts))
    return "\n".join(lines)

def _process_message(user_text, chat_id, scope="brendan", sender_id=""):
    try:
        projects = fetch_all_projects()
        if scope != "brendan":
            projects = [p for p in projects if scope in p.get("__table_name__", "").lower()]
        chat_hist = _get_conversation(chat_id)
        _add_to_conversation(chat_id, "user", user_text)
        context = build_context(projects)
        system_prompt = "You are IRON BOT, HLT internal assistant powered by Claude. Be conversational and proactive. 'Due Date' = 'In Hand Date'. Timestamps are Unix ms."
        user_message = f"--- LARK DATA ---\n{context}\n--- END ---\n\nQuestion: {user_text}"
        response = anthropic_client.messages.create(model="claude-sonnet-4-6", max_tokens=4096, system=system_prompt, messages=(chat_hist or []) + [{"role": "user", "content": user_message}])
        answer = response.content[0].text.strip()
        _add_to_conversation(chat_id, "assistant", answer)
        lark.send_group_message(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Process message error: {e}")
        lark.send_group_message(f"Error: {str(e)[:200]}", chat_id=chat_id)

def _is_already_processed(message_id):
    now = time.time()
    expired = [mid for mid, ts in processed_message_ids.items() if now - ts > DEDUP_TTL]
    for mid in expired:
        del processed_message_ids[mid]
    if message_id in processed_message_ids:
        return True
    processed_message_ids[message_id] = now
    return False

# =========================================================================
# FLASK ROUTES
# =========================================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})
    # --- Debug: log all incoming events ---
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    event = body.get("event", {})
    msg = event.get("message", {})
    chat_id = msg.get("chat_id", "")
    msg_type = msg.get("message_type", "")
    sender = event.get("sender", {})
    sender_type = sender.get("sender_type", "")
    logger.info(f"Webhook: type={event_type}, msg_type={msg_type}, chat_id={chat_id}, sender_type={sender_type}")

    # Store event for debugging
    _recent_events.append({"time": datetime.now(timezone.utc).isoformat(), "event_type": event_type, "msg_type": msg_type, "chat_id": chat_id[:20] if chat_id else "", "sender_type": sender_type, "keys": list(event.keys()) if isinstance(event, dict) else []})
    if len(_recent_events) > MAX_RECENT_EVENTS:
        _recent_events.pop(0)

    # --- Handle comment events (drive.notice.comment_add_v1) ---
    # These events fire when comments are added to docs/base records the bot has access to
    COMMENT_EVENT_TYPES = [
        "drive.notice.comment_add_v1",
        "drive.file.comment_created_v1",
        "drive.file.comment_replied_v1",
        "drive.file.comment_mentioned_v1",
    ]
    if event_type in COMMENT_EVENT_TYPES or ("comment" in event_type.lower() and "approval" not in event_type.lower() and "moments" not in event_type.lower() and "task" not in event_type.lower()):
        logger.info(f"Comment event received: type={event_type}")
        logger.info(f"Comment event payload: {json.dumps(body, default=str)[:2000]}")
        try:
            _forward_comment_to_founders(event_type, event, body)
        except Exception as e:
            logger.error(f"Error forwarding comment event: {e}")
        return jsonify({"code": 0})

    if msg_type != "text":
        return jsonify({"code": 0})
    message_id = msg.get("message_id", "")
    if _is_already_processed(message_id):
        return jsonify({"code": 0})
    user_text = extract_question(msg)
    if not user_text:
        return jsonify({"code": 0})
    chat_id = msg.get("chat_id", "")
    if not chat_id:
        return jsonify({"code": 0})
    sender = event.get("sender", {})
    sender_open_id = sender.get("sender_id", {}).get("open_id", "")
    scope = get_user_scope(sender_open_id)
    threading.Thread(target=_process_message, args=(user_text, chat_id, scope, sender_open_id), daemon=True).start()
    return jsonify({"code": 0})

@app.route("/card-callback", methods=["POST"])
def card_callback():
    body = request.get_json(silent=True) or {}
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})
    return jsonify(handle_card_callback(body))

@app.route("/notify", methods=["POST"])
def notify_endpoint():
    body = request.get_json(silent=True) or {}
    table_id = body.get("table_id", "")
    record_id = body.get("record_id", "")
    if not table_id or not record_id:
        return jsonify({"error": "table_id and record_id required"}), 400
    return jsonify(handle_notify_button(table_id, record_id))

@app.route("/update-team", methods=["POST"])
def update_team_endpoint():
    body = request.get_json(silent=True) or {}
    table_id = body.get("table_id", "")
    record_id = body.get("record_id", "")
    if not table_id or not record_id:
        return jsonify({"error": "table_id and record_id required"}), 400
    return jsonify(handle_update_team_button(table_id, record_id))


@app.route("/status-request", methods=["POST"])
def status_request_endpoint():
    body = request.get_json(silent=True) or {}
    table_id = body.get("table_id", "")
    record_id = body.get("record_id", "")
    if not table_id or not record_id:
        return jsonify({"error": "table_id and record_id required"}), 400
    return jsonify(handle_status_request_button(table_id, record_id))


@app.route("/morning-digest", methods=["POST", "GET"])
def morning_digest():
    if DIGEST_SECRET:
        provided = request.headers.get("X-Digest-Secret", "") or request.args.get("secret", "")
        if provided != DIGEST_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    chat_id = DIGEST_CHAT
    if not chat_id:
        return jsonify({"error": "No digest channel"}), 500
    try:
        projects = fetch_all_projects()
        if not projects:
            return jsonify({"error": "No data"}), 500
        digest = build_morning_digest(projects)
        card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"Morning Digest - {datetime.now(timezone.utc).strftime('%B %d, %Y')}"}, "template": "blue"}, "elements": [{"tag": "markdown", "content": digest}]}
        lark.send_card(card, chat_id=chat_id)
        send_due_date_alerts()
        return jsonify({"status": "ok", "length": len(digest)})
    except Exception as e:
        logger.error(f"Digest error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/event", methods=["POST"])
def event_subscription():
    body = request.get_json(silent=True) or {}
    # Handle URL verification challenge
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})

    # Handle event callback (v2 schema)
    header = body.get("header", {})
    event = body.get("event", {})
    event_type = header.get("event_type", "")

    logger.info(f"Event endpoint received: type={event_type}")
    logger.info(f"Event endpoint payload: {json.dumps(body, default=str)[:2000]}")

    # Route comment events to the handler
    COMMENT_EVENT_TYPES = [
        "drive.notice.comment_add_v1",
        "drive.file.comment_created_v1",
        "drive.file.comment_replied_v1",
        "drive.file.comment_mentioned_v1",
    ]
    if event_type in COMMENT_EVENT_TYPES or ("comment" in event_type.lower() and "approval" not in event_type.lower() and "moments" not in event_type.lower() and "task" not in event_type.lower()):
        try:
            _forward_comment_to_founders(event_type, event, body)
        except Exception as e:
            logger.error(f"Error forwarding comment event: {e}")
        return jsonify({"code": 0})

    return jsonify({"code": 0})


def _forward_comment_to_founders(event_type, event, full_body):
    """Extract comment details from drive.notice.comment_add_v1 or similar events
    and send a card to the Urgent Approvals channel.
    
    Card includes:
    - Who commented and the comment text
    - View Record button (links to the Lark Base record)
    - Mark as Resolved button (updates the card when clicked)
    """
    target_chat = URGENT_APPROVALS_CHAT or FOUNDERS_CHAT
    if not target_chat:
        logger.warning("No urgent approvals/founders chat configured; skipping comment forward")
        return

    # Log full event for debugging
    logger.info(f"Processing comment event: {event_type}")
    logger.info(f"Event keys: {list(event.keys()) if isinstance(event, dict) else type(event)}")

    # --- Extract commenter info ---
    commenter_name = "Someone"
    commenter_open_id = ""
    
    # Try various payload formats
    # Format 1: drive.notice.comment_add_v1
    user_id_info = event.get("user_id", {})
    if isinstance(user_id_info, dict):
        commenter_open_id = user_id_info.get("open_id", "") or user_id_info.get("user_id", "")
    
    # Format 2: direct user fields
    if not commenter_open_id:
        commenter_open_id = event.get("operator_id", {}).get("open_id", "") if isinstance(event.get("operator_id"), dict) else ""
    if not commenter_open_id:
        commenter_open_id = event.get("user_id", "") if isinstance(event.get("user_id"), str) else ""
    
    if commenter_open_id:
        commenter_name = get_user_name(commenter_open_id)
        if commenter_name == "Unknown":
            # Try to get name from Lark API
            try:
                if hasattr(lark, 'get_user_info'):
                    user_info = lark.get_user_info(commenter_open_id)
                    commenter_name = user_info.get("name", commenter_open_id)
                else:
                    commenter_name = commenter_open_id
            except Exception:
                commenter_name = commenter_open_id

    # --- Extract comment content ---
    comment_text = ""
    
    # Try comment.reply_list.replies (standard Drive comment format)
    comment = event.get("comment", {})
    if isinstance(comment, dict):
        reply_list = comment.get("reply_list", {}).get("replies", [])
        if reply_list:
            last_reply = reply_list[-1]
            # Reply content can be nested
            reply_content = last_reply.get("content", {})
            if isinstance(reply_content, dict):
                comment_text = reply_content.get("text", "")
            elif isinstance(reply_content, str):
                comment_text = reply_content
        if not comment_text:
            content_obj = comment.get("content", {})
            if isinstance(content_obj, dict):
                comment_text = content_obj.get("text", "")
            elif isinstance(content_obj, str):
                comment_text = content_obj

    # Try direct content field
    if not comment_text:
        content_obj = event.get("content", {})
        if isinstance(content_obj, dict):
            comment_text = content_obj.get("text", "")
        elif isinstance(content_obj, str):
            comment_text = content_obj

    # Try comment_text directly
    if not comment_text:
        comment_text = event.get("comment_text", "")

    # --- Extract file/record info ---
    file_token = event.get("file_token", "") or event.get("file_key", "")
    file_type = event.get("file_type", "")
    file_name = event.get("file_name", "") or event.get("title", "")
    record_id = event.get("record_id", "")
    table_id = event.get("table_id", "")
    comment_id = ""
    
    # Check for comment_id
    if isinstance(comment, dict):
        comment_id = comment.get("comment_id", "")

    # Try context object
    if not table_id:
        context = event.get("context", {})
        if isinstance(context, dict):
            table_id = context.get("table_id", "")
            if not record_id:
                record_id = context.get("record_id", "")

    # Check if this is a Base (bitable) comment by file_type or file_token
    is_base_comment = file_type == "bitable" or (file_token and file_token == LARK_BASE_APP_TOKEN)

    logger.info(f"Comment details: commenter={commenter_name}, file_token={file_token}, file_type={file_type}, table_id={table_id}, record_id={record_id}, comment_text={comment_text[:100] if comment_text else 'empty'}")

    # --- Build record link ---
    link = ""
    if table_id and record_id:
        link = record_link(table_id, record_id)
    elif file_token and file_token == LARK_BASE_APP_TOKEN:
        # It's our Base but we don't know the table/record, link to the Base
        link = f"{LARK_BASE_RECORD_URL}{LARK_BASE_APP_TOKEN}"
    elif record_id:
        link = f"{LARK_BASE_RECORD_URL}{LARK_BASE_APP_TOKEN}?record={record_id}"
    elif file_token:
        # Try to link to the file
        link = f"https://ojpglhhzxlvc.jp.larksuite.com/base/{file_token}"

    # --- Build the card ---
    action_id = f"comment_resolved_{record_id or file_token or 'unknown'}_{int(time.time())}"

    body_text = f"**{commenter_name}** added a comment"
    if file_name:
        body_text += f" in **{file_name}**"
    if record_id:
        body_text += f" (Record: {record_id})"
    
    elements = [{"tag": "markdown", "content": body_text}]
    
    if comment_text:
        # Clean up comment text - remove @mentions markup if present
        clean_text = re.sub(r'<at [^>]*>([^<]*)</at>', r'@\1', comment_text)
        elements.append({"tag": "markdown", "content": f"> {clean_text[:500]}"})

    # Action buttons: View Record + Mark as Resolved
    buttons = []
    if link:
        buttons.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": "📎 View Record"},
            "type": "default",
            "url": link,
        })
    if _is_action_clicked(action_id):
        buttons.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": "Resolved ✓"},
            "type": "default",
            "disabled": True,
        })
    else:
        buttons.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": "✅ Mark as Resolved"},
            "type": "primary",
            "value": {"action": action_id, "commenter": commenter_name, "record": record_id},
        })
    if buttons:
        elements.append({"tag": "action", "actions": buttons})

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"💬 New Comment — {commenter_name}"},
            "template": "orange",
        },
        "elements": elements,
    }

    lark.send_card(card, chat_id=target_chat)
    logger.info(f"Comment event forwarded to Urgent Approvals: {event_type} by {commenter_name}")

# Store last N events for debugging
_recent_events = []
MAX_RECENT_EVENTS = 50

@app.route("/debug-events", methods=["GET"])
def debug_events():
    return jsonify({"events": _recent_events, "count": len(_recent_events)})

@app.route("/test-comments/<table_id>/<record_id>", methods=["GET"])
def test_comments(table_id, record_id):
        """Try undocumented Bitable comment APIs to see which one works."""
        results = {}
        token = lark.get_tenant_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        base_url = "https://open.larksuite.com/open-apis"
        app_token = LARK_BASE_APP_TOKEN

    # Attempt 1: bitable/v1/apps/{app}/tables/{table}/records/{record}/comments
    url1 = f"{base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}/comments"
    try:
                r1 = requests.get(url1, headers=headers, timeout=10)
                results["attempt1_bitable_record_comments"] = {"status": r1.status_code, "body": r1.json() if r1.status_code < 500 else r1.text[:500]}
except Exception as e:
            results["attempt1_bitable_record_comments"] = {"error": str(e)}

    # Attempt 2: bitable/v1/apps/{app}/tables/{table}/records/{record}/comments/list
    url2 = f"{base_url}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}/comments/list"
    try:
                r2 = requests.get(url2, headers=headers, timeout=10)
                results["attempt2_comments_list"] = {"status": r2.status_code, "body": r2.json() if r2.status_code < 500 else r2.text[:500]}
except Exception as e:
            results["attempt2_comments_list"] = {"error": str(e)}

    # Attempt 3: drive/v1/files/{app_token}/comments?file_type=bitable
    url3 = f"{base_url}/drive/v1/files/{app_token}/comments?file_type=bitable"
    try:
                r3 = requests.get(url3, headers=headers, timeout=10)
                results["attempt3_drive_bitable"] = {"status": r3.status_code, "body": r3.json() if r3.status_code < 500 else r3.text[:500]}
except Exception as e:
            results["attempt3_drive_bitable"] = {"error": str(e)}

    # Attempt 4: drive/v1/files/{app_token}/comments (no file_type)
    url4 = f"{base_url}/drive/v1/files/{app_token}/comments"
    try:
                r4 = requests.get(url4, headers=headers, timeout=10)
                results["attempt4_drive_no_type"] = {"status": r4.status_code, "body": r4.json() if r4.status_code < 500 else r4.text[:500]}
except Exception as e:
            results["attempt4_drive_no_type"] = {"error": str(e)}

    return jsonify(results)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID, "version": "2.0"})

@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({"version": "2.0", "claude_ready": bool(ANTHROPIC_API_KEY), "founders_chat": bool(FOUNDERS_CHAT), "hannah_chat": bool(LARK_CHAT_ID_HANNAH), "lucy_chat": bool(LARK_CHAT_ID_LUCY), "bot_open_id": BOT_OPEN_ID, "features": ["notify", "update_team", "status_request", "morning_digest", "due_date_alerts"]})

@app.route("/test-notify/<table_id>/<record_id>", methods=["GET"])
def test_notify(table_id, record_id):
    return jsonify(handle_notify_button(table_id, record_id))

@app.route("/test-update-team/<table_id>/<record_id>", methods=["GET"])
def test_update_team(table_id, record_id):
    return jsonify(handle_update_team_button(table_id, record_id))

@app.route("/test-status-request/<table_id>/<record_id>", methods=["GET"])
def test_status_request(table_id, record_id):
    return jsonify(handle_status_request_button(table_id, record_id))

@app.route("/test-alerts", methods=["GET"])
def test_alerts():
    try:
        send_due_date_alerts()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/list-chats", methods=["GET"])
def list_chats():
    try:
        chats = lark.list_chats()
        result = []
        for c in chats:
            result.append({"chat_id": c.get("chat_id", ""), "name": c.get("name", ""), "owner_id": c.get("owner_id", "")})
        return jsonify({"chats": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def index():
    return jsonify({"code": 0, "bot": "Iron Bot v2.0", "features": 4})

# =========================================================================
# STARTUP
# =========================================================================
_init_db()
threading.Thread(target=_fetch_bot_open_id, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)


