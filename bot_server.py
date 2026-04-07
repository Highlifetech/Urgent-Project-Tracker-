import os
import logging
import json
import re
import time
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
import anthropic
import psycopg2
import psycopg2.extras
import psycopg2.pool
from lark_client import LarkClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
app = Flask(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BOT_NAME = os.environ.get("BOT_NAME", "Iron Bot")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

from config import (
    LARK_CHAT_ID_HANNAH, LARK_CHAT_ID_LUCY,
    LARK_BASE_APP_TOKEN, LARK_BASE_RECORD_URL,
    FIELD_ORDER_NUM, FIELD_STATUS, FIELD_DESCRIPTION, FIELD_CLIENT,
    FIELD_DUE_DATE, FIELD_PRODUCTION_ARTWORK, FIELD_ASSIGNED_TO,
    FIELD_CLIENT_EMAIL,
    SKIP_STATUSES, DIGEST_EXCLUDED_BOARDS,
    LARK_CHAT_ID_DIGEST, DIGEST_SECRET,
    ALL_ORDERS_VIEW_KEYWORD,
    ALT_ORDER_NUM_FIELDS, ALT_CLIENT_FIELDS, ALT_STATUS_FIELDS, ALT_DUE_DATE_FIELDS,
)

FOUNDERS_CHAT = os.environ.get("LARK_CHAT_ID_FOUNDERS", "")
UPDATES_CHAT = os.environ.get("LARK_CHAT_ID_UPDATES", "")
DIGEST_CHAT = os.environ.get("LARK_CHAT_ID_DIGEST", "")
URGENT_APPROVALS_CHAT = os.environ.get("LARK_CHAT_ID_URGENT_APPROVALS", "")

HANNAH_OPEN_ID = os.environ.get("HANNAH_OPEN_ID", "ou_42c3063bcfefad67c05c615ba0088146")
LUCY_OPEN_ID = os.environ.get("LUCY_OPEN_ID", "ou_0f26700382eae7f58ea889b7e98388b4")
BRENDAN_OPEN_ID = os.environ.get("BRENDAN_OPEN_ID", "")

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
lark = LarkClient()
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
# DATABASE CONNECTION POOL
# =========================================================================
_db_pool = None


def _get_db_pool():
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        try:
            _db_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL, connect_timeout=10)
            logger.info("DB connection pool created")
        except Exception as e:
            logger.error(f"DB pool creation error: {e}")
    return _db_pool


def _get_db_conn():
    pool = _get_db_pool()
    if pool:
        try:
            return pool.getconn()
        except Exception as e:
            logger.error(f"DB pool getconn error: {e}")
    if DATABASE_URL:
        try:
            return psycopg2.connect(DATABASE_URL, connect_timeout=10)
        except Exception as e:
            logger.error(f"DB direct conn error: {e}")
    return None


def _put_db_conn(conn):
    pool = _get_db_pool()
    if pool and conn:
        try:
            pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    elif conn:
        try:
            conn.close()
        except Exception:
            pass


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
            cur.execute("""CREATE TABLE IF NOT EXISTS seen_comments (
                id SERIAL PRIMARY KEY, comment_id TEXT UNIQUE NOT NULL,
                table_id TEXT NOT NULL, record_id TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW())""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_conv_chat ON conversations (chat_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_act ON card_actions (action_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_seen_cmt ON seen_comments (comment_id)")
        conn.commit()
        logger.info("DB tables ready")
    except Exception as e:
        logger.error(f"DB init error: {e}")
    finally:
        _put_db_conn(conn)


def _is_action_clicked(action_id):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM card_actions WHERE action_id = %s", (action_id,))
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        _put_db_conn(conn)


def _mark_action_clicked(action_id, clicked_by=""):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO card_actions (action_id, clicked_by) VALUES (%s, %s) ON CONFLICT (action_id) DO NOTHING", (action_id, clicked_by))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Mark action error: {e}")
        return False
    finally:
        _put_db_conn(conn)


def _is_comment_seen(comment_id):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM seen_comments WHERE comment_id = %s", (comment_id,))
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        _put_db_conn(conn)


def _mark_comment_seen(comment_id, table_id, record_id):
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO seen_comments (comment_id, table_id, record_id) VALUES (%s, %s, %s) ON CONFLICT (comment_id) DO NOTHING", (comment_id, table_id, record_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Mark comment seen error: {e}")
        return False
    finally:
        _put_db_conn(conn)


def _get_conversation(chat_id):
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("DELETE FROM conversations WHERE created_at < NOW() - INTERVAL '%s seconds'", (CONVERSATION_TTL,))
                cur.execute("SELECT role, content FROM conversations WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s", (chat_id, CONVERSATION_MAX_TURNS * 2))
                rows = cur.fetchall()
            conn.commit()
            return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
        except Exception as e:
            logger.error(f"DB read: {e}")
        finally:
            _put_db_conn(conn)
    return _memory_history.get(chat_id, [])


def _add_to_conversation(chat_id, role, content):
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO conversations (chat_id, role, content) VALUES (%s, %s, %s)", (chat_id, role, content[:8000]))
            conn.commit()
            return
        except Exception:
            pass
        finally:
            _put_db_conn(conn)
    if chat_id not in _memory_history:
        _memory_history[chat_id] = []
    _memory_history[chat_id].append({"role": role, "content": content[:8000]})
    if len(_memory_history[chat_id]) > CONVERSATION_MAX_TURNS * 2:
        _memory_history[chat_id] = _memory_history[chat_id][-CONVERSATION_MAX_TURNS * 2:]


# =========================================================================
# HELPERS - Flexible field lookup (tries multiple field names)
# =========================================================================
def _get_field(fields, primary, alternates=None):
    """Try primary field name, then alternates. Returns raw value."""
    val = fields.get(primary)
    if val is not None and val != "" and val != []:
        return val
    if alternates:
        for alt in alternates:
            val = fields.get(alt)
            if val is not None and val != "" and val != []:
                return val
    return ""


def get_order_num(fields):
    return field_to_text(_get_field(fields, FIELD_ORDER_NUM, ALT_ORDER_NUM_FIELDS))


def get_client_name(fields):
    return field_to_text(_get_field(fields, FIELD_CLIENT, ALT_CLIENT_FIELDS))


def get_status(fields):
    return field_to_text(_get_field(fields, FIELD_STATUS, ALT_STATUS_FIELDS))


def get_due_date_raw(fields):
    return _get_field(fields, FIELD_DUE_DATE, ALT_DUE_DATE_FIELDS)


def record_link(table_id, record_id):
    return f"{LARK_BASE_RECORD_URL}{LARK_BASE_APP_TOKEN}?table={table_id}&view=vewGgswDcu&record={record_id}"


def field_to_text(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, (int, float)):
        return str(val)
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
    return str(val)


def get_assigned_to(fields):
    val = fields.get(FIELD_ASSIGNED_TO) or fields.get("Assigned To", "")
    name = field_to_text(val)
    if not name or name == "Brendan":
        return "Brendan"
    if "hannah" in name.lower():
        return "Hannah"
    if "lucy" in name.lower():
        return "Lucy"
    return name


def get_assigned_from_table(table_name):
    tname = (table_name or "").lower()
    if "hannah" in tname:
        return "Hannah"
    if "lucy" in tname:
        return "Lucy"
    return "Brendan"


def get_image_key_from_field(fields, field_name="Production Artwork"):
    val = fields.get(field_name)
    if isinstance(val, list) and val:
        first = val[0]
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
    try:
        info = lark._get(f"/open-apis/contact/v3/users/{open_id}", {"user_id_type": "open_id"})
        return info.get("data", {}).get("user", {}).get("name", open_id[:10])
    except Exception:
        return open_id[:10] if open_id else "Unknown"


def _est_now():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


# =========================================================================
# FETCH ALL PROJECTS
# =========================================================================
def _find_all_orders_view(table_id):
    try:
        views = lark.list_views(table_id) or []
        for v in views:
            vname = (v.get("view_name", "") or "").lower()
            if ALL_ORDERS_VIEW_KEYWORD in vname:
                return v.get("view_id")
    except Exception as e:
        logger.warning(f"list_views error for {table_id}: {str(e)[:60]}")
    return None


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
            view_id = _find_all_orders_view(table_id)
            try:
                records = lark.get_table_records(table_id, view_id=view_id) or []
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
        logger.info(f"Fetched {len(all_records)} records from ALL ORDERS views")
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
# FEATURE 1 - NOTIFY CARD -> Founders Channel
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
        order_num = get_order_num(fields)
        client = get_client_name(fields)
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            assigned_to = get_assigned_from_table("")
        image_key = get_image_key_from_field(fields)
        card = build_notify_card(order_num, client, assigned_to, table_id, record_id, image_key)
        target = FOUNDERS_CHAT
        if target:
            lark.send_card(card, chat_id=target)
            logger.info(f"Notify card sent to Founders Channel for {order_num}")
        return {"status": "ok", "order": order_num}
    except Exception as e:
        logger.error(f"Notify error: {e}")
        return {"status": "error", "detail": str(e)}


# =========================================================================
# FEATURE 2 - UPDATE TEAM CARD -> Hannah/Lucy channels (Purple)
# =========================================================================
def build_update_team_card(order_num, description, assigned_to, table_id, record_id):
    link = record_link(table_id, record_id)
    action_id = f"mark_resolved_{table_id}_{record_id}"
    elements = [{"tag": "markdown", "content": f"**Sales Order:** {order_num}\n**Description:** {description}"}]
    view_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udcce View Record"}, "type": "default", "url": link}
    if _is_action_clicked(action_id):
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "Resolved \u2713"}, "type": "default", "disabled": True}
    else:
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark Resolved"}, "type": "primary", "value": {"action": action_id, "order_num": order_num, "assigned_to": assigned_to}}
    elements.append({"tag": "action", "actions": [view_btn, resolve_btn]})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udce9 Project Update \u2014 {order_num}"}, "template": "purple"}, "elements": elements}


def handle_update_team_button(table_id, record_id):
    try:
        record = lark.get_record(table_id, record_id)
        fields = record.get("fields", {})
        order_num = get_order_num(fields)
        description = field_to_text(fields.get(FIELD_DESCRIPTION, ""))
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            tables = lark.get_all_tables()
            for t in tables:
                if t.get("table_id") == table_id:
                    assigned_to = get_assigned_from_table(t.get("name", ""))
                    break
        card = build_update_team_card(order_num, description, assigned_to, table_id, record_id)
        if assigned_to == "Hannah":
            target = LARK_CHAT_ID_HANNAH
        elif assigned_to == "Lucy":
            target = LARK_CHAT_ID_LUCY
        else:
            target = FOUNDERS_CHAT
        if target:
            lark.send_card(card, chat_id=target)
            logger.info(f"Update Team card for {order_num} sent to {assigned_to} channel")
        return {"status": "ok", "order": order_num}
    except Exception as e:
        logger.error(f"Update Team error: {e}")
        return {"status": "error", "detail": str(e)}


# =========================================================================
# FEATURE 2B - PROJECT UPDATE REQUEST CARD -> Hannah/Lucy with Mark Resolved
# =========================================================================

def build_project_update_request_card(order_num, assigned_to, table_id, record_id, table_name=""):
    """Card asking team member to provide an update, with a Mark Resolved button."""
    link = record_link(table_id, record_id)
    action_id = f"project_update_resolved_{table_id}_{record_id}"

    names = "Hannah and Chen" if assigned_to == "Hannah" else "Lucy" if assigned_to == "Lucy" else "Team"

    elements = [
        {"tag": "markdown", "content": f"Hello {names},\n\nPlease provide an update on the status of order **{order_num}** in the project comments."},
    ]

    add_update_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "Add Update"}, "type": "default", "url": link}

    if _is_action_clicked(action_id):
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "Resolved \u2713"}, "type": "default", "disabled": True}
    else:
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark Resolved"}, "type": "primary", "value": {"action": action_id, "order_num": order_num, "assigned_to": assigned_to}}

    elements.append({"tag": "action", "actions": [add_update_btn, resolve_btn]})

    from_label = table_name or "PRODUCTION"
    elements.append({"tag": "markdown", "content": f"From [2026 {from_label.upper()}]({link})"})

    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "Project Update Request"}, "template": "purple"},
        "elements": elements,
    }


def handle_request_update_button(table_id, record_id):
    """Send a Project Update Request card to the assigned person's channel."""
    try:
        record = lark.get_record(table_id, record_id)
        fields = record.get("fields", {})
        order_num = get_order_num(fields)
        assigned_to = get_assigned_to(fields)
        if assigned_to == "Brendan":
            tables = lark.get_all_tables()
            for t in tables:
                if t.get("table_id") == table_id:
                    assigned_to = get_assigned_from_table(t.get("name", ""))
                    break

        table_name = ""
        try:
            tables = lark.get_all_tables()
            for t in tables:
                if t.get("table_id") == table_id:
                    table_name = t.get("name", "")
                    break
        except Exception:
            pass

        card = build_project_update_request_card(order_num, assigned_to, table_id, record_id, table_name)

        if assigned_to == "Hannah":
            target = LARK_CHAT_ID_HANNAH
        elif assigned_to == "Lucy":
            target = LARK_CHAT_ID_LUCY
        else:
            target = FOUNDERS_CHAT

        if target:
            lark.send_card(card, chat_id=target)
            logger.info(f"Project Update Request sent for {order_num} to {assigned_to} channel")

        return {"status": "ok", "order": order_num}
    except Exception as e:
        logger.error(f"Request Update error: {e}")
        return {"status": "error", "detail": str(e)}


# =========================================================================
# FEATURE 3 - MORNING DIGEST (uses flexible field lookups)
# =========================================================================
def build_morning_digest(projects):
    today = datetime.now(timezone.utc).date()
    status_counts = {}
    waiting_art = []
    due_7 = []
    due_14 = []
    overdue = []
    seen = set()
    person_projects = {"Brendan": [], "Hannah": [], "Lucy": []}
    skipped_no_order = 0
    skipped_no_status = 0

    for p in projects:
        order_num = get_order_num(p)
        if not order_num:
            skipped_no_order += 1
            continue
        if order_num in seen:
            continue
        seen.add(order_num)

        status = get_status(p)
        if not status:
            skipped_no_status += 1
            continue
        status_upper = status.upper()
        status_counts[status] = status_counts.get(status, 0) + 1

        if any(s in status_upper for s in ("SHIPPED", "RESOLVED", "CANCELLED")):
            continue

        client = get_client_name(p)
        tname = p.get("__table_name__", "")
        tid = p.get("__table_id__", "")
        rid = p.get("__record_id__", "")
        link = record_link(tid, rid)
        assigned = get_assigned_to(p)
        if assigned == "Brendan":
            assigned = get_assigned_from_table(tname)

        due_raw = get_due_date_raw(p)
        due_ms = parse_date_ms(due_raw)
        due_date = ms_to_date(due_ms)

        # Note: PENDING ARTWORK is the default status in Lark, not an active artwork request
        # Need Artwork section is kept as a placeholder for future manual flagging

        if due_date:
            days = (due_date - today).days
            entry = {"order": order_num, "client": client, "board": tname, "date": due_date, "days": days, "status": status, "link": link, "assigned": assigned}
            if days < 0:
                overdue.append(entry)
            elif days <= 7:
                due_7.append(entry)
            elif days <= 14:
                due_14.append(entry)
            if assigned in person_projects:
                person_projects[assigned].append({"order": order_num, "client": client, "status": status, "days": days, "board": tname})

    overdue.sort(key=lambda x: x["days"])
    due_7.sort(key=lambda x: x["days"])
    due_14.sort(key=lambda x: x["days"])

    total_active = sum(v for k, v in status_counts.items() if k.upper() not in ("SHIPPED", "RESOLVED", "CANCELLED"))

    logger.info(f"Digest stats: {len(seen)} unique orders, {skipped_no_order} skipped (no order#), {skipped_no_status} skipped (no status), {total_active} active")

    s = [f"**\ud83d\udcca Project Overview** | Active Projects: **{total_active}**"]
    for st, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        su = st.upper()
        if su in ("QUOTE NEEDED", "IN PRODUCTION", "PART SHIPPED", "SHIPPED"):
            emoji = "\ud83d\udd35"
        elif su in ("QUOTED", "PART CONFIRMED"):
            emoji = "\ud83d\udfe2"
        elif su in ("PENDING ARTWORK", "ARTWORK CONFIRMED"):
            emoji = "\ud83d\udfe0"
        elif su == "ON HOLD":
            emoji = "\ud83d\udfe1"
        elif su in ("NEEDS REVISION", "NEEDS RESOLUTION"):
            emoji = "\ud83d\udfe3"
        elif su == "CANCELLED":
            emoji = "\ud83d\udd34"
        else:
            emoji = "\u26aa"
        s.append(f"  {emoji} {st}: **{c}**")

    s.append(f"\n**\ud83c\udfa8 Need Artwork \u2014 {len(waiting_art)} projects**")
    if waiting_art:
        for w in waiting_art:
            s.append(f"  [{w['order']}]({w['link']}) \u2014 {w['client']} ({w.get('board', '')})")
    else:
        s.append("  No artwork-pending orders. Clear on this front.")

    s.append(f"\n**\ud83d\udea8 Overdue \u2014 {len(overdue)} projects**")
    for o in overdue:
        s.append(f"  **{o['order']}** | {o['status']} | **{abs(o['days'])} days overdue** \u2014 {o['client']} \u2014 {o['board']} - [View]({o['link']})")

    s.append(f"\n**\u23f0 Due Within 7 Days \u2014 {len(due_7)} projects**")
    for d in due_7:
        label = "**TODAY**" if d["days"] == 0 else f"due in {d['days']}d"
        s.append(f"  **{d['order']}** | {d['status']} | {label} \u2014 {d['client']} \u2014 {d['board']} - [View]({d['link']})")

    s.append(f"\n**\ud83d\udcc5 Due Within 14 Days \u2014 {len(due_14)} projects**")
    for d in due_14:
        s.append(f"  **{d['order']}** | {d['status']} | due in {d['days']}d \u2014 {d['client']} \u2014 {d['board']} - [View]({d['link']})")

    digest = "\n".join(s)

    try:
        brendan_summary = _person_summary("Brendan", person_projects.get("Brendan", []))
        hannah_summary = _person_summary("Hannah", person_projects.get("Hannah", []))
        lucy_summary = _person_summary("Lucy", person_projects.get("Lucy", []))
        summary_data = f"Overdue: {len(overdue)}, Due 7d: {len(due_7)}, Due 14d: {len(due_14)}, Waiting Art: {len(waiting_art)}\nBrendan: {brendan_summary}\nHannah: {hannah_summary}\nLucy: {lucy_summary}"
        prompt = f"Write a brief daily focus summary for the HLT production team. Address what Brendan, Hannah, and Lucy each need to focus on today. Flag anything urgent. Be direct, 3-5 sentences.\n\n{summary_data}"
        resp = anthropic_client.messages.create(model="claude-sonnet-4-6", max_tokens=600, system="You are Iron Bot, HLT's production assistant.", messages=[{"role": "user", "content": prompt}])
        digest += f"\n\n**\ud83d\udcdd Daily Focus**\n{resp.content[0].text.strip()}"
    except Exception as e:
        logger.error(f"AI summary error: {e}")

    return digest


def _person_summary(name, projects):
    if not projects:
        return "No active projects"
    overdue_count = sum(1 for p in projects if p.get("days") is not None and p["days"] < 0)
    due_soon = sum(1 for p in projects if p.get("days") is not None and 0 <= p["days"] <= 7)
    total = len(projects)
    parts = [f"{total} active"]
    if overdue_count:
        parts.append(f"{overdue_count} OVERDUE")
    if due_soon:
        parts.append(f"{due_soon} due within 7d")
    return ", ".join(parts)


# =========================================================================
# FEATURE 4 - DUE DATE ALERTS with REQUEST UPDATE
# =========================================================================
def send_due_date_alerts():
    projects = fetch_all_projects()
    today = datetime.now(timezone.utc).date()
    alerts_7 = {}
    alerts_14 = {}
    seen = set()
    for p in projects:
        order_num = get_order_num(p)
        if not order_num or order_num in seen:
            continue
        seen.add(order_num)
        status = get_status(p).upper()
        if any(s in status for s in ("SHIPPED", "ARTWORK CONFIRMED", "RESOLVED", "CANCELLED")):
            continue
        due_ms = parse_date_ms(get_due_date_raw(p))
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
        client = get_client_name(p)
        tid = p.get("__table_id__", "")
        rid = p.get("__record_id__", "")
        link = record_link(tid, rid)
        entry = {"order": order_num, "client": client, "date": due_date.strftime("%m/%d/%Y"), "days": days, "status": get_status(p), "link": link, "tid": tid, "rid": rid}
        if days <= 7:
            alerts_7.setdefault(assigned, []).append(entry)
        else:
            alerts_14.setdefault(assigned, []).append(entry)
    for assigned, entries in alerts_7.items():
        # Only send separate alert cards to Hannah/Lucy channels
        # Brendan's alerts are already in the morning digest
        if assigned == "Hannah" and LARK_CHAT_ID_HANNAH:
            lark.send_card(_build_alert_card(entries, 7, assigned), chat_id=LARK_CHAT_ID_HANNAH)
        elif assigned == "Lucy" and LARK_CHAT_ID_LUCY:
            lark.send_card(_build_alert_card(entries, 7, assigned), chat_id=LARK_CHAT_ID_LUCY)
    for assigned, entries in alerts_14.items():
        if assigned == "Hannah" and LARK_CHAT_ID_HANNAH:
            lark.send_card(_build_alert_card(entries, 14, assigned), chat_id=LARK_CHAT_ID_HANNAH)
        elif assigned == "Lucy" and LARK_CHAT_ID_LUCY:
            lark.send_card(_build_alert_card(entries, 14, assigned), chat_id=LARK_CHAT_ID_LUCY)
    logger.info(f"Due alerts sent: {len(alerts_7)} groups (7d), {len(alerts_14)} groups (14d)")


def _build_alert_card(entries, window, assigned):
    color = "yellow" if window == 7 else "orange"
    title = f"Due Within {window} Days"
    lines = []
    for e in entries:
        days_label = "**TODAY**" if e["days"] == 0 else f"{e['days']}d left"
        lines.append(f"**{e['order']}** \u2014 {e['client']} | In-Hand: {e['date']} | {days_label} | {e['status']}")
        lines.append(f"  [View Record]({e['link']})")
    elements = [{"tag": "markdown", "content": "\n".join(lines)}]
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\u26a0\ufe0f {title} \u2014 {assigned}"}, "template": color}, "elements": elements}

# =========================================================================
# FEATURE 6 - COMMENT ALERTS -> Urgent/Approvals channel
# =========================================================================
def check_new_comments():
    if not URGENT_APPROVALS_CHAT:
        logger.warning("No URGENT_APPROVALS_CHAT set")
        return
    try:
        tables = lark.get_all_tables()
    except Exception as e:
        logger.error(f"Comment check - tables error: {e}")
        return
    new_count = 0
    errors = 0
    for table in tables:
        table_id = table.get("table_id", "")
        table_name = table.get("name", "")
        if not table_id or _is_excluded_board(table_name):
            continue
        try:
            records = lark.get_table_records(table_id) or []
        except Exception:
            errors += 1
            continue
        for rec in records[:50]:
            record_id = rec.get("record_id", "")
            if not record_id:
                continue
            try:
                comments = lark.get_record_comments(table_id, record_id)
            except Exception:
                continue
            if not comments:
                continue
            fields = rec.get("fields", {})
            order_num = get_order_num(fields)
            for comment in comments:
                cid = comment.get("comment_id", "")
                if not cid or _is_comment_seen(cid):
                    continue
                user_name = comment.get("user_name", "Unknown")
                if user_name.lower() not in ("hannah", "lucy"):
                    _mark_comment_seen(cid, table_id, record_id)
                    continue
                _mark_comment_seen(cid, table_id, record_id)
                content_text = comment.get("content", "")
                link = record_link(table_id, record_id)
                action_id = f"comment_resolved_{table_id}_{record_id}_{cid}"
                card = _build_comment_card(order_num, table_name, user_name, content_text, link, action_id)
                try:
                    lark.send_card(card, chat_id=URGENT_APPROVALS_CHAT)
                    new_count += 1
                except Exception as e:
                    logger.error(f"Comment card send error: {e}")
                    errors += 1
    logger.info(f"Comment check: {new_count} new, {errors} errors")


def _build_comment_card(order_num, table_name, user_name, content_text, link, action_id):
    display_order = order_num or "Unknown Record"
    elements = [{"tag": "markdown", "content": f"**From:** {user_name}\n**Record:** {display_order}\n**Board:** {table_name}\n\n{content_text[:500]}"}]
    view_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\ud83d\udd17 View Record"}, "type": "default", "url": link}
    if _is_action_clicked(action_id):
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "Resolved \u2713"}, "type": "default", "disabled": True}
    else:
        resolve_btn = {"tag": "button", "text": {"tag": "plain_text", "content": "\u2705 Mark as Resolved"}, "type": "primary", "value": {"action": action_id}}
    elements.append({"tag": "action", "actions": [view_btn, resolve_btn]})
    return {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83d\udcac Comment: {display_order} \u2014 {user_name}"}, "template": "turquoise"}, "elements": elements}


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
        if FOUNDERS_CHAT:
            now_str = _est_now().strftime("%I:%M %p ET, %b %d")
            confirm_card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": "\u2705 Resolved"}, "template": "green"}, "elements": [{"tag": "markdown", "content": f"**{operator_name}** marked **{order_num}** as resolved \u2014 {now_str}"}]}
            lark.send_card(confirm_card, chat_id=FOUNDERS_CHAT)
        return {"toast": {"type": "success", "content": f"Resolved by {operator_name}"}}

    if action_str.startswith("project_update_resolved_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already resolved"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        assigned_to = action_value.get("assigned_to", "")
        if FOUNDERS_CHAT:
            now_str = _est_now().strftime("%I:%M %p ET, %b %d")
            confirm_card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": "\u2705 Update Request Resolved"}, "template": "green"}, "elements": [{"tag": "markdown", "content": f"**{operator_name}** resolved the update request for **{order_num}** \u2014 {now_str}"}]}
            lark.send_card(confirm_card, chat_id=FOUNDERS_CHAT)
        return {"toast": {"type": "success", "content": f"Resolved by {operator_name}"}}

    if action_str.startswith("request_update_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already acknowledged"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        date_str = action_value.get("date", "")
        status_str = action_value.get("status", "")
        if FOUNDERS_CHAT:
            now_str = _est_now().strftime("%I:%M %p ET, %b %d")
            ack_card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": "\ud83d\udcdd Update Acknowledged"}, "template": "blue"}, "elements": [{"tag": "markdown", "content": f"**{operator_name}** acknowledged **{order_num}**\nIn-Hand Date: {date_str}\nCurrent Status: {status_str}\nTime: {now_str}"}]}
            lark.send_card(ack_card, chat_id=FOUNDERS_CHAT)
        return {"toast": {"type": "success", "content": "Acknowledged"}}

    if action_str.startswith("comment_resolved_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already resolved"}}
        _mark_action_clicked(action_str, operator_name)
        return {"toast": {"type": "success", "content": f"Comment resolved by {operator_name}"}}

    if action_str.startswith("mark_updated_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already updated"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        if FOUNDERS_CHAT:
            lark.send_text(f"\ud83d\udcca {operator_name} updated status for {order_num}", chat_id=FOUNDERS_CHAT)
        return {"toast": {"type": "success", "content": "Marked as updated"}}

    if action_str.startswith("artwork_sent_"):
        if _is_action_clicked(action_str):
            return {"toast": {"type": "info", "content": "Already marked as sent"}}
        _mark_action_clicked(action_str, operator_name)
        order_num = action_value.get("order_num", "")
        if FOUNDERS_CHAT:
            lark.send_text(f"\ud83c\udfa8 {operator_name} sent artwork for {order_num}", chat_id=FOUNDERS_CHAT)
        return {"toast": {"type": "success", "content": f"Artwork sent by {operator_name}"}}

    return {"toast": {"type": "info", "content": "Unknown action"}}


# =========================================================================
# AI CHAT
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
    clean = re.sub(r"@[^\s]+", "", raw_text).strip()
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
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    event = body.get("event", {})
    msg = event.get("message", {})
    msg_type = msg.get("message_type", "")
    sender = event.get("sender", {})
    if event_type != "im.message.receive_v1" or msg_type != "text":
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
    sender_open_id = sender.get("sender_id", {}).get("open_id", "")
    scope = get_user_scope(sender_open_id)
    threading.Thread(target=_process_message, args=(user_text, chat_id, scope, sender_open_id), daemon=True).start()
    return jsonify({"code": 0})


@app.route("/card-callback", methods=["POST"])
def card_callback():
    body = request.get_json(silent=True) or {}
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})
    result = handle_card_callback(body)
    return jsonify(result)


@app.route("/notify/<table_id>/<record_id>", methods=["POST", "GET"])
def notify_endpoint(table_id, record_id):
    return jsonify(handle_notify_button(table_id, record_id))


@app.route("/update-team/<table_id>/<record_id>", methods=["POST", "GET"])
def update_team_endpoint(table_id, record_id):
    return jsonify(handle_update_team_button(table_id, record_id))

@app.route("/request-update/<table_id>/<record_id>", methods=["POST", "GET"])
def request_update_endpoint(table_id, record_id):
    return jsonify(handle_request_update_button(table_id, record_id))


@app.route("/morning-digest", methods=["POST", "GET"])
def morning_digest():
    global _last_digest_sent
    if DIGEST_SECRET:
        provided = request.headers.get("X-Digest-Secret", "") or request.args.get("secret", "")
        if provided != DIGEST_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    now_ts = time.time()
    if now_ts - _last_digest_sent < 3600:
        logger.info(f"HTTP /morning-digest: Already sent {int(now_ts - _last_digest_sent)}s ago, skipping duplicate")
        return jsonify({"status": "skipped", "reason": "digest already sent within the last hour"}), 200
    _last_digest_sent = now_ts
    chat_id = DIGEST_CHAT or FOUNDERS_CHAT
    if not chat_id:
        return jsonify({"error": "No digest channel configured"}), 500
    try:
        global _projects_cache_time
        _projects_cache_time = 0
        projects = fetch_all_projects()
        if not projects:
            return jsonify({"error": "No data"}), 500
        digest = build_morning_digest(projects)
        now_str = _est_now().strftime("%A, %B %d, %Y")
        total = len(projects)
        card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83c\udf05 IRON BOT MORNING BRIEFING"}, "template": "blue"}, "elements": [{"tag": "markdown", "content": f"**{now_str}** | HLT Active Projects: **{total}**\n---"}, {"tag": "markdown", "content": digest}]}
        lark.send_card(card, chat_id=chat_id)
        send_due_date_alerts()
        return jsonify({"status": "ok", "records": total})
    except Exception as e:
        logger.error(f"Digest error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/check-comments", methods=["POST", "GET"])
def check_comments_endpoint():
    if DIGEST_SECRET:
        provided = request.headers.get("X-Digest-Secret", "") or request.args.get("secret", "")
        if provided != DIGEST_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    threading.Thread(target=check_new_comments, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/debug-fields", methods=["GET"])
def debug_fields():
    if DIGEST_SECRET:
        provided = request.args.get("secret", "")
        if provided != DIGEST_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    global _projects_cache_time
    _projects_cache_time = 0
    projects = fetch_all_projects()
    if not projects:
        return jsonify({"error": "No projects"})
    field_names_all = set()
    for p in projects[:50]:
        for k in p.keys():
            if not k.startswith("__"):
                field_names_all.add(k)
    raw_samples = []
    for p in projects[:15]:
        tname = p.get("__table_name__", "")
        if _is_excluded_board(tname):
            continue
        sample = {"__table__": tname}
        for fname in [FIELD_ORDER_NUM, FIELD_STATUS, FIELD_CLIENT, FIELD_DUE_DATE]:
            raw = p.get(fname)
            sample["RAW_" + fname] = str(type(raw).__name__) + "|" + repr(raw)[:200] if raw is not None else "MISSING"
        sample["parsed_order"] = get_order_num(p)
        sample["parsed_status"] = get_status(p)
        sample["parsed_client"] = get_client_name(p)
        sample["parsed_due"] = str(get_due_date_raw(p))[:100]
        raw_samples.append(sample)
        if len(raw_samples) >= 3:
            break
    seen = set()
    skip_no_order = 0
    skip_no_status = 0
    has_order = 0
    has_status = 0
    status_vals = {}
    for p in projects:
        on = get_order_num(p)
        if not on:
            skip_no_order += 1
            continue
        if on in seen:
            continue
        seen.add(on)
        has_order += 1
        st = get_status(p)
        if not st:
            skip_no_status += 1
        else:
            has_status += 1
            status_vals[st] = status_vals.get(st, 0) + 1
    return jsonify({"total": len(projects), "field_names": sorted(list(field_names_all)), "config": {"ORDER": FIELD_ORDER_NUM, "STATUS": FIELD_STATUS, "CLIENT": FIELD_CLIENT, "DUE": FIELD_DUE_DATE}, "raw_samples": raw_samples, "digest_sim": {"unique_with_order": has_order, "skip_no_order": skip_no_order, "skip_no_status": skip_no_status, "has_status": has_status, "top_statuses": dict(sorted(status_vals.items(), key=lambda x: -x[1])[:15])}})



@app.route("/debug-artwork", methods=["GET"])
def debug_artwork():
    if DIGEST_SECRET:
        provided = request.args.get("secret", "")
        if provided != DIGEST_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
    global _projects_cache_time
    _projects_cache_time = 0
    projects = fetch_all_projects()
    if not projects:
        return jsonify({"error": "No projects"})
    seen = set()
    artwork_projects = []
    for p in projects:
        order_num = get_order_num(p)
        if not order_num or order_num in seen:
            continue
        seen.add(order_num)
        status = get_status(p)
        if not status:
            continue
        status_upper = status.upper()
        if status_upper == "PENDING ARTWORK":
            artwork_projects.append({
                "order": order_num,
                "status_raw": status,
                "status_upper": status_upper,
                "client": get_client_name(p),
                "board": p.get("__table_name__", ""),
                "table_id": p.get("__table_id__", ""),
                "record_id": p.get("__record_id__", "")
            })
    return jsonify({"total_records": len(projects), "pending_artwork_count": len(artwork_projects), "projects": artwork_projects})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot": BOT_NAME, "bot_open_id": BOT_OPEN_ID or "loading", "version": "4.7"})


@app.route("/", methods=["GET"])
def index():
    return jsonify({"code": 0, "bot": "Iron Bot v4.7", "features": ["notify", "update-team", "digest", "due-alerts", "comment-alerts", "ai-chat"]})


# =========================================================================
# STARTUP — guarded to prevent double-init
# =========================================================================
_background_started = False
_last_digest_sent = 0  # Unix timestamp of last digest send (dedup guard)

COMMENT_POLL_INTERVAL = int(os.environ.get("COMMENT_POLL_INTERVAL", "300"))


def _comment_poll_loop():
    time.sleep(30)
    while True:
        try:
            logger.info("Comment poll loop: starting check...")
            check_new_comments()
        except Exception as e:
            logger.error(f"Comment poll loop error: {e}")
        time.sleep(COMMENT_POLL_INTERVAL)


# =========================================================================
# BUILT-IN DAILY SCHEDULER (runs inside Railway, no GitHub Actions needed)
# =========================================================================
def _scheduled_morning_digest():
    """Triggered by APScheduler at 8am ET Mon-Fri."""
    global _last_digest_sent
    now_ts = time.time()
    if now_ts - _last_digest_sent < 3600:
        logger.info(f"SCHEDULER: Digest already sent {int(now_ts - _last_digest_sent)}s ago, skipping duplicate")
        return
    _last_digest_sent = now_ts
    logger.info("SCHEDULER: Morning digest triggered at 8am ET")
    try:
        global _projects_cache_time
        _projects_cache_time = 0
        projects = fetch_all_projects()
        if not projects:
            logger.error("SCHEDULER: No projects fetched")
            return
        digest = build_morning_digest(projects)
        now_str = _est_now().strftime("%A, %B %d, %Y")
        total = len(projects)
        card = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": "\ud83c\udf05 IRON BOT MORNING BRIEFING"}, "template": "blue"},
            "elements": [
                {"tag": "markdown", "content": f"**{now_str}** | HLT Active Projects: **{total}**\n---"},
                {"tag": "markdown", "content": digest},
            ],
        }
        chat_id = DIGEST_CHAT or FOUNDERS_CHAT
        if chat_id:
            lark.send_card(card, chat_id=chat_id)
            logger.info(f"SCHEDULER: Digest sent ({total} records)")
        send_due_date_alerts()
        logger.info("SCHEDULER: Due date alerts sent")
    except Exception as e:
        logger.error(f"SCHEDULER: Digest error: {e}")


def _start_background_tasks():
    """Initialize DB, bot info, comment polling, and scheduler. Only runs once."""
    global _background_started
    if _background_started:
        logger.info("Background tasks already started, skipping")
        return
    _background_started = True

    _init_db()
    threading.Thread(target=_fetch_bot_open_id, daemon=True).start()

    if URGENT_APPROVALS_CHAT:
        threading.Thread(target=_comment_poll_loop, daemon=True).start()
        logger.info(f"Comment polling started (interval={COMMENT_POLL_INTERVAL}s)")

    try:
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            _scheduled_morning_digest,
            CronTrigger(hour=8, minute=0, day_of_week="mon-fri", timezone="America/New_York"),
            id="morning_digest",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("APScheduler started: morning digest at 8:00 AM ET, Mon-Fri")
    except Exception as e:
        logger.error(f"APScheduler setup error: {e}")


if __name__ == "__main__":
    _start_background_tasks()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
