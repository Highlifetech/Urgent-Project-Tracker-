


# =========================================================================
# BOT CHAT Q&A (existing functionality preserved)
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
        today = datetime.now(timezone.utc)
    lines = [f"Today is {today.strftime('%A %B %d %Y')}.", f"Total records: {len(projects)}", ""]
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
                    system_prompt = (
                                    "You are IRON BOT, the HLT (Highlife Tech) internal assistant powered by Claude. "
                                    "Be conversational, helpful, and proactive. 'Due Date' = 'In Hand Date'. "
                                    "Timestamps in data are Unix milliseconds. Convert to readable dates. "
                                    "STATUS: WAITING ART=awaiting artwork, ARTWORK CONFIRMED=approved, "
                                    "PLATING/POLISHING=in production, SHIPPED=done, NEEDS RESOLUTION=problem."
                    )
                    user_message = f"--- LARK DATA ---\n{context}\n--- END ---\n\nQuestion: {user_text}"
                    response = anthropic_client.messages.create(
                                    model="claude-sonnet-4-6", max_tokens=4096, system=system_prompt,
                                    messages=(chat_hist or []) + [{"role": "user", "content": user_message}])
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
            event = body.get("event", {})
    msg = event.get("message", {})
    if msg.get("message_type") != "text":
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
                result = handle_card_callback(body)
    return jsonify(result)

@app.route("/notify", methods=["POST"])
def notify_endpoint():
        body = request.get_json(silent=True) or {}
    table_id = body.get("table_id", "")
    record_id = body.get("record_id", "")
    if not table_id or not record_id:
                return jsonify({"error": "table_id and record_id required"}), 400
            result = handle_notify_button(table_id, record_id)
    return jsonify(result)

@app.route("/update-team", methods=["POST"])
def update_team_endpoint():
        body = request.get_json(silent=True) or {}
    table_id = body.get("table_id", "")
    record_id = body.get("record_id", "")
    if not table_id or not record_id:
                return jsonify({"error": "table_id and record_id required"}), 400
            result = handle_update_team_button(table_id, record_id)
    return jsonify(result)

@app.route("/morning-digest", methods=["POST", "GET"])
def morning_digest():
        digest_secret = DIGEST_SECRET
    if digest_secret:
                provided = request.headers.get("X-Digest-Secret", "") or request.args.get("secret", "")
                if provided != digest_secret:
                                return jsonify({"error": "Unauthorized"}), 401
                        chat_id = LARK_CHAT_ID_FOUNDERS or LARK_CHAT_ID_DIGEST
    if not chat_id:
                return jsonify({"error": "No digest channel configured"}), 500
    try:
                projects = fetch_all_projects()
        if not projects:
                        return jsonify({"error": "No data"}), 500
        digest = build_morning_digest(projects)
        card = {"config": {"wide_screen_mode": True}, "header": {"title": {"tag": "plain_text", "content": f"\ud83c\udf05 Morning Digest - {datetime.now(timezone.utc).strftime('%B %d, %Y')}"}, "template": "blue"}, "elements": [{"tag": "markdown", "content": digest}]}
        lark.send_card(card, chat_id=chat_id)
        send_due_date_alerts()
        return jsonify({"status": "ok", "length": len(digest)})
except Exception as e:
        logger.error(f"Digest error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/event", methods=["POST"])
def event_subscription():
        body = request.get_json(silent=True) or {}
    if body.get("type") == "url_verification":
                return jsonify({"challenge": body.get("challenge", "")})
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    event = body.get("event", {})
    logger.info(f"Event: {event_type}")
    if event_type == "im.chat.member.bot.added_v1":
                chat_id = event.get("chat_id", "")
        if chat_id:
                        lark.send_text("Hello! I'm IRON BOT. @ mention me with any question.", chat_id=chat_id)
    return jsonify({"code": 0})

@app.route("/health", methods=["GET"])
def health():
        return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID, "version": "2.0"})

@app.route("/debug", methods=["GET"])
def debug():
        return jsonify({
                    "version": "2.0-features",
                    "claude_ready": bool(ANTHROPIC_API_KEY),
                    "founders_chat": bool(LARK_CHAT_ID_FOUNDERS or LARK_CHAT_ID_DIGEST),
                    "hannah_chat": bool(LARK_CHAT_ID_HANNAH),
                    "lucy_chat": bool(LARK_CHAT_ID_LUCY),
                    "bot_open_id": BOT_OPEN_ID,
                    "features": ["notify", "update_team", "morning_digest", "due_date_alerts"],
        })

@app.route("/test-notify/<table_id>/<record_id>", methods=["GET"])
def test_notify(table_id, record_id):
        result = handle_notify_button(table_id, record_id)
    return jsonify(result)

@app.route("/test-update-team/<table_id>/<record_id>", methods=["GET"])
def test_update_team(table_id, record_id):
        result = handle_update_team_button(table_id, record_id)
    return jsonify(result)

@app.route("/test-alerts", methods=["GET"])
def test_alerts():
        try:
                    send_due_date_alerts()
                    return jsonify({"status": "ok"})
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
        app.run(host="0.0.0.0", port=port, debug=False)import os
import logging
import json
import re
import time
import threading
import requests
from datetime import datetime, timezone
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
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
BOT_NAME = os.environ.get("BOT_NAME", "Iron Bot")  # Set to the bot's display name in Lark
DATABASE_URL = os.environ.get("DATABASE_URL", "")
from config import LARK_CHAT_ID_HANNAH_ARTWORK, LARK_CHAT_ID_LUCY_ARTWORK, FIELD_PRODUCTION_DRAWING, ARTWORK_CONFIRMED_STATUS

# Known user open_ids for scoping (set via env vars or defaults)
HANNAH_OPEN_ID = os.environ.get("HANNAH_OPEN_ID", "ou_42c3063bcfefad67c05c615ba0088146")
LUCY_OPEN_ID = os.environ.get("LUCY_OPEN_ID", "ou_0f26700382eae7f58ea889b7e98388b4")

anthropic_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
logger.info("Claude client ready")

# Deduplication: dict of {message_id: timestamp}
processed_message_ids = {}
DEDUP_TTL = 300

# Store last webhook payloads for debugging
_last_webhooks = []

# Bot's own open_id - fetched at startup
BOT_OPEN_ID = None

# -------------------------------------------------------------------------
# Persistent Conversation History (Postgres-backed)
# -------------------------------------------------------------------------
CONVERSATION_MAX_TURNS = 10  # Keep last 10 exchanges per chat
CONVERSATION_TTL = 3600  # Expire messages older than 1 hour

def _get_db_conn():
    """Get a Postgres connection. Returns None if DATABASE_URL not set."""
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        return conn
    except Exception as e:
        logger.error("Postgres connection error: " + str(e))
        return None

def _init_db():
    """Create conversation tables if they don't exist."""
    conn = _get_db_conn()
    if not conn:
        logger.warning("DATABASE_URL not set â conversation memory will be in-memory only (non-persistent)")
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_conversations_chat_id
                ON conversations (chat_id, created_at DESC)
            """)
            conn.commit()
        logger.info("Postgres conversation table ready")
        conn.close()
        return True
    except Exception as e:
        logger.error("DB init error: " + str(e))
        conn.close()
        return False

# In-memory fallback if Postgres is unavailable
_memory_history = {}
_memory_timestamps = {}

def _get_conversation(chat_id):
    """Get conversation history for a chat from Postgres (or in-memory fallback)."""
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Clean up stale messages first
                cur.execute(
                    "DELETE FROM conversations WHERE created_at < NOW() - INTERVAL '%s seconds'",
                    (CONVERSATION_TTL,)
                )
                # Fetch recent messages for this chat
                max_messages = CONVERSATION_MAX_TURNS * 2
                cur.execute(
                    "SELECT role, content FROM conversations WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s",
                    (chat_id, max_messages)
                )
                rows = cur.fetchall()
                conn.commit()
            conn.close()
            # Reverse so oldest first (chronological order)
            return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
        except Exception as e:
            logger.error("DB read error: " + str(e))
            try:
                conn.close()
            except Exception:
                pass
    # Fallback to in-memory
    now = time.time()
    stale = [cid for cid, ts in _memory_timestamps.items() if now - ts > CONVERSATION_TTL]
    for cid in stale:
        _memory_history.pop(cid, None)
        _memory_timestamps.pop(cid, None)
    return _memory_history.get(chat_id, [])

def _add_to_conversation(chat_id, role, content):
    """Add a message to conversation history in Postgres (or in-memory fallback)."""
    conn = _get_db_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO conversations (chat_id, role, content) VALUES (%s, %s, %s)",
                    (chat_id, role, content[:8000])
                )
                # Trim to keep only the last N messages per chat
                max_messages = CONVERSATION_MAX_TURNS * 2
                cur.execute("""
                    DELETE FROM conversations WHERE id IN (
                        SELECT id FROM conversations WHERE chat_id = %s
                        ORDER BY created_at DESC OFFSET %s
                    )
                """, (chat_id, max_messages))
                conn.commit()
            conn.close()
            return
        except Exception as e:
            logger.error("DB write error: " + str(e))
            try:
                conn.close()
            except Exception:
                pass
    # Fallback to in-memory
    if chat_id not in _memory_history:
        _memory_history[chat_id] = []
    _memory_history[chat_id].append({"role": role, "content": content})
    max_messages = CONVERSATION_MAX_TURNS * 2
    if len(_memory_history[chat_id]) > max_messages:
        _memory_history[chat_id] = _memory_history[chat_id][-max_messages:]
    _memory_timestamps[chat_id] = time.time()

lark = LarkClient()
netsuite = NetSuiteClient()
pipedrive = PipedriveClient()


def _fetch_bot_open_id():
    """Fetch the bot's own open_id from Lark API so we can identify when it's mentioned."""
    global BOT_OPEN_ID
    try:
        url = lark.base_url + "/open-apis/bot/v3/info"
        resp = requests.get(url, headers=lark._headers(), timeout=10)
        data = resp.json()
        if data.get("code") == 0:
            bot_info = data.get("bot", {})
            BOT_OPEN_ID = bot_info.get("open_id", "")
            bot_name = bot_info.get("app_name", "")
            logger.info("Bot open_id: " + BOT_OPEN_ID + ", name: " + bot_name)
        else:
            logger.warning("Could not fetch bot info: " + str(data))
    except Exception as e:
        logger.warning("Error fetching bot open_id: " + str(e))


# -------------------------------------------------------------------------
# User Scoping
# -------------------------------------------------------------------------
def get_user_scope(sender_open_id):
    """
    Returns 'hannah', 'lucy', or 'brendan' based on sender's open_id.
    Brendan (and any unknown user) gets full access.
    """
    if not sender_open_id:
        return "brendan"
    if HANNAH_OPEN_ID and sender_open_id == HANNAH_OPEN_ID:
        return "hannah"
    if LUCY_OPEN_ID and sender_open_id == LUCY_OPEN_ID:
        return "lucy"
    return "brendan"


def filter_projects_by_scope(projects, scope):
    """
    If scope is 'hannah' or 'lucy', only return boards whose name contains that word.
    'brendan' gets everything.
    """
    if scope == "brendan":
        return projects
    filtered = [p for p in projects if scope in p.get("__table_name__", "").lower()]
    logger.info(f"Scope '{scope}': filtered {len(projects)} -> {len(filtered)} records")
    return filtered


# -------------------------------------------------------------------------
# Lark data helpers
# -------------------------------------------------------------------------
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


def filter_relevant_projects(question, projects):
    q = question.lower()
    broad = any(w in q for w in ["all", "every", "list", "show", "overview", "summary", "status"])
    keywords = [w for w in q.split() if len(w) > 3]
    if broad or not keywords:
        return projects[:200]
    relevant = []
    for p in projects:
        tname = p.get("__table_name__", "").lower()
        row_text = " ".join(str(v) for v in p.values()).lower()
        if any(kw in tname or kw in row_text for kw in keywords):
            relevant.append(p)
    return relevant[:200] if relevant else projects[:200]


def build_context(projects):
    today = datetime.now(timezone.utc)
    lines = ["Today is " + today.strftime("%A %B %d %Y") + ".", "Total records: " + str(len(projects)), ""]
    for p in projects:
        tname = p.get("__table_name__", "Unknown")
        parts = ["[Board: " + tname + "]"]
        for key, val in p.items():
            if key == "__table_name__":
                continue
            parts.append(key + ": " + field_to_text(val))
        lines.append(" | ".join(parts))
    return "\n".join(lines)


# -------------------------------------------------------------------------
# NetSuite
# -------------------------------------------------------------------------
def detect_pipedrive_type(question):
    q = question.lower()
    deal_kw = ["deal", "deals", "pipeline", "quote", "proposal", "client stage", "won", "lost", "opportunity"]
    contact_kw = ["contact", "person", "who is", "email for", "phone for"]
    activity_kw = ["activity", "activities", "meeting", "call", "task", "follow up", "upcoming"]
    revenue_kw = ["revenue", "won deals", "closed", "total sales", "how much have we made"]
    if any(k in q for k in revenue_kw):
        return "revenue"
    if any(k in q for k in deal_kw):
        return "deals"
    if any(k in q for k in contact_kw):
        return "contacts"
    if any(k in q for k in activity_kw):
        return "activities"
    return None

def fetch_pipedrive_data(question):
    pd_type = detect_pipedrive_type(question)
    if not pd_type:
        return None
    try:
        if pd_type == "deals":
            # Try to search by keyword first
            words = [w for w in question.split() if len(w) > 3]
            for word in words:
                result = pipedrive.search_deals(word)
                if result.get("count", 0) > 0:
                    return result
            return pipedrive.get_all_deals()
        elif pd_type == "contacts":
            words = [w for w in question.split() if len(w) > 3]
            for word in words:
                result = pipedrive.search_contacts(word)
                if result.get("count", 0) > 0:
                    return result
            return None
        elif pd_type == "activities":
            return pipedrive.get_upcoming_activities()
        elif pd_type == "revenue":
            return pipedrive.get_won_deals_summary()
    except Exception as e:
        logger.error("Pipedrive fetch error: " + str(e))
        return {"error": str(e)}
    return None

def detect_netsuite_type(question):
    q = question.lower()
    balance_keywords = ["balance", "owe", "owes", "owed", "invoice", "invoices", "outstanding", "ar ", "accounts receivable", "how much", "payment due", "overdue", "aged", "past due"]
    address_keywords = ["address", "where to ship", "ship to", "shipping address", "deliver to", "where does", "where should"]
    shipping_keywords = ["tracking", "shipment", "shipped", "tracking number", "package", "delivery status"]
    if any(k in q for k in balance_keywords):
        return "balance"
    if any(k in q for k in address_keywords):
        return "address"
    if any(k in q for k in shipping_keywords):
        return "shipping"
    return None


def fetch_netsuite_data(question):
    netsuite_type = detect_netsuite_type(question)
    if not netsuite_type:
        return None
    try:
        if netsuite_type == "balance":
            return netsuite.get_customer_balance()
        elif netsuite_type == "address":
            return netsuite.get_ship_address("")
        elif netsuite_type == "shipping":
            return netsuite.get_recent_shipments()
    except Exception as e:
        logger.error("NetSuite fetch error: " + str(e))
        return {"error": str(e)}
    return None


# -------------------------------------------------------------------------
# Lark project fetching
# -------------------------------------------------------------------------
# Cache for Lark project data
_projects_cache = []
_projects_cache_time = 0
PROJECTS_CACHE_TTL = 300  # 5 minutes â project data doesn't change that fast


def fetch_all_projects():
    """Fetch all records from all Lark Base tables, with caching."""
    global _projects_cache, _projects_cache_time
    now = time.time()
    if _projects_cache and (now - _projects_cache_time) < PROJECTS_CACHE_TTL:
        logger.info("Using cached Lark data: " + str(len(_projects_cache)) + " records")
        return _projects_cache
    try:
        tables = lark.get_all_tables()
        all_records = []
        for table in tables:
            table_id = table.get("table_id", "")
            table_name = table.get("name", table_id)
            if not table_id:
                continue
            try:
                raw_records = lark.get_table_records(table_id)
                for raw in raw_records:
                    # Flatten: pull fields to top level
                    flat = dict(raw.get("fields", {}))
                    flat["__table_name__"] = table_name
                    flat["__record_id__"] = raw.get("record_id", "")
                    all_records.append(flat)
                logger.info("Fetched " + str(len(raw_records)) + " records from " + table_name)
            except Exception as e:
                logger.warning("Failed to fetch table " + table_name + ": " + str(e)[:80])
        _projects_cache = all_records
        _projects_cache_time = now
        logger.info("Total records fetched: " + str(len(all_records)))
        return all_records
    except Exception as e:
        logger.error("Lark fetch error: " + str(e))
        return _projects_cache  # Return stale cache if available



# -------------------------------------------------------------------------
# Lark Wiki
# -------------------------------------------------------------------------
_wiki_cache = []
_wiki_cache_time = 0
WIKI_CACHE_TTL = 300  # 5 minutes

def fetch_lark_wiki():
    """Fetch all Lark Wiki pages and return their text content."""
    global _wiki_cache, _wiki_cache_time
    now = time.time()
    if _wiki_cache and (now - _wiki_cache_time) < WIKI_CACHE_TTL:
        return _wiki_cache
    try:
        headers = lark._headers()
        base_url = lark.base_url
        # Get all wiki spaces
        spaces_resp = requests.get(
            f"{base_url}/open-apis/wiki/v2/spaces",
            headers=headers, params={"page_size": 50}, timeout=20
        )
        spaces_data = spaces_resp.json()
        if spaces_data.get("code") != 0:
            logger.warning("Wiki spaces fetch failed: " + str(spaces_data))
            return []
        spaces = spaces_data.get("data", {}).get("items", [])
        all_pages = []
        for space in spaces:
            space_id = space.get("space_id", "")
            space_name = space.get("name", "")
            try:
                nodes_resp = requests.get(
                    f"{base_url}/open-apis/wiki/v2/spaces/{space_id}/nodes",
                    headers=headers, params={"page_size": 50}, timeout=20
                )
                nodes_data = nodes_resp.json()
                if nodes_data.get("code") != 0:
                    continue
                nodes = nodes_data.get("data", {}).get("items", [])
                for node in nodes:
                    node_token = node.get("node_token", "")
                    node_title = node.get("title", "")
                    try:
                        doc_resp = requests.get(
                            f"{base_url}/open-apis/docx/v1/documents/{node_token}/raw_content",
                            headers=headers, timeout=20
                        )
                        doc_data = doc_resp.json()
                        if doc_data.get("code") == 0:
                            raw_content = doc_data.get("data", {}).get("content", "")
                            all_pages.append({
                                "space": space_name,
                                "title": node_title,
                                "content": raw_content[:3000]  # cap per page
                            })
                    except Exception as e:
                        logger.warning(f"Wiki page fetch error ({node_title}): {e}")
            except Exception as e:
                logger.warning(f"Wiki space fetch error ({space_name}): {e}")
        _wiki_cache = all_pages
        _wiki_cache_time = now
        logger.info(f"Wiki: fetched {len(all_pages)} pages")
        return all_pages
    except Exception as e:
        logger.error("Wiki fetch error: " + str(e))
        return _wiki_cache

# -------------------------------------------------------------------------
# Gemini AI
# -------------------------------------------------------------------------

def ask_gemini(question, projects, netsuite_data=None, scope="brendan", pipedrive_data=None, wiki_pages=None, comments_data=None, calendar_data=None, tasks_data=None, doc_data=None, contact_data=None, approval_data=None, chat_data=None, chat_history=None, user_facts=None):
    if not ANTHROPIC_API_KEY:
        return "AI not available. Check ANTHROPIC_API_KEY."
    relevant = filter_relevant_projects(question, projects)
    context = build_context(relevant)
    netsuite_section = ""
    if netsuite_data:
        if "error" in netsuite_data:
            netsuite_section = "\n--- NETSUITE DATA ---\nError: " + netsuite_data["error"] + "\n--- END NETSUITE ---\n"
        else:
            netsuite_section = "\n--- NETSUITE DATA ---\n" + json.dumps(netsuite_data, indent=2)[:4000] + "\n--- END NETSUITE ---\n"
    pipedrive_section = ""
    if pipedrive_data:
        if "error" in pipedrive_data:
            pipedrive_section = "\n--- PIPEDRIVE CRM DATA ---\nError: " + pipedrive_data["error"] + "\n--- END PIPEDRIVE ---\n"
        else:
            pipedrive_section = "\n--- PIPEDRIVE CRM DATA ---\n" + json.dumps(pipedrive_data, indent=2)[:4000] + "\n--- END PIPEDRIVE ---\n"
    wiki_section = ""
    if wiki_pages:
        wiki_lines = ["\n--- LARK WIKI KNOWLEDGE BASE ---"]
        for page in wiki_pages[:10]:
            wiki_lines.append(f"[{page['space']} / {page['title']}]\n{page['content'][:800]}")
        wiki_lines.append("--- END WIKI ---\n")
        wiki_section = "\n".join(wiki_lines)
    comments_section = ""
    if comments_data:
        order_num = comments_data.get("order_num", "")
        clist = comments_data.get("comments", [])
        lines = [f"\n--- COMMENTS FOR {order_num} ---"]
        for c in clist:
            from datetime import datetime
            ts = c.get("create_time", 0)
            dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M") if ts else "unknown"
            lines.append(f"[{dt}] {c.get('user_name','?')}: {c.get('content','')}")
        lines.append("--- END COMMENTS ---\n")
        comments_section = "\n".join(lines)
    calendar_section = ""
    if calendar_data:
        events = calendar_data.get("events", [])
        if events:
            cal_lines = ["\n--- CALENDAR EVENTS ---"]
            for ev in events[:20]:
                summary = ev.get("summary", "No title")
                start = ev.get("start_time", {})
                end = ev.get("end_time", {})
                start_str = start.get("date", "") or start.get("timestamp", "")
                end_str = end.get("date", "") or end.get("timestamp", "")
                cal_lines.append(f"- {summary} | Start: {start_str} | End: {end_str}")
            cal_lines.append("--- END CALENDAR ---\n")
            calendar_section = "\n".join(cal_lines)
    tasks_section = ""
    if tasks_data:
        task_list = tasks_data.get("tasks", [])
        if task_list:
            task_lines = ["\n--- TASKS ---"]
            for tk in task_list[:20]:
                title = tk.get("summary", "No title")
                due = tk.get("due", {}).get("timestamp", "no due date")
                status = "completed" if tk.get("completed_at") else "open"
                task_lines.append(f"- [{status}] {title} | Due: {due}")
            task_lines.append("--- END TASKS ---\n")
            tasks_section = "\n".join(task_lines)
    doc_section = ""
    if doc_data:
        doc_section = "\n--- DOCUMENT DATA ---\n" + json.dumps(doc_data, indent=2)[:4000] + "\n--- END DOCUMENT ---\n"
    contact_section = ""
    if contact_data:
        contact_section = "\n--- CONTACT DATA ---\n" + json.dumps(contact_data, indent=2)[:4000] + "\n--- END CONTACT ---\n"
    approval_section = ""
    if approval_data:
        approval_section = "\n--- APPROVAL DATA ---\n" + json.dumps(approval_data, indent=2)[:4000] + "\n--- END APPROVAL ---\n"
    chat_section = ""
    if chat_data:
        chat_section = "\n--- CHAT DATA ---\n" + json.dumps(chat_data, indent=2)[:4000] + "\n--- END CHAT ---\n"
    facts_section = ""
    if user_facts:
        fact_lines = ["\n--- SAVED USER NOTES ---"]
        for f in user_facts:
            fact_lines.append(f"- {f['fact']}")
        fact_lines.append("--- END SAVED NOTES ---\n")
        facts_section = "\n".join(fact_lines)
    if scope == "hannah":
        scope_instruction = "IMPORTANT: You are speaking with Hannah. Only discuss Hannah's projects and boards. Do not mention Lucy's or Brendan's projects.\n"
    elif scope == "lucy":
        scope_instruction = "IMPORTANT: You are speaking with Lucy. Only discuss Lucy's projects and boards. Do not mention Hannah's or Brendan's projects.\n"
    else:
        scope_instruction = ""
    system_prompt = (
        scope_instruction +
        "You are IRON BOT, the intelligent assistant for HLT (High Life Tech / Highlife Tech). "
        "You are powered by Claude and should respond the way Claude would â thoughtful, helpful, "
        "conversational, and thorough. You have access to live company data including projects, "
        "production status, shipping, clients, CRM (Pipedrive), financials (NetSuite), calendar, "
        "tasks, documents, wiki, contacts, approvals, email, and chat management.\n\n"

        "HOW TO RESPOND:\n"
        "- Be natural and conversational. Talk like a knowledgeable team member, not a database.\n"
        "- Give complete, thoughtful answers. If someone asks about a project, provide context â "
        "what stage it's at, what's coming next, any concerns or deadlines approaching.\n"
        "- Be proactive: if you notice something relevant (like an overdue item, an upcoming deadline, "
        "or a potential issue), mention it even if they didn't ask.\n"
        "- When listing items, organize them clearly but don't just dump raw data â summarize and "
        "highlight what matters.\n"
        "- If you're unsure about something or the data is incomplete, say so honestly and suggest "
        "what they could do to find out.\n"
        "- Use a warm, professional tone. You're part of the team.\n"
        "- You can handle follow-up questions â conversation history is provided when available.\n"
        "- For general knowledge questions (not about company data), answer them like Claude would â "
        "you're not limited to just company data queries.\n\n"

        "COMPANY DATA KNOWLEDGE:\n"
        "- 'Due Date' in the data = 'In Hand Date' (the date the client needs delivery). "
        "Always refer to it as 'In Hand Date' when talking to the team.\n"
        "- Timestamps in the data are Unix milliseconds â always convert them to readable dates.\n"
        "- Board ownership: tables with 'Lucy' in the name are Lucy's, 'Hannah' are Hannah's, "
        "otherwise they're Brendan's.\n\n"

        "STATUS VALUES (use these to understand project stages):\n"
        "- WAITING ART = awaiting artwork, not yet paid\n"
        "- PAID/WAITING ART = paid, awaiting artwork\n"
        "- QUOTE NEEDED = needs price quote (not artwork)\n"
        "- QUOTE ADDED = quote provided, waiting on decision\n"
        "- ARTWORK CONFIRMED = artwork approved, ready for production\n"
        "- PART CONFIRMED = partially confirmed\n"
        "- PLATING / POLISHING = in production\n"
        "- PART SHIPPED = partially shipped\n"
        "- SHIPPED / RESOLVED/SHIPPED = completed\n"
        "- NEEDS RESOLUTION = has an active problem that needs attention\n\n"

        "FILTERING (when users ask about categories):\n"
        "- 'awaiting artwork' = WAITING ART or PAID/WAITING ART\n"
        "- 'needs quote' = QUOTE NEEDED only\n"
        "- 'in production' = PLATING, POLISHING, or PART CONFIRMED\n"
        "- 'shipped/done' = SHIPPED or RESOLVED/SHIPPED\n"
        "- 'issues/problems' = NEEDS RESOLUTION\n"
    )
    user_message = (
        "--- LARK PROJECT DATA ---\n" + context + "\n--- END LARK DATA ---\n" +
        netsuite_section +
        pipedrive_section +
        wiki_section +
        comments_section +
        calendar_section +
        tasks_section +
        doc_section +
        contact_section +
        approval_section +
        chat_section +
        facts_section +
        "\nQuestion: " + question
    )
    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system_prompt,
            messages=(chat_history or []) + [{"role": "user", "content": user_message}]
        )
        answer = response.content[0].text.strip()
        logger.info("Claude replied: " + str(len(answer)) + " chars")
        return answer
    except Exception as e:
        logger.error("Claude error: " + str(e))
        return "AI error: " + str(e)[:200]


# -------------------------------------------------------------------------
# Artwork Approval
# -------------------------------------------------------------------------
def detect_artwork_approval(text):
    t = text.lower()
    if ("artwork" in t or "art" in t) and ("approv" in t or "confirm" in t or "approved" in t):
        match = re.search(r'hlt[\s\-]?(\d+)', text, re.IGNORECASE)
        if match:
            return "HLT" + match.group(1)
    return None


def detect_comments_request(text):
    """Return the order number if the user is asking about comments on a record, else None."""
    t = text.lower()
    comment_kw = ["comment", "comments", "note", "notes", "feedback", "remark", "remarks"]
    if any(kw in t for kw in comment_kw):
        match = re.search(r'hlt[s-]?(\d+)', text, re.IGNORECASE)
        if match:
            return "HLT" + match.group(1)
    return None




def handle_artwork_approval(order_num, user_text, chat_id):
    logger.info(f"Artwork approval for order: {order_num}")
    artwork_chats = []
    if LARK_CHAT_ID_HANNAH_ARTWORK:
        artwork_chats.append(("Hannah", LARK_CHAT_ID_HANNAH_ARTWORK))
    if LARK_CHAT_ID_LUCY_ARTWORK:
        artwork_chats.append(("Lucy", LARK_CHAT_ID_LUCY_ARTWORK))
    record = lark.find_record_by_order_num(order_num)
    if not record:
        return f"Could not find order {order_num} in any board."
    board_name = record.get("__table_name__", "").lower()
    owner = "Hannah" if "hannah" in board_name else ("Lucy" if "lucy" in board_name else "Brendan")
    target_chat = None
    for name, cid in artwork_chats:
        if name.lower() == owner.lower():
            target_chat = cid
            break
    if not target_chat and artwork_chats:
        target_chat = artwork_chats[0][1]
    try:
        lark.update_record_status(record, ARTWORK_CONFIRMED_STATUS, FIELD_PRODUCTION_DRAWING)
        msg = f"â Artwork confirmed for {order_num}. Status updated to '{ARTWORK_CONFIRMED_STATUS}'."
        if target_chat and target_chat != chat_id:
            lark.send_response(msg, chat_id=target_chat)
        return msg
    except Exception as e:
        logger.error(f"Artwork approval error: {e}")
        return f"Error updating artwork for {order_num}: {str(e)}"


# -------------------------------------------------------------------------
# Deduplication helpers
# -------------------------------------------------------------------------
def _is_already_processed(message_id):
    now = time.time()
    # Clean up old entries
    expired = [mid for mid, ts in processed_message_ids.items() if now - ts > DEDUP_TTL]
    for mid in expired:
        del processed_message_ids[mid]
    if message_id in processed_message_ids:
        return True
    processed_message_ids[message_id] = now
    return False


# -------------------------------------------------------------------------
# Smart Question Classification (skip heavy data fetches for simple questions)
# -------------------------------------------------------------------------
CASUAL_PATTERNS = [
    "hello", "hi ", "hey", "good morning", "good afternoon", "good evening",
    "what can you do", "help me", "who are you", "what are you",
    "thanks", "thank you", "bye", "goodbye", "see you",
    "how are you", "what's up", "sup", "yo ", "haha", "lol",
    "tell me a joke", "what time", "what day", "what is the date"
]

KNOWLEDGE_PATTERNS = [
    "what does", "what is", "define", "explain", "how does",
    "tell me about", "meaning of", "difference between",
    "why is", "why do", "why are", "how to", "can you explain"
]

def _classify_question(text):
    """Classify question to determine what data to fetch.
    Returns: 'casual', 'knowledge', 'project', or 'full'
    """
    t = text.lower().strip()
    # Casual greetings/chitchat - no data needed
    for pat in CASUAL_PATTERNS:
        if t.startswith(pat) or t == pat.strip():
            return "casual"
    # General knowledge questions (not about company data)
    for pat in KNOWLEDGE_PATTERNS:
        if t.startswith(pat) and not any(kw in t for kw in ["project", "order", "board", "status", "production", "shipped", "artwork", "quote", "client", "so#", "hlt"]):
            return "knowledge"
    # Check if it's a "remember" command
    if t.startswith("remember that") or t.startswith("remember:") or t.startswith("save this"):
        return "remember"
    if t.startswith("what do you remember") or t.startswith("what have i told") or t.startswith("my notes") or t.startswith("show my facts"):
        return "recall"
    return "full"

def _send_thinking(chat_id):
    """Send a quick thinking indicator so the user knows the bot is working."""
    try:
        lark.send_response("\u2699\ufe0f Processing your request...", chat_id=chat_id)
    except Exception:
        pass

# -------------------------------------------------------------------------
# User Facts / "Remember That" System (Postgres-backed)
# -------------------------------------------------------------------------
def _init_facts_table():
    """Create the user facts table if it doesn't exist."""
    conn = _get_db_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_facts (
                    id SERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL DEFAULT '',
                    fact TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_facts_chat
                ON user_facts (chat_id, created_at DESC)
            """)
            conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Facts table init error: " + str(e))
        try:
            conn.close()
        except Exception:
            pass

def _save_fact(chat_id, sender_id, fact):
    """Save a user fact to Postgres."""
    conn = _get_db_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO user_facts (chat_id, sender_id, fact) VALUES (%s, %s, %s)",
                (chat_id, sender_id, fact[:2000])
            )
            conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error("Save fact error: " + str(e))
        try:
            conn.close()
        except Exception:
            pass
        return False

def _get_facts(chat_id, limit=20):
    """Retrieve saved facts for a chat."""
    conn = _get_db_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT fact, created_at FROM user_facts WHERE chat_id = %s ORDER BY created_at DESC LIMIT %s",
                (chat_id, limit)
            )
            rows = cur.fetchall()
            conn.commit()
        conn.close()
        return [{"fact": r["fact"], "saved_at": str(r["created_at"])} for r in rows]
    except Exception as e:
        logger.error("Get facts error: " + str(e))
        try:
            conn.close()
        except Exception:
            pass
        return []

# -------------------------------------------------------------------------
# Message processing (runs in background thread)
# -------------------------------------------------------------------------
def _process_message(user_text, chat_id, artwork_order, scope="brendan", sender_id=""):
    if artwork_order:
        answer = handle_artwork_approval(artwork_order, user_text, chat_id)
        try:
            lark.send_response(answer, chat_id=chat_id)
        except Exception as e:
            logger.error("Send failed: " + str(e))
        return

    # --- Smart classification: skip heavy fetches for simple questions ---
    q_class = _classify_question(user_text)
    logger.info(f"Question classified as: {q_class}")

    # Handle "remember that" commands instantly
    if q_class == "remember":
        fact = user_text.lower().replace("remember that", "").replace("remember:", "").replace("save this:", "").strip()
        if fact:
            saved = _save_fact(chat_id, sender_id, fact)
            answer = f"Got it, I'll remember that! \U0001f4dd" if saved else "I tried to save that but hit a database issue. I'll remember it for this session though."
        else:
            answer = "What would you like me to remember? Just say something like: remember that Carlo handles plating."
        lark.send_response(answer, chat_id=chat_id)
        return

    # Handle "recall" commands instantly
    if q_class == "recall":
        facts = _get_facts(chat_id)
        if facts:
            lines = ["Here's what I remember: \U0001f4cb\n"]
            for f in facts:
                lines.append(f"\u2022 {f['fact']}")
            answer = "\n".join(lines)
        else:
            answer = "I don't have any saved notes yet. You can tell me things like: remember that Lucy handles jewelry orders."
        lark.send_response(answer, chat_id=chat_id)
        return

    # Handle casual greetings â fast response, no data fetch
    if q_class == "casual":
        chat_hist = _get_conversation(chat_id)
        _add_to_conversation(chat_id, "user", user_text)
        facts = _get_facts(chat_id, limit=5)
        facts_context = ""
        if facts:
            facts_context = "\nSaved notes: " + "; ".join([f["fact"] for f in facts])
        try:
            response = anthropic_client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=1024,
                system="You are IRON BOT, HLT's friendly internal assistant. Respond warmly and briefly. You're powered by Claude." + facts_context,
                messages=(chat_hist or []) + [{"role": "user", "content": user_text}]
            )
            answer = response.content[0].text.strip()
        except Exception as e:
            answer = "Hey! How can I help you today? \U0001f44b"
        _add_to_conversation(chat_id, "assistant", answer)
        lark.send_response(answer, chat_id=chat_id)
        return

    # Handle general knowledge â use Haiku, no data fetch
    if q_class == "knowledge":
        chat_hist = _get_conversation(chat_id)
        _add_to_conversation(chat_id, "user", user_text)
        facts = _get_facts(chat_id, limit=5)
        facts_context = ""
        if facts:
            facts_context = "\nSaved notes: " + "; ".join([f["fact"] for f in facts])
        try:
            response = anthropic_client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=2048,
                system="You are IRON BOT, HLT's internal assistant powered by Claude. Answer knowledge questions thoughtfully and conversationally. If the question seems like it might relate to company data, suggest the user ask more specifically." + facts_context,
                messages=(chat_hist or []) + [{"role": "user", "content": user_text}]
            )
            answer = response.content[0].text.strip()
        except Exception as e:
            answer = "I had trouble processing that. Could you try rephrasing?"
        _add_to_conversation(chat_id, "assistant", answer)
        lark.send_response(answer, chat_id=chat_id)
        return

    # --- Command routing: detect specialized commands and route to handlers ---
    cmd_type = detect_command_type(user_text)
    if cmd_type == "calendar":
        handle_calendar_query(user_text, chat_id, scope)
        return
    elif cmd_type == "task":
        handle_task_command(user_text, chat_id, scope)
        return
    elif cmd_type == "approval":
        handle_approval_query(user_text, chat_id, scope)
        return
    elif cmd_type == "doc":
        handle_doc_query(user_text, chat_id, scope)
        return
    elif cmd_type == "contact":
        handle_contact_lookup(user_text, chat_id, scope)
        return
    elif cmd_type == "chat":
        handle_chat_management(user_text, chat_id, scope)
        return
    # --- End command routing ---

    # Full question â send thinking indicator, then fetch all data
    threading.Thread(target=_send_thinking, args=(chat_id,), daemon=True).start()

    netsuite_result = {}
    projects_result = {}
    pipedrive_result = {}
    wiki_result = {}
    comments_result = {}
    def get_lark():
        projects_result["data"] = fetch_all_projects()
    def get_netsuite():
        data = fetch_netsuite_data(user_text)
        if data:
            netsuite_result["data"] = data
    def get_pipedrive():
        data = fetch_pipedrive_data(user_text)
        if data:
            pipedrive_result["data"] = data
    def get_wiki():
        pages = fetch_lark_wiki()
        if pages:
            wiki_result["data"] = pages
    def get_comments():
        order_num = detect_comments_request(user_text)
        if order_num:
            try:
                data = lark.get_comments_for_order(order_num)
                if data:
                    comments_result["data"] = {"order_num": order_num, "comments": data}
            except Exception as e:
                logger.error("Comments fetch error: " + str(e))
    t1 = threading.Thread(target=get_lark)
    t2 = threading.Thread(target=get_netsuite)
    t3 = threading.Thread(target=get_pipedrive)
    t4 = threading.Thread(target=get_wiki)
    t5 = threading.Thread(target=get_comments)
    for t in [t1, t2, t3, t4, t5]:
        t.start()
    for t in [t1, t2, t3, t4, t5]:
        t.join(timeout=15)
    projects = projects_result.get("data", [])
    netsuite_data = netsuite_result.get("data")
    pipedrive_data = pipedrive_result.get("data")
    wiki_pages = wiki_result.get("data", [])
    comments_data = comments_result.get("data")
    # Apply user scope filter BEFORE passing to Claude
    scoped_projects = filter_projects_by_scope(projects, scope)
    # Get conversation history and saved facts for this chat
    chat_hist = _get_conversation(chat_id)
    _add_to_conversation(chat_id, "user", user_text)
    facts = _get_facts(chat_id, limit=10)
    if not scoped_projects and not netsuite_data:
        answer = "I couldn't load project data right now. This might be a temporary issue with the Lark Base connection. Could you try again in a moment?"
    else:
        answer = ask_gemini(user_text, scoped_projects, netsuite_data, scope=scope, pipedrive_data=pipedrive_data, wiki_pages=wiki_pages, comments_data=comments_data, chat_history=chat_hist, user_facts=facts)
    _add_to_conversation(chat_id, "assistant", answer)
    try:
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error("Send failed: " + str(e))


# -------------------------------------------------------------------------
# Extract question (only if bot is @mentioned)
# -------------------------------------------------------------------------
def extract_question(msg):
    """
    Returns question text ONLY if the bot was directly @mentioned.
    Uses the bot's open_id (fetched at startup) to identify bot mentions.
    Falls back to BOT_NAME string match.
    """
    try:
        content = json.loads(msg.get("content", "{}"))
        raw_text = content.get("text", "").strip()
    except Exception:
        return None
    if not raw_text:
        return None

    # Direct/P2P chat: always respond
    if msg.get("chat_type", "") == "p2p":
        return raw_text

    # Group chat: ONLY respond if bot itself is in mentions
    mentions = msg.get("mentions", [])
    logger.info("Checking " + str(len(mentions)) + " mentions, bot_open_id=" + str(BOT_OPEN_ID))

    bot_mentioned = False
    for mention in mentions:
        mid = mention.get("id", {})
        mention_open_id = mid.get("open_id", "")
        mention_name = mention.get("name", "")
        logger.info("Mention: open_id=" + mention_open_id + " name=" + mention_name)

        # Primary check: match by open_id
        if BOT_OPEN_ID and mention_open_id == BOT_OPEN_ID:
            bot_mentioned = True
            break
        # Fallback: match by name
        if BOT_NAME and BOT_NAME.lower() in mention_name.lower():
            bot_mentioned = True
            break

    if not bot_mentioned:
        logger.info("Bot NOT mentioned - ignoring message")
        return None

    # Strip @bot mention tag from text
    clean = re.sub(r'@[^\s]+', '', raw_text).strip()
    return clean if clean else raw_text


# -------------------------------------------------------------------------
# Flask Routes
# -------------------------------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}
    _last_webhooks.append(body)
    if len(_last_webhooks) > 5:
        _last_webhooks.pop(0)
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})
    event = body.get("event", {})
    msg = event.get("message", {})
    if msg.get("message_type") != "text":
        return jsonify({"code": 0})
    message_id = msg.get("message_id", "")
    if _is_already_processed(message_id):
        logger.info("Duplicate message ignored: " + message_id)
        return jsonify({"code": 0})
    user_text = extract_question(msg)
    if not user_text:
        return jsonify({"code": 0})
    chat_id = msg.get("chat_id", "")
    if not chat_id:
        return jsonify({"code": 0})

    # Determine sender scope
    sender = event.get("sender", {})
    sender_open_id = sender.get("sender_id", {}).get("open_id", "")
    scope = get_user_scope(sender_open_id)
    logger.info("Question: " + repr(user_text) + " chat=" + chat_id + " scope=" + scope + " sender=" + sender_open_id)

    artwork_order = detect_artwork_approval(user_text)
    threading.Thread(
        target=_process_message,
        args=(user_text, chat_id, artwork_order, scope, sender_open_id),
        daemon=True
    ).start()
    return jsonify({"code": 0})


@app.route("/last-webhook", methods=["GET"])
def last_webhook():
    safe = []
    for body in _last_webhooks[-3:]:
        event = body.get("event", {})
        msg = event.get("message", {})
        sender = event.get("sender", {})
        try:
            content = json.loads(msg.get("content", "{}"))
        except Exception:
            content = {}
        safe.append({
            "chat_type": msg.get("chat_type"),
            "chat_id": msg.get("chat_id"),
            "message_id": msg.get("message_id"),
            "text": content.get("text", ""),
            "mentions": msg.get("mentions", []),
            "sender_open_id": sender.get("sender_id", {}).get("open_id", ""),
            "message_type": msg.get("message_type"),
        })
    return jsonify({"last_webhooks": safe, "bot_open_id": BOT_OPEN_ID})


@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({
        "claude_ready": bool(ANTHROPIC_API_KEY),
        "claude_model": "claude-sonnet-4-6",
        "lark_app_id_prefix": LARK_APP_ID[:10] + "..." if LARK_APP_ID else "NOT SET",
        "env_app_id": bool(os.environ.get("LARK_APP_ID")),
        "env_app_secret": bool(os.environ.get("LARK_APP_SECRET")),
        "env_base_token": bool(os.environ.get("LARK_BASE_APP_TOKEN")),
        "auth": "OK - token length " + str(len(lark._headers().get("Authorization", ""))) if lark._headers() else "FAIL",
        "table_count": "N/A",
        "cache_records": len(lark._cache) if hasattr(lark, '_cache') else 0,
        "cache_age_seconds": int(time.time() - lark._cache_time) if hasattr(lark, '_cache_time') and lark._cache_time else None,
        "bot_open_id": BOT_OPEN_ID,
        "bot_name": BOT_NAME,
        "hannah_open_id": HANNAH_OPEN_ID,
        "lucy_open_id": LUCY_OPEN_ID or "NOT SET",
        "netsuite_configured": netsuite.is_configured() if hasattr(netsuite, 'is_configured') else bool(os.environ.get("NETSUITE_ACCOUNT_ID")),
    })


@app.route("/list-models", methods=["GET"])
def list_models():
    return jsonify({"model": "claude-sonnet-4-6", "provider": "Google"})


@app.route("/list-chats", methods=["GET"])
def list_chats():
    try:
        import requests as req
        url = lark.base_url + "/open-apis/im/v1/chats"
        params = {"page_size": 100}
        resp = req.get(url, headers=lark._headers(), params=params, timeout=30)
        data = resp.json()
        if data.get("code") != 0:
            return jsonify({"error": data})
        chats = data.get("data", {}).get("items", [])
        result = [{"chat_id": c.get("chat_id"), "name": c.get("name", "")} for c in chats]
        return jsonify({"chats": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/test-netsuite", methods=["GET"])
def test_netsuite():
    """Test all NetSuite connections and show exact errors."""
    if not netsuite.configured:
        return jsonify({"error": "NetSuite not configured - check env vars"})
    results = {}
    try:
        bal = netsuite.get_customer_balance()
        results["balance"] = {"ok": "error" not in bal, "preview": str(bal)[:300]}
    except Exception as e:
        results["balance"] = {"ok": False, "error": str(e)[:300]}
    try:
        addr = netsuite.get_ship_address("test")
        results["address"] = {"ok": "error" not in addr, "preview": str(addr)[:300]}
    except Exception as e:
        results["address"] = {"ok": False, "error": str(e)[:300]}
    try:
        ship = netsuite.get_recent_shipments()
        results["shipments"] = {"ok": "error" not in ship, "preview": str(ship)[:300]}
    except Exception as e:
        results["shipments"] = {"ok": False, "error": str(e)[:300]}
    return jsonify(results)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID, "model": "claude-sonnet-4-6"})


@app.route("/sample-data", methods=["GET"])
def sample_data():
    """Return a sample of records showing field names and status values."""
    projects = fetch_all_projects()
    statuses = {}
    field_names = set()
    for p in projects:
        status = str(p.get("Status", p.get("status", "")))
        if status and status != 'None':
            statuses[status] = statuses.get(status, 0) + 1
        for k in p.keys():
            if not k.startswith("__"):
                field_names.add(k)
    samples = []
    for p in projects[:5]:
        samples.append({k: str(v)[:100] for k, v in p.items()})
    return jsonify({
        "total_records": len(projects),
        "status_counts": statuses,
        "field_names": sorted(list(field_names)),
        "sample_records": samples,
    })


# -------------------------------------------------------------------------
# Morning Digest
# -------------------------------------------------------------------------
# Exact board names (case-insensitive) to exclude from the morning digest
DIGEST_EXCLUDED_BOARDS = {
    "hannah quotes",
    "lucy quotes",
    "brendan quotes",
    "master production tab",
}

def _is_digest_excluded_board(table_name):
    """Return True if this board should be excluded from the morning digest.
    Matches on exact board name (case-insensitive) only."""
    tname = table_name.strip().lower()
    # Exact match first
    if tname in DIGEST_EXCLUDED_BOARDS:
        return True
    # Also catch truncated names like "master production tab..." stored with ellipsis
    for excl in DIGEST_EXCLUDED_BOARDS:
        if tname.startswith(excl):
            return True
    return False


def build_morning_digest(projects):
    """Ask Claude to write a morning briefing from all project data."""
    if not ANTHROPIC_API_KEY:
        return "Morning digest unavailable: ANTHROPIC_API_KEY not set."
    today = datetime.now(timezone.utc)
    today_ms = today.timestamp() * 1000

    # Filter out excluded boards (QUOTES, PART SHIPPED, etc.)
    projects = [
        p for p in projects
        if not _is_digest_excluded_board(p.get("__table_name__", ""))
    ]

    # Separate projects by urgency
    overdue = []
    due_soon = []
    in_production = []
    awaiting_art = []

    for p in projects:
        status = str(p.get("Status", p.get("status", ""))).upper()
        if any(s in status for s in ("SHIPPED", "RESOLVED/SHIPPED", "DONE", "PART SHIPPED", "QUOTE NEEDED", "QUOTE ADDED")):
            continue
        # "In-Hand Date" is the canonical field name on cards
        due_raw = p.get("In-Hand Date") or p.get("In Hand Date") or p.get("Due Date")
        due_ms = None
        if isinstance(due_raw, (int, float)):
            due_ms = float(due_raw)
        days_until = None
        if due_ms is not None:
            days_until = (due_ms - today_ms) / (1000 * 60 * 60 * 24)

        # Only overdue if it HAS an In-Hand Date AND that date is before today
        if due_ms is not None and days_until is not None and days_until < 0:
            overdue.append({**p, "_days_overdue": abs(int(days_until))})
        elif due_ms and days_until is not None and days_until <= 7:
            due_soon.append({**p, "_days_until": int(days_until)})
        elif "PLATING" in status or "POLISHING" in status or "PART CONFIRMED" in status:
            in_production.append(p)
        elif "WAITING ART" in status or "PAID/WAITING" in status:
            awaiting_art.append(p)

    def fmt(p):
        order_num = p.get("Order #", "") or p.get("Sales Order", "") or p.get("SO#", "") or "No SO#"
        status = p.get("Status", "")
        return f"- #{order_num} | {status}"

    sections = []
    sections.append(f"Today is {today.strftime('%A, %B %d %Y')}.")
    sections.append(f"Total active projects (excl. quotes & partial-shipped): {len(projects)}")
    sections.append(f"NEEDS ARTWORK: {len(awaiting_art)} projects")
    sections.append(f"OVERDUE: {len(overdue)} projects")
    sections.append(f"DUE WITHIN 7 DAYS: {len(due_soon)} projects")
    sections.append(f"IN PRODUCTION: {len(in_production)} projects")
    if overdue:
        sections.append(f"\nOVERDUE ({len(overdue)}):")
        for p in sorted(overdue, key=lambda x: x.get('_days_overdue', 0), reverse=True)[:10]:
            sections.append(fmt(p) + f" | {p['_days_overdue']} days overdue")
    if due_soon:
        sections.append(f"\nDUE WITHIN 7 DAYS ({len(due_soon)}):")
        for p in sorted(due_soon, key=lambda x: x.get('_days_until', 99))[:10]:
            sections.append(fmt(p) + f" | due in {p['_days_until']} days")
    if in_production:
        sections.append(f"\nIN PRODUCTION ({len(in_production)}):")
        for p in in_production[:10]:
            sections.append(fmt(p))
    if awaiting_art:
        sections.append(f"\nAWAITING ARTWORK ({len(awaiting_art)}):")
        for p in awaiting_art[:10]:
            sections.append(fmt(p))

    data_summary = "\n".join(sections)

    system_prompt = (
        "You are IRON BOT â the HLT (Highlife Tech) internal assistant. "
        "Write a morning briefing for the team. Be concise, direct, and actionable. "
        "Use emojis sparingly for visual scanning (ð´ overdue, ð¡ due soon, ðµ in production, âª awaiting art). "
        "Your digest MUST include these 4 sections in order: "
        "## NEEDS ARTWORK â X projects | "
        "## OVERDUE â X projects | "
        "## DUE WITHIN 7 DAYS â X projects | "
        "## TODAY'S PRIORITY LIST (numbered, most urgent first). "
        "IMPORTANT RULES:\n"
        "- DO NOT include any master tables or quotes boards in the digest.\n"
        "- For OVERDUE, only include projects whose In Hand Date is strictly before today.\n"
        "- For each status section (NEEDS ARTWORK, OVERDUE, DUE WITHIN 7 DAYS, IN PRODUCTION), "
        "list the actual sales order numbers (SO#) for every project in that group, not just counts.\n"
        "- Do NOT show raw data tables or quotes board data anywhere in the digest.\n"
        "- For IN PRODUCTION and other status lists, display each order on its OWN LINE as a bullet point (one order per row). NEVER join multiple orders on one line with pipes or commas.\n"
        "Use the exact counts from the data. Lead with overdue, then due-soon. "
        "End with a one-line morale note if the day looks heavy."
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            system=system_prompt,
            messages=[{"role": "user", "content": f"Write the morning briefing from this data:\n\n{data_summary}"}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Morning digest Claude error: {e}")
        return f"Morning digest error: {str(e)[:200]}"


@app.route("/morning-digest", methods=["POST", "GET"])
def morning_digest():
    """Called by GitHub Actions every morning to post the daily briefing."""
    # Auth check - require a secret token to prevent abuse
    digest_secret = os.environ.get("DIGEST_SECRET", "")
    if digest_secret:
        provided = request.headers.get("X-Digest-Secret", "") or request.args.get("secret", "")
        if provided != digest_secret:
            return jsonify({"error": "Unauthorized"}), 401

    chat_id = os.environ.get("LARK_CHAT_ID_DIGEST", "")
    if not chat_id:
        return jsonify({"error": "LARK_CHAT_ID_DIGEST not configured â morning digest can only post to the designated digest channel"}), 500

    try:
        projects = fetch_all_projects()
        if not projects:
            return jsonify({"error": "No project data available"}), 500

        digest = build_morning_digest(projects)
        lark.send_group_message(digest, chat_id=chat_id)
        logger.info(f"Morning digest posted to {chat_id}")
        return jsonify({"status": "ok", "length": len(digest), "chat_id": chat_id})
    except Exception as e:
        logger.error(f"Morning digest error: {e}")
        return jsonify({"error": str(e)}), 500


# -------------------------------------------------------------------------
# Interactive Card Action Callback (buttons in cards)
# -------------------------------------------------------------------------
@app.route("/card-callback", methods=["POST"])
def card_callback():
    """Handle interactive card button clicks from Lark."""
    body = request.get_json(silent=True) or {}

    # URL verification for card callback
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})

    # Extract action info
    action = body.get("action", {})
    action_value = action.get("value", {}).get("action", "")
    operator = body.get("operator", {})
    operator_id = operator.get("open_id", "")
    open_message_id = body.get("open_message_id", "")
    open_chat_id = body.get("open_chat_id", "")

    logger.info(f"Card callback: action={action_value} operator={operator_id}")

    if not action_value:
        return jsonify({"code": 0})

    # Route card actions
    try:
        if action_value.startswith("approve_artwork_"):
            order_num = action_value.replace("approve_artwork_", "")
            result = handle_artwork_approval(order_num, f"Artwork approved for {order_num}", open_chat_id)
            lark.reply_text(open_message_id, result)

        elif action_value.startswith("complete_task_"):
            task_id = action_value.replace("complete_task_", "")
            lark.complete_task(task_id)
            lark.reply_text(open_message_id, f"Task {task_id} marked as complete.")

        elif action_value.startswith("approve_"):
            instance_id = action_value.replace("approve_", "")
            lark.reply_text(open_message_id, f"Approval {instance_id} approved.")

        elif action_value.startswith("reject_"):
            instance_id = action_value.replace("reject_", "")
            lark.reply_text(open_message_id, f"Approval {instance_id} rejected.")

        elif action_value == "refresh_digest":
            threading.Thread(target=_send_refreshed_digest, args=(open_chat_id,), daemon=True).start()

        elif action_value.startswith("pin_"):
            msg_id = action_value.replace("pin_", "")
            lark.pin_message(msg_id)
            lark.reply_text(open_message_id, "Message pinned.")

        else:
            logger.info(f"Unknown card action: {action_value}")

    except Exception as e:
        logger.error(f"Card callback error: {e}")

    return jsonify({"code": 0})


def _send_refreshed_digest(chat_id):
    try:
        projects = fetch_all_projects()
        if projects:
            digest = build_morning_digest(projects)
            lark.send_group_message(digest, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Refresh digest error: {e}")


# -------------------------------------------------------------------------
# New Command Processors (detected by Claude in ask_gemini)
# -------------------------------------------------------------------------

def handle_create_record(question, chat_id, scope):
    """Handle requests to create new records in Lark Base."""
    try:
        answer = ask_gemini(
            question, fetch_all_projects(),
            scope=scope,
            wiki_pages=fetch_lark_wiki()
        )
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Create record error: {e}")
        lark.send_response(f"Error creating record: {str(e)[:200]}", chat_id=chat_id)


def handle_calendar_query(question, chat_id, scope):
    """Handle calendar-related questions."""
    try:
        calendar_data = None
        calendar_id = os.environ.get("LARK_PRIMARY_CALENDAR_ID", "")
        if calendar_id:
            events = lark.list_events(calendar_id)
            calendar_data = {"events": events[:20]}

        projects = fetch_all_projects()
        answer = ask_gemini(question, projects, scope=scope,
                           wiki_pages=fetch_lark_wiki(),
                           calendar_data=calendar_data)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Calendar query error: {e}")
        lark.send_response(f"Calendar error: {str(e)[:200]}", chat_id=chat_id)


def handle_task_command(question, chat_id, scope):
    """Handle task creation and management via the bot."""
    try:
        projects = fetch_all_projects()
        tasks_data = None
        try:
            tasks = lark.list_tasks(page_size=20)
            tasks_data = {"tasks": tasks}
        except Exception:
            pass

        answer = ask_gemini(question, projects, scope=scope,
                           wiki_pages=fetch_lark_wiki(),
                           tasks_data=tasks_data)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Task command error: {e}")
        lark.send_response(f"Task error: {str(e)[:200]}", chat_id=chat_id)


def handle_doc_query(question, chat_id, scope):
    """Handle document-related questions - search wiki and docs."""
    try:
        wiki_pages = fetch_lark_wiki()
        doc_results = None
        try:
            words = [w for w in question.split() if len(w) > 3]
            if words:
                doc_results = lark.search_docs(" ".join(words[:3]))
        except Exception:
            pass

        projects = fetch_all_projects()
        answer = ask_gemini(question, projects, scope=scope,
                           wiki_pages=wiki_pages,
                           doc_data=doc_results)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Doc query error: {e}")
        lark.send_response(f"Doc error: {str(e)[:200]}", chat_id=chat_id)


def handle_contact_lookup(question, chat_id, scope):
    """Handle contact/people lookup questions."""
    try:
        words = [w for w in question.split() if len(w) > 2]
        contact_data = None
        for word in words:
            try:
                results = lark.search_users(word)
                if results:
                    contact_data = {"users": results[:5]}
                    break
            except Exception:
                continue

        projects = fetch_all_projects()
        answer = ask_gemini(question, projects, scope=scope,
                           contact_data=contact_data)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Contact lookup error: {e}")
        lark.send_response(f"Contact error: {str(e)[:200]}", chat_id=chat_id)


def handle_approval_query(question, chat_id, scope):
    """Handle approval-related queries."""
    try:
        approval_data = None
        try:
            definitions = lark.list_approval_definitions()
            approval_data = {"definitions": definitions[:10]}
        except Exception:
            pass

        projects = fetch_all_projects()
        answer = ask_gemini(question, projects, scope=scope,
                           approval_data=approval_data)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Approval query error: {e}")
        lark.send_response(f"Approval error: {str(e)[:200]}", chat_id=chat_id)


def handle_chat_management(question, chat_id, scope):
    """Handle group chat management requests."""
    try:
        chat_data = None
        try:
            chats = lark.list_chats(limit=50)
            chat_data = {"chats": [{"chat_id": c.get("chat_id"), "name": c.get("name", "")} for c in chats]}
        except Exception:
            pass

        projects = fetch_all_projects()
        answer = ask_gemini(question, projects, scope=scope,
                           chat_data=chat_data)
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error(f"Chat management error: {e}")
        lark.send_response(f"Chat error: {str(e)[:200]}", chat_id=chat_id)


# -------------------------------------------------------------------------
# Enhanced command detection for routing to new handlers
# -------------------------------------------------------------------------
CALENDAR_KEYWORDS = ["calendar", "event", "meeting", "schedule", "appointment", "time off", "who is out", "busy"]
TASK_KEYWORDS = ["task", "todo", "to-do", "assign", "reminder", "follow up", "follow-up", "action item"]
DOC_KEYWORDS = ["document", "wiki", "doc ", "docs", "sop", "procedure", "manual", "knowledge base", "write a doc"]
CONTACT_KEYWORDS = ["who is", "contact", "email for", "phone for", "department", "team member", "org chart"]
APPROVAL_KEYWORDS = ["approval", "approve", "reject", "pending approval", "sign off"]
CHAT_KEYWORDS = ["create chat", "create group", "add to chat", "remove from chat", "chat members"]
SHEET_KEYWORDS = ["spreadsheet", "sheet", "excel", "csv", "export data"]

def detect_command_type(text):
    """Detect which handler should process this message."""
    t = text.lower()
    for kw in CHAT_KEYWORDS:
        if kw in t:
            return "chat"
    for kw in CALENDAR_KEYWORDS:
        if kw in t:
            return "calendar"
    for kw in TASK_KEYWORDS:
        if kw in t:
            return "task"
    for kw in APPROVAL_KEYWORDS:
        if kw in t:
            return "approval"
    for kw in CONTACT_KEYWORDS:
        if kw in t:
            return "contact"
    for kw in DOC_KEYWORDS:
        if kw in t:
            return "doc"
    for kw in SHEET_KEYWORDS:
        if kw in t:
            return "sheet"
    return "general"


# -------------------------------------------------------------------------
# Event Subscription Endpoint (for Lark real-time events)
# -------------------------------------------------------------------------
@app.route("/event", methods=["POST"])
def event_subscription():
    """Handle Lark event subscriptions (Base record changes, chat events, etc.)."""
    body = request.get_json(silent=True) or {}

    # URL verification
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})

    # Event processing
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    event = body.get("event", {})

    logger.info(f"Event received: {event_type}")

    try:
        if event_type == "drive.file.bitable_record_changed_v1":
            # Base record changed - could trigger notifications
            logger.info(f"Bitable record changed: {event}")

        elif event_type == "im.chat.member.bot.added_v1":
            # Bot added to a new chat
            chat_id = event.get("chat_id", "")
            logger.info(f"Bot added to chat: {chat_id}")
            if chat_id:
                lark.send_text("Hello! I'm IRON BOT. @ mention me with any question about projects, orders, shipments, or anything else. Type 'help' for a list of commands.", chat_id=chat_id)

        elif event_type == "im.chat.member.bot.deleted_v1":
            chat_id = event.get("chat_id", "")
            logger.info(f"Bot removed from chat: {chat_id}")

        elif event_type == "approval.approval.updated_v4":
            logger.info(f"Approval updated: {event}")

        elif event_type == "calendar.calendar.event.changed_v4":
            logger.info(f"Calendar event changed: {event}")

    except Exception as e:
        logger.error(f"Event processing error: {e}")

    return jsonify({"code": 0})


# -------------------------------------------------------------------------
# Sheets Export Endpoint
# -------------------------------------------------------------------------
@app.route("/export-data", methods=["GET"])
def export_data():
    """Export project data as JSON (can be used to create sheets)."""
    try:
        projects = fetch_all_projects()
        return jsonify({
            "status": "ok",
            "total_records": len(projects),
            "records": projects[:500]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------------------------------------------------------
# Chat Management Endpoint
# -------------------------------------------------------------------------
@app.route("/create-project-chat", methods=["POST"])
def create_project_chat():
    """Create a new project-specific chat."""
    body = request.get_json(silent=True) or {}
    name = body.get("name", "")
    user_ids = body.get("user_ids", [])

    if not name:
        return jsonify({"error": "Chat name required"}), 400

    try:
        result = lark.create_chat(name, user_ids=user_ids)
        return jsonify({"status": "ok", "chat": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------------------------------------------------------
# Startup
# -------------------------------------------------------------------------
_init_db()  # Initialize Postgres conversation tables
_init_facts_table()  # Initialize Postgres user facts table
threading.Thread(target=_fetch_bot_open_id, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

