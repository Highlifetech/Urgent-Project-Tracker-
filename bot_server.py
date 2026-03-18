import os
import logging
import json
import re
import time
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import anthropic
from lark_client import LarkClient
from netsuite_client import NetSuiteClient
from pipedrive_client import PipedriveClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
app = Flask(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
BOT_NAME = os.environ.get("BOT_NAME", "Iron Bot")  # Set to the bot's display name in Lark
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
PROJECTS_CACHE_TTL = 120  # seconds


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

def ask_gemini(question, projects, netsuite_data=None, scope="brendan", pipedrive_data=None, wiki_pages=None, comments_data=None):
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
    if scope == "hannah":
        scope_instruction = "IMPORTANT: You are speaking with Hannah. Only discuss Hannah's projects and boards. Do not mention Lucy's or Brendan's projects.\n"
    elif scope == "lucy":
        scope_instruction = "IMPORTANT: You are speaking with Lucy. Only discuss Lucy's projects and boards. Do not mention Hannah's or Brendan's projects.\n"
    else:
        scope_instruction = ""
    system_prompt = (
        scope_instruction +
        "You are IRON BOT — the HLT (Highlife Tech) company assistant. "
        "You have access to live production, project, shipping, client, and CRM data.\n\n"
        "RESPONSE RULES (follow strictly):\n"
        "- Give ONLY the final answer. Never show your reasoning or thinking process.\n"
        "- Never say 'let me re-check' or 're-evaluate' or explain your logic.\n"
        "- Be concise and direct. Use bullet points for lists.\n"
        "- Highlight urgent or overdue items clearly.\n"
        "- If nothing matches, say so in one sentence.\n\n"
        "FIELD NOTES:\n"
        "- 'Due Date' in data = 'In Hand Date' (date client needs delivery). Always call it 'In Hand Date'.\n"
        "- Timestamps are Unix milliseconds — convert to readable dates in all answers.\n"
        "- Board ownership: tables with 'Lucy' = Lucy's, 'Hannah' = Hannah's, else Brendan's.\n\n"
        "STATUS VALUES:\n"
        "- WAITING ART = awaiting artwork, not yet paid\n"
        "- PAID/WAITING ART = paid, awaiting artwork\n"
        "- QUOTE NEEDED = needs price quote only (NOT artwork)\n"
        "- QUOTE ADDED = quote provided, awaiting decision\n"
        "- ARTWORK CONFIRMED = artwork approved, ready for production\n"
        "- PART CONFIRMED = partially confirmed\n"
        "- PLATING = in production, plating stage\n"
        "- POLISHING = in production, polishing stage\n"
        "- PART SHIPPED = partially shipped\n"
        "- SHIPPED = completed and shipped\n"
        "- RESOLVED/SHIPPED = resolved and shipped\n"
        "- NEEDS RESOLUTION = active problem\n\n"
        "FILTERING RULES:\n"
        "- 'awaiting artwork' = ONLY WAITING ART or PAID/WAITING ART\n"
        "- 'needs quote' = ONLY QUOTE NEEDED\n"
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
        "\nQuestion: " + question
    )
    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
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
        msg = f"✅ Artwork confirmed for {order_num}. Status updated to '{ARTWORK_CONFIRMED_STATUS}'."
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
# Message processing (runs in background thread)
# -------------------------------------------------------------------------
def _process_message(user_text, chat_id, artwork_order, scope="brendan"):
    if artwork_order:
        answer = handle_artwork_approval(artwork_order, user_text, chat_id)
        try:
            lark.send_response(answer, chat_id=chat_id)
        except Exception as e:
            logger.error("Send failed: " + str(e))
        return
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
    t1.start(); t2.start(); t3.start(); t4.start(); t5.start()
    t1.join(); t2.join(); t3.join(); t4.join(); t5.join()
    projects = projects_result.get("data", [])
    netsuite_data = netsuite_result.get("data")
    pipedrive_data = pipedrive_result.get("data")
    wiki_pages = wiki_result.get("data", [])
    comments_data = comments_result.get("data")
    # Apply user scope filter BEFORE passing to Claude
    scoped_projects = filter_projects_by_scope(projects, scope)
    if not scoped_projects and not netsuite_data:
        answer = "Could not load project data. Check bot access to Lark Base."
    else:
        answer = ask_gemini(user_text, scoped_projects, netsuite_data, scope=scope, pipedrive_data=pipedrive_data, wiki_pages=wiki_pages, comments_data=comments_data)
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
        args=(user_text, chat_id, artwork_order, scope),
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
DIGEST_EXCLUDED_BOARDS = ["quote", "partial ship", "part ship"]

def _is_digest_excluded_board(table_name):
    """Return True if this board should be excluded from the morning digest."""
    tname = table_name.lower()
    return any(excl in tname for excl in DIGEST_EXCLUDED_BOARDS)


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
        due_raw = p.get("Due Date") or p.get("In-Hand Date") or p.get("In Hand Date")
        due_ms = None
        if isinstance(due_raw, (int, float)):
            due_ms = float(due_raw)
        days_until = None
        if due_ms is not None:
            days_until = (due_ms - today_ms) / (1000 * 60 * 60 * 24)

        if due_ms and days_until is not None and days_until < 0:
            overdue.append({**p, "_days_overdue": abs(int(days_until))})
        elif due_ms and days_until is not None and days_until <= 7:
            due_soon.append({**p, "_days_until": int(days_until)})
        elif "PLATING" in status or "POLISHING" in status or "PART CONFIRMED" in status:
            in_production.append(p)
        elif "WAITING ART" in status or "PAID/WAITING" in status:
            awaiting_art.append(p)

    def fmt(p):
        order_num = p.get("Order #", "") or "No Order #"
        client = p.get("Client") or p.get("__table_name__", "")
        status = p.get("Status", "")
        return f"- {order_num} | {status}"

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
        "You are IRON BOT — the HLT (Highlife Tech) internal assistant. "
        "Write a morning briefing for the team. Be concise, direct, and actionable. "
        "Use emojis sparingly for visual scanning (🔴 overdue, 🟡 due soon, 🔵 in production, ⚪ awaiting art). "
        "Your digest MUST include these 4 sections in order: "
        "## NEEDS ARTWORK — X projects | "
        "## OVERDUE — X projects | "
        "## DUE WITHIN 7 DAYS — X projects | "
        "## TODAY'S PRIORITY LIST (numbered, most urgent first). "
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
        return jsonify({"error": "LARK_CHAT_ID_DIGEST not configured — morning digest can only post to the designated digest channel"}), 500

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
# Startup
# -------------------------------------------------------------------------
threading.Thread(target=_fetch_bot_open_id, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
