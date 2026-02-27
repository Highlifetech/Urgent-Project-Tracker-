import os
import logging
import json
import re
import time
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from google import genai
from lark_client import LarkClient
from netsuite_client import NetSuiteClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
app = Flask(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
BOT_NAME = os.environ.get("BOT_NAME", "Iron Bot")  # Set to the bot's display name in Lark
from config import LARK_CHAT_ID_HANNAH_ARTWORK, LARK_CHAT_ID_LUCY_ARTWORK, FIELD_PRODUCTION_DRAWING, ARTWORK_CONFIRMED_STATUS

# Known user open_ids for scoping (set via env vars or defaults)
HANNAH_OPEN_ID = os.environ.get("HANNAH_OPEN_ID", "ou_42c3063bcfefad67c05c615ba0088146")
LUCY_OPEN_ID = os.environ.get("LUCY_OPEN_ID", "")

gemini_client = genai.Client(api_key=GEMINI_API_KEY)
gemini_model_name = "gemini-2.5-flash"
logger.info("Gemini client ready, model: " + gemini_model_name)

# Deduplication: dict of {message_id: timestamp}
processed_message_ids = {}
DEDUP_TTL = 300

# Store last webhook payloads for debugging
_last_webhooks = []

# Bot's own open_id - fetched at startup
BOT_OPEN_ID = None

lark = LarkClient()
netsuite = NetSuiteClient()


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
            return netsuite.get_customer_balances()
        elif netsuite_type == "address":
            return netsuite.get_shipping_addresses()
        elif netsuite_type == "shipping":
            return netsuite.get_shipment_tracking()
    except Exception as e:
        logger.error("NetSuite fetch error: " + str(e))
        return {"error": str(e)}
    return None


# -------------------------------------------------------------------------
# Lark project fetching
# -------------------------------------------------------------------------
def fetch_all_projects():
    try:
        return lark.get_all_records()
    except Exception as e:
        logger.error("Lark fetch error: " + str(e))
        return []


# -------------------------------------------------------------------------
# Gemini
# -------------------------------------------------------------------------
def ask_gemini(question, projects, netsuite_data=None, scope="brendan"):
    if not GEMINI_API_KEY:
        return "AI model not available. Check GEMINI_API_KEY."
    relevant = filter_relevant_projects(question, projects)
    context = build_context(relevant)
    netsuite_section = ""
    if netsuite_data:
        if "error" in netsuite_data:
            netsuite_section = "\n--- NETSUITE DATA ---\nError fetching data: " + netsuite_data["error"] + "\n--- END NETSUITE ---\n"
        else:
            netsuite_section = "\n--- NETSUITE DATA ---\n" + json.dumps(netsuite_data, indent=2)[:4000] + "\n--- END NETSUITE ---\n"

    # Scope instruction for the prompt
    if scope == "hannah":
        scope_instruction = "IMPORTANT: You are speaking with Hannah. Only discuss Hannah's projects and boards. Do not mention or reveal details about Lucy's or Brendan's projects.\n"
    elif scope == "lucy":
        scope_instruction = "IMPORTANT: You are speaking with Lucy. Only discuss Lucy's projects and boards. Do not mention or reveal details about Hannah's or Brendan's projects.\n"
    else:
        scope_instruction = ""

    prompt = (
        scope_instruction +
        "You are IRON BOT — the HLT (Highlife Tech) Production Assistant for production, projects, shipping, addresses, and client balances.\n"
        "Board ownership: tables with Lucy in the name belong to Lucy, Hannah to Hannah, everything else to Brendan.\n"
        "Be helpful and concise. Use bullet points. Highlight overdue or urgent items.\n"
        "For shipping, tracking, addresses and client balances, use the NetSuite data provided.\n"
        "For production boards and project tasks, use the Lark data.\n\n"
        "--- LARK PROJECT DATA ---\n" + context + "\n--- END LARK DATA ---\n"
        + netsuite_section +
        "\nQuestion: " + question + "\nAnswer:"
    )
    models_to_try = [gemini_model_name, "gemini-2.0-flash-lite", "gemini-2.0-flash-lite-001", "gemini-1.5-flash-8b"]
    last_error = None
    for model in models_to_try:
        try:
            resp = gemini_client.models.generate_content(model=model, contents=prompt)
            answer = resp.text.strip()
            logger.info("Gemini replied using " + model + ": " + str(len(answer)) + " chars")
            return answer
        except Exception as e:
            logger.warning("Model " + model + " failed: " + str(e)[:100])
            last_error = e
    return "AI error: " + str(last_error)[:300]


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
    def get_lark():
        projects_result["data"] = fetch_all_projects()
    def get_netsuite():
        data = fetch_netsuite_data(user_text)
        if data:
            netsuite_result["data"] = data
    t1 = threading.Thread(target=get_lark)
    t2 = threading.Thread(target=get_netsuite)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    projects = projects_result.get("data", [])
    netsuite_data = netsuite_result.get("data")
    # Apply user scope filter BEFORE passing to Gemini
    scoped_projects = filter_projects_by_scope(projects, scope)
    if not scoped_projects and not netsuite_data:
        answer = "Could not load project data. Check bot access to Lark Base."
    else:
        answer = ask_gemini(user_text, scoped_projects, netsuite_data, scope=scope)
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
        "gemini_ready": bool(GEMINI_API_KEY),
        "gemini_model": gemini_model_name,
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
    try:
        models = gemini_client.models.list()
        model_names = [m.name for m in models]
        return jsonify({"models": model_names, "count": len(model_names)})
    except Exception as e:
        return jsonify({"error": str(e)})


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


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID, "model": gemini_model_name})


# -------------------------------------------------------------------------
# Startup
# -------------------------------------------------------------------------
_fetch_bot_open_id()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
