import os
import logging
import json
import re
import time
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import google.generativeai as genai
from lark_client import LarkClient
from netsuite_client import NetSuiteClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
app = Flask(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
genai.configure(api_key=GEMINI_API_KEY)

gemini_model = None
gemini_model_name = None
for _name in ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-pro"]:
    try:
        gemini_model = genai.GenerativeModel(_name)
        gemini_model_name = _name
        logger.info("Gemini model loaded: " + _name)
        break
    except Exception as _e:
        logger.warning("Model " + _name + " failed: " + str(_e))

processed_message_ids = set()
lark = LarkClient()
netsuite = NetSuiteClient()

# -------------------------------------------------------------------------
# Cache — avoid re-fetching Lark data on every message
# -------------------------------------------------------------------------
_cache_lock = threading.Lock()
_cached_projects = []
_cache_timestamp = 0
CACHE_TTL = 300  # seconds (5 minutes)


def fetch_all_projects(force=False):
    global _cached_projects, _cache_timestamp
    with _cache_lock:
        age = time.time() - _cache_timestamp
        if not force and _cached_projects and age < CACHE_TTL:
            logger.info("Using cached project data (" + str(int(age)) + "s old, " + str(len(_cached_projects)) + " records)")
            return _cached_projects

    projects = []
    try:
        tables = lark.get_all_tables()
        logger.info("Found " + str(len(tables)) + " tables")
    except Exception as e:
        logger.error("Failed to get tables: " + str(e))
        with _cache_lock:
            return _cached_projects  # return stale cache on error rather than empty

    for table in tables:
        tid = table["table_id"]
        tname = table.get("name", "Unknown")
        try:
            records = lark.get_table_records(tid)
            for raw in records:
                fields = dict(raw.get("fields", {}))
                fields["__table_name__"] = tname
                projects.append(fields)
            logger.info("Table " + tname + ": " + str(len(records)) + " records")
        except Exception as e:
            logger.error("Failed table " + tname + ": " + str(e))

    with _cache_lock:
        _cached_projects = projects
        _cache_timestamp = time.time()
    logger.info("Cache refreshed: " + str(len(projects)) + " total records")
    return projects


def _refresh_cache_background():
    """Refresh cache in a background thread so the next request is fast."""
    try:
        fetch_all_projects(force=True)
    except Exception as e:
        logger.error("Background cache refresh failed: " + str(e))


def field_to_text(val):
    if val is None:
        return "N/A"
    if isinstance(val, list):
        parts = []
        for item in val:
            if isinstance(item, dict):
                parts.append(item.get("text", str(item)))
            else:
                parts.append(str(item))
        return " ".join(parts).strip() or "N/A"
    if isinstance(val, dict):
        return val.get("text", str(val)).strip() or "N/A"
    if isinstance(val, (int, float)) and val > 1000000000000:
        try:
            dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
            return dt.strftime("%b %d, %Y")
        except Exception:
            pass
    return str(val).strip() or "N/A"


def filter_relevant_projects(question, projects):
    """Only send records relevant to the question to Gemini — much faster."""
    q = question.lower()

    # Keywords to filter by table name or field content
    keywords = [w for w in q.split() if len(w) > 3]

    # Always include if question is broad
    broad = any(w in q for w in ["all", "every", "list", "show", "overview", "summary", "status"])
    if broad or not keywords:
        return projects[:200]  # cap at 200 records max

    # Filter by keyword match in table name or any field value
    relevant = []
    for p in projects:
        tname = p.get("__table_name__", "").lower()
        row_text = " ".join(str(v) for v in p.values()).lower()
        if any(kw in tname or kw in row_text for kw in keywords):
            relevant.append(p)

    # If nothing matched, fall back to all (capped)
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


def detect_netsuite_query(question):
    q = question.lower()
    shipping_keywords = [
        "ship", "tracking", "shipment", "transit", "order status",
        "fulfillment", "so-", "sales order", "netsuite", "shipped",
        "delivery", "carrier", "fedex", "ups", "usps", "dhl", "in transit"
    ]
    return any(kw in q for kw in shipping_keywords)


def fetch_netsuite_shipping(question):
    so_match = re.search(r'\bso[\s\-]?(\d+)\b', question, re.IGNORECASE)
    order_num = so_match.group(0).replace(" ", "-").upper() if so_match else None
    num_match = re.search(r'\b(\d{4,})\b', question)
    if not order_num and num_match:
        order_num = num_match.group(1)
    try:
        if order_num:
            data = netsuite.get_shipment_by_order(order_num)
        else:
            data = netsuite.get_recent_shipments()
        return data
    except Exception as e:
        logger.error("NetSuite error: " + str(e))
        return {"error": str(e)}


def ask_gemini(question, projects, netsuite_data=None):
    if not gemini_model:
        return "AI model not available. Check GEMINI_API_KEY."

    relevant = filter_relevant_projects(question, projects)
    context = build_context(relevant)
    logger.info("Sending " + str(len(relevant)) + " records to Gemini (filtered from " + str(len(projects)) + ")")

    netsuite_section = ""
    if netsuite_data:
        if "error" in netsuite_data:
            netsuite_section = "\n--- NETSUITE DATA ---\nError: " + netsuite_data["error"] + "\n--- END NETSUITE ---\n"
        else:
            netsuite_section = "\n--- NETSUITE SHIPPING DATA ---\n" + json.dumps(netsuite_data, indent=2)[:3000] + "\n--- END NETSUITE ---\n"

    prompt = (
        "You are IRON BOT — the HLT (Highlife Tech) Production Assistant for all things production, projects, shipping, and operations.\n"
        "Board ownership: tables with Lucy in the name belong to Lucy, Hannah to Hannah, everything else to Brendan.\n"
        "Be helpful and concise. Use bullet points for lists. Highlight overdue or urgent items.\n"
        "If asked about shipping or orders, use NetSuite data. For production boards and tasks, use Lark data.\n\n"
        "--- LARK PROJECT DATA ---\n" + context + "\n--- END LARK DATA ---\n"
        + netsuite_section +
        "\nQuestion: " + question + "\nAnswer:"
    )
    try:
        resp = gemini_model.generate_content(prompt)
        answer = resp.text.strip()
        logger.info("Gemini replied: " + str(len(answer)) + " chars")
        return answer
    except Exception as e:
        logger.error("Gemini error: " + str(e))
        return "AI error: " + str(e)[:300]


def extract_question(msg):
    try:
        content = json.loads(msg.get("content", "{}"))
        raw_text = content.get("text", "").strip()
    except Exception:
        return None
    mentions = msg.get("mentions", [])
    bot_mentioned = bool(mentions) or raw_text.startswith("@")
    if not bot_mentioned:
        return None
    if raw_text.startswith("@"):
        space_idx = raw_text.find(" ")
        if space_idx == -1:
            return None
        raw_text = raw_text[space_idx:].strip()
    return raw_text if raw_text else None


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
    if message_id in processed_message_ids:
        return jsonify({"code": 0})
    processed_message_ids.add(message_id)
    if len(processed_message_ids) > 1000:
        processed_message_ids.clear()
    user_text = extract_question(msg)
    if not user_text:
        return jsonify({"code": 0})
    chat_id = msg.get("chat_id", "")
    if not chat_id:
        return jsonify({"code": 0})
    logger.info("Question: " + repr(user_text) + " chat=" + chat_id)

    # Fetch Lark data and NetSuite data in parallel using threads
    netsuite_data = None
    netsuite_result = {}
    projects_result = {}

    def get_lark():
        projects_result["data"] = fetch_all_projects()

    def get_netsuite():
        if detect_netsuite_query(user_text):
            logger.info("NetSuite query detected")
            netsuite_result["data"] = fetch_netsuite_shipping(user_text)

    t1 = threading.Thread(target=get_lark)
    t2 = threading.Thread(target=get_netsuite)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    projects = projects_result.get("data", [])
    netsuite_data = netsuite_result.get("data")

    if not projects and not netsuite_data:
        answer = "Could not load project data. Check bot access to Lark Base."
    else:
        answer = ask_gemini(user_text, projects, netsuite_data)

    try:
        lark.send_response(answer, chat_id=chat_id)
    except Exception as e:
        logger.error("Send failed: " + str(e))

    # Pre-warm cache in background so next question is even faster
    cache_age = time.time() - _cache_timestamp
    if cache_age > CACHE_TTL * 0.8:
        threading.Thread(target=_refresh_cache_background, daemon=True).start()

    return jsonify({"code": 0})


@app.route("/refresh", methods=["GET"])
def refresh():
    """Force a cache refresh."""
    threading.Thread(target=_refresh_cache_background, daemon=True).start()
    return jsonify({"status": "cache refresh triggered"})


@app.route("/debug", methods=["GET"])
def debug():
    result = {}
    result["env_app_id"] = bool(os.environ.get("LARK_APP_ID"))
    result["env_app_secret"] = bool(os.environ.get("LARK_APP_SECRET"))
    result["env_base_token"] = bool(os.environ.get("LARK_BASE_APP_TOKEN"))
    result["base_token_value"] = os.environ.get("LARK_BASE_APP_TOKEN", "")[:8] + "..."
    result["lark_base_url"] = os.environ.get("LARK_BASE_URL", "not set")
    result["gemini_ready"] = gemini_model is not None
    result["gemini_model"] = gemini_model_name
    result["netsuite_configured"] = bool(os.environ.get("NETSUITE_ACCOUNT_ID"))
    result["cache_records"] = len(_cached_projects)
    result["cache_age_seconds"] = int(time.time() - _cache_timestamp)

    try:
        token = lark._get_tenant_token()
        result["auth"] = "OK - token length " + str(len(token))
    except Exception as e:
        result["auth"] = "FAILED: " + str(e)

    try:
        tables = lark.get_all_tables()
        result["tables"] = [t["name"] for t in tables]
        result["table_count"] = len(tables)
    except Exception as e:
        result["tables"] = "FAILED: " + str(e)

    return jsonify(result)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "gemini_model": gemini_model_name, "cache_records": len(_cached_projects)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
