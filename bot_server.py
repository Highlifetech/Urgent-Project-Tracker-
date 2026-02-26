"""
Lark Project Tracker — Interactive Bot Server

This Flask server:
1. Receives messages from Lark chat via webhook
2. Fetches live project data from Lark Base
3. Sends data + question to Gemini AI
4. Returns the answer back to Lark chat
"""
import os
import logging
import json
from datetime import datetime, timezone

from flask import Flask, request, jsonify
import google.generativeai as genai

from lark_client import LarkClient

logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# -------------------------------------------------------------------------
# Gemini setup
# -------------------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
      logger.error("GEMINI_API_KEY is not set!")
else:
      logger.info(f"GEMINI_API_KEY loaded (length={len(GEMINI_API_KEY)})")

genai.configure(api_key=GEMINI_API_KEY)

# Try multiple model names in case one is unavailable
def _init_gemini():
      for model_name in ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-flash-latest", "gemini-pro"]:
                try:
                              m = genai.GenerativeModel(model_name)
                              # Quick test to verify the model works
                              logger.info(f"Gemini model initialized: {model_name}")
                              return m, model_name
except Exception as e:
            logger.warning(f"Model {model_name} failed: {e}")
    return None, None

gemini_model, gemini_model_name = _init_gemini()

LARK_VERIFICATION_TOKEN = os.environ.get("LARK_VERIFICATION_TOKEN", "")

# Track processed message IDs to prevent duplicate replies
processed_message_ids = set()

lark = LarkClient()


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def fetch_all_projects() -> list:
      """Pull ALL records from every table in the Base (including all fields raw)."""
      projects = []
      try:
                tables = lark.get_all_tables()
                logger.info(f"Found {len(tables)} tables: {[t['name'] for t in tables]}")
except Exception as e:
        logger.error(f"Failed to get tables: {e}")
        return projects

    for table in tables:
              table_id = table["table_id"]
              table_name = table.get("name", "Unknown")
              try:
                            records = lark.get_table_records(table_id)
                            logger.info(f"Table '{table_name}': {len(records)} records")
                            for raw in records:
                                              # Store raw fields AND table name so Gemini has full context
                                              fields = raw.get("fields", {})
                                              fields["__table_name__"] = table_name
                                              projects.append(fields)
              except Exception as e:
                            logger.error(f"Failed to read table '{table_name}' ({table_id}): {e}")

          logger.info(f"Total records fetched: {len(projects)}")
    return projects


def field_to_text(val) -> str:
      """Convert any Lark field value to a readable string."""
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
    if isinstance(val, (int, float)):
              # Could be a timestamp in ms
              if val > 1_000_000_000_000:
                            try:
                                              dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
                                              return dt.strftime("%b %d, %Y")
except Exception:
                pass
        return str(val)
    return str(val).strip() or "N/A"


def build_context(projects: list) -> str:
      """Format ALL project data as readable text for Gemini."""
    today = datetime.now(timezone.utc)
    today_str = today.strftime("%A, %B %d %Y")
    lines = [f"Today is {today_str}.", f"Total records: {len(projects)}", ""]

    for p in projects:
              table_name = p.get("__table_name__", "Unknown Board")
        line_parts = [f"[Board: {table_name}]"]
        for key, val in p.items():
                      if key == "__table_name__":
                                        continue
                                    text_val = field_to_text(val)
            line_parts.append(f"{key}: {text_val}")
        lines.append(" | ".join(line_parts))

    return "\n".join(lines)


def ask_gemini(user_question: str, projects: list) -> str:
      """Send live project data + user question to Gemini and return the answer."""
    if not gemini_model:
              return "Sorry, the AI model is not available right now. Please check the GEMINI_API_KEY."

    context = build_context(projects)
    logger.info(f"Context size: {len(context)} chars, {len(projects)} records")

    prompt = f"""You are a helpful project tracking assistant for HLT (Highlife Tech).

    You have access to live project board data below. Each record shows all fields from the Lark Base boards.
    Board names tell you who owns them: boards with "Lucy" in the name belong to Lucy, "Hannah" belongs to Hannah, others belong to Brendan.

    Answer the user's question accurately based only on this data. Be friendly and concise.
    Use bullet points for lists. Highlight overdue items clearly.
    If you don't have enough data to answer, say so honestly.

    --- LIVE PROJECT DATA ---
    {context}
    --- END DATA ---

    User question: {user_question}

    Answer:"""

    try:
              logger.info(f"Sending to Gemini model: {gemini_model_name}")
        response = gemini_model.generate_content(prompt)
        answer = response.text.strip()
        logger.info(f"Gemini answered ({len(answer)} chars)")
        return answer
except Exception as e:
        logger.error(f"Gemini error: {type(e).__name__}: {e}")
        return f"Sorry, I had trouble getting an answer. Error: {type(e).__name__}: {str(e)[:200]}"


# -------------------------------------------------------------------------
# Lark webhook endpoint
# -------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
      body = request.get_json(silent=True) or {}

    # Step 1: Handle Lark URL verification challenge
    if body.get("type") == "url_verification":
              logger.info("Lark URL verification challenge received")
        return jsonify({"challenge": body.get("challenge", "")})

    # Step 2: Handle incoming messages
    event = body.get("event", {})
    msg = event.get("message", {})
    msg_type = msg.get("message_type", "")

    if msg_type != "text":
              return jsonify({"code": 0})

    message_id = msg.get("message_id", "")

    # Deduplicate
    if message_id in processed_message_ids:
              return jsonify({"code": 0})
    processed_message_ids.add(message_id)
    if len(processed_message_ids) > 1000:
              processed_message_ids.clear()

    # Extract text
    try:
              content = json.loads(msg.get("content", "{}"))
        user_text = content.get("text", "").strip()
        # Remove @mentions (e.g. "@URGENT PROJECT TRACKER ")
        if user_text.startswith("@"):
                      # Strip the @mention prefix
                      parts = user_text.split(" ", 1)
            user_text = parts[1].strip() if len(parts) > 1 else ""
except Exception:
        return jsonify({"code": 0})

    if not user_text:
              return jsonify({"code": 0})

    chat_id = msg.get("chat_id", "")
    if not chat_id:
              return jsonify({"code": 0})

    logger.info(f"Question: '{user_text}' from chat {chat_id}")

    # Step 3: Fetch live data and ask Gemini
    projects = fetch_all_projects()

    if not projects:
              logger.warning("No projects fetched — check LARK_BASE_APP_TOKEN and table access")
        answer = "I couldn't load any project data right now. Please check that the bot has access to the Lark Base."
else:
        answer = ask_gemini(user_text, projects)

    logger.info(f"Reply: {answer[:150]}...")

    # Step 4: Send reply back
    try:
              lark.send_group_message(answer, chat_id=chat_id)
except Exception as e:
        logger.error(f"Failed to send reply: {e}")

    return jsonify({"code": 0})


@app.route("/health", methods=["GET"])
def health():
      tables = []
    try:
              tables = lark.get_all_tables()
except Exception as e:
        logger.error(f"Health check - table fetch failed: {e}")
    return jsonify({
              "status": "ok",
              "service": "HLT Project Tracker Bot",
              "gemini_model": gemini_model_name,
              "gemini_ready": gemini_model is not None,
              "lark_tables": len(tables),
              "table_names": [t["name"] for t in tables],
    })


if __name__ == "__main__":
      port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting bot server on port {port}")
    app.run(host="0.0.0.0", port=port)
