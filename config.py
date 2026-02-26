"""
Configuration for HLT Production Bot
All settings loaded from environment variables (Railway / GitHub Secrets)
"""

import os

# =============================================================================
# LARK APP CREDENTIALS
# =============================================================================
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET", "")
LARK_BASE_URL = os.environ.get("LARK_BASE_URL", "https://open.larksuite.com")

# =============================================================================
# LARK GROUP CHATS
# Hannah and Lucy each get their own warning channel.
# The chatbot replies to whatever channel the question came from.
# =============================================================================
LARK_CHAT_ID_HANNAH = os.environ.get("LARK_CHAT_ID_HANNAH", "")
LARK_CHAT_ID_LUCY = os.environ.get("LARK_CHAT_ID_LUCY", "")
LARK_CHAT_ID_HANNAH_ARTWORK = os.environ.get("LARK_CHAT_ID_HANNAH_ARTWORK", "")
LARK_CHAT_ID_LUCY_ARTWORK = os.environ.get("LARK_CHAT_ID_LUCY_ARTWORK", "")

# =============================================================================
# LARK BASE APP TOKEN
# From your Base URL: https://xxx.larksuite.com/base/<APP_TOKEN>
# All tables/boards inside are discovered automatically.
# =============================================================================
LARK_BASE_APP_TOKEN = os.environ.get("LARK_BASE_APP_TOKEN", "")

# =============================================================================
# GEMINI AI
# =============================================================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# =============================================================================
# LARK WEBHOOK VERIFICATION TOKEN
# =============================================================================
LARK_VERIFICATION_TOKEN = os.environ.get("LARK_VERIFICATION_TOKEN", "")

# =============================================================================
# NETSUITE (Optional — for shipping data)
# Add these to Railway env vars to enable NetSuite shipping queries:
#   NETSUITE_ACCOUNT_ID      e.g. 1234567
#   NETSUITE_CONSUMER_KEY
#   NETSUITE_CONSUMER_SECRET
#   NETSUITE_TOKEN_ID
#   NETSUITE_TOKEN_SECRET
# =============================================================================
NETSUITE_ACCOUNT_ID = os.environ.get("NETSUITE_ACCOUNT_ID", "")
NETSUITE_CONSUMER_KEY = os.environ.get("NETSUITE_CONSUMER_KEY", "")
NETSUITE_CONSUMER_SECRET = os.environ.get("NETSUITE_CONSUMER_SECRET", "")
NETSUITE_TOKEN_ID = os.environ.get("NETSUITE_TOKEN_ID", "")
NETSUITE_TOKEN_SECRET = os.environ.get("NETSUITE_TOKEN_SECRET", "")

# =============================================================================
# FIELD NAMES (must match exactly as they appear in your Lark Base columns)
# =============================================================================
FIELD_ORDER_NUM = "Order #"
FIELD_ORDER_DATE = "Order Date"
FIELD_DUE_DATE = "Due Date"
FIELD_STATUS = "Status"
FIELD_DESCRIPTION = "Description"
FIELD_ADDRESS = "Address"
FIELD_QTY_ORDERED = "Quantity Ordered"
FIELD_PRODUCTION_DRAWING = "Production Drawing"
ARTWORK_CONFIRMED_STATUS = "Artwork Confirmed"

# =============================================================================
# BOT SETTINGS
# =============================================================================
# Status that means a project is fully done
DONE_STATUS = "Shipped"

# Warning thresholds in days before due date
WARNING_DAYS = [21, 14, 7]  # 3 weeks, 2 weeks, 1 week

# Labels for each threshold
WARNING_LABELS = {
    21: "3 weeks",
    14: "2 weeks",
    7: "1 week",
}

# Mapping of table name keywords to chat IDs for scheduled warnings
# lucy -> Lucy's chat, hannah -> Hannah's chat, else -> Brendan
CHAT_ROUTING = {
    "hannah": LARK_CHAT_ID_HANNAH,
    "lucy": LARK_CHAT_ID_LUCY,
    "hannah_artwork": LARK_CHAT_ID_HANNAH_ARTWORK,
    "lucy_artwork": LARK_CHAT_ID_LUCY_ARTWORK,
}
