"""
Configuration for HLT Iron Bot
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
# LARK BASE APP TOKEN
# =============================================================================
LARK_BASE_APP_TOKEN = os.environ.get("LARK_BASE_APP_TOKEN", "")

# Base URL for record links (JP region)
LARK_BASE_RECORD_URL = os.environ.get(
    "LARK_BASE_RECORD_URL",
    "https://ojpglhhzxlvc.jp.larksuite.com/base/",
)

# =============================================================================
# LARK GROUP CHATS
# =============================================================================
# Founders Channel (Brendan) - receives Notify cards, digests, confirmations
LARK_CHAT_ID_FOUNDERS = os.environ.get("LARK_CHAT_ID_DIGEST", "")

# Production channels for Hannah and Lucy
LARK_CHAT_ID_HANNAH = os.environ.get("LARK_CHAT_ID_HANNAH", "")
LARK_CHAT_ID_LUCY = os.environ.get("LARK_CHAT_ID_LUCY", "")
LARK_CHAT_ID_CHEN = os.environ.get("LARK_CHAT_ID_CHEN", "")
LARK_CHAT_ID_MASTER = os.environ.get("LARK_CHAT_ID_MASTER", "")

# Artwork approval channels
LARK_CHAT_ID_HANNAH_ARTWORK = os.environ.get("LARK_CHAT_ID_HANNAH_ARTWORK", "")
LARK_CHAT_ID_LUCY_ARTWORK = os.environ.get("LARK_CHAT_ID_LUCY_ARTWORK", "")

# All other known chats
LARK_CHAT_ID_PRODUCTION_CHEN = os.environ.get("LARK_CHAT_ID_CHEN", "")
LARK_CHAT_ID_SHIPMENTS_LUCY = os.environ.get("LARK_CHAT_ID_SHIPMENTS_LUCY", "")
LARK_CHAT_ID_HLT_DESIGN = os.environ.get("LARK_CHAT_ID_HLT_DESIGN", "")
LARK_CHAT_ID_ORDER_ISSUES_HANNAH = os.environ.get("LARK_CHAT_ID_ORDER_ISSUES_HANNAH", "")
LARK_CHAT_ID_ORDER_ISSUES_LUCY = os.environ.get("LARK_CHAT_ID_ORDER_ISSUES_LUCY", "")
LARK_CHAT_ID_QUOTES_HANNAH = os.environ.get("LARK_CHAT_ID_QUOTES_HANNAH", "")
LARK_CHAT_ID_QUOTES_LUCY = os.environ.get("LARK_CHAT_ID_QUOTES_LUCY", "")
LARK_CHAT_ID_SAMPLES_LUCY = os.environ.get("LARK_CHAT_ID_SAMPLES_LUCY", "")
LARK_CHAT_ID_HLT_CARLO = os.environ.get("LARK_CHAT_ID_HLT_CARLO", "")
LARK_CHAT_ID_HLT_INBOUND = os.environ.get("LARK_CHAT_ID_HLT_INBOUND", "")

# =============================================================================
# AI
# =============================================================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# =============================================================================
# LARK WEBHOOK VERIFICATION
# =============================================================================
LARK_VERIFICATION_TOKEN = os.environ.get("LARK_VERIFICATION_TOKEN", "")
LARK_ENCRYPT_KEY = os.environ.get("LARK_ENCRYPT_KEY", "")

# =============================================================================
# CARD CALLBACK VERIFICATION
# =============================================================================
CARD_CALLBACK_VERIFICATION_TOKEN = os.environ.get("CARD_CALLBACK_VERIFICATION_TOKEN", "")

# =============================================================================
# NETSUITE (Optional)
# =============================================================================
NETSUITE_ACCOUNT_ID = os.environ.get("NETSUITE_ACCOUNT_ID", "")
NETSUITE_CONSUMER_KEY = os.environ.get("NETSUITE_CONSUMER_KEY", "")
NETSUITE_CONSUMER_SECRET = os.environ.get("NETSUITE_CONSUMER_SECRET", "")
NETSUITE_TOKEN_ID = os.environ.get("NETSUITE_TOKEN_ID", "")
NETSUITE_TOKEN_SECRET = os.environ.get("NETSUITE_TOKEN_SECRET", "")

# =============================================================================
# FIELD NAMES (must match Lark Base columns exactly)
# =============================================================================
FIELD_ORDER_NUM = "Sales Order"
FIELD_ORDER_DATE = "Order Date"
FIELD_DUE_DATE = "In-Hand Date"
FIELD_STATUS = "Status"
FIELD_DESCRIPTION = "Description"
FIELD_ADDRESS = "Address"
FIELD_QTY_ORDERED = "Quantity"
FIELD_PRODUCTION_DRAWING = "Production Drawing"
FIELD_CLIENT = "Client Name"
FIELD_ASSIGNED_TO = "Assigned To"
FIELD_PRODUCTION_ARTWORK = "Production Artwork"
FIELD_CLIENT_EMAIL = "Client Email"
FIELD_VENDOR = "Vendor"
ARTWORK_CONFIRMED_STATUS = "Artwork Confirmed"

# Alternate field names (some tables may use different names)
ALT_ORDER_NUM_FIELDS = ["Sales Order", "Order #", "SO#", "Order Number"]
ALT_CLIENT_FIELDS = ["Client Name", "CLIENT", "Client"]
ALT_STATUS_FIELDS = ["Status"]
ALT_DUE_DATE_FIELDS = ["In-Hand Date", "Due Date", "In Hand Date"]

# =============================================================================
# KNOWN USER OPEN IDS
# =============================================================================
BRENDAN_OPEN_ID = os.environ.get("BRENDAN_OPEN_ID", "")
HANNAH_OPEN_ID = os.environ.get("HANNAH_OPEN_ID", "ou_42c3063bcfefad67c05c615ba0088146")
LUCY_OPEN_ID = os.environ.get("LUCY_OPEN_ID", "ou_0f26700382eae7f58ea889b7e98388b4")

# =============================================================================
# BOT SETTINGS
# =============================================================================
DONE_STATUS = "Shipped"
SKIP_STATUSES = ["Shipped", "Artwork Confirmed", "RESOLVED/SHIPPED"]
WARNING_DAYS = [21, 14, 7]
WARNING_LABELS = {21: "3 weeks", 14: "2 weeks", 7: "1 week"}

# =============================================================================
# CHAT ROUTING
# =============================================================================
CHAT_ROUTING = {
    "hannah": LARK_CHAT_ID_HANNAH,
    "lucy": LARK_CHAT_ID_LUCY,
    "chen": LARK_CHAT_ID_CHEN,
}

NOTIFICATION_MASTER_CHAT = LARK_CHAT_ID_MASTER
NOTIFICATION_CHATS = [
    LARK_CHAT_ID_HANNAH,
    LARK_CHAT_ID_LUCY,
    LARK_CHAT_ID_CHEN,
    LARK_CHAT_ID_MASTER,
]

# =============================================================================
# CALENDAR / DIGEST
# =============================================================================
LARK_PRIMARY_CALENDAR_ID = os.environ.get("LARK_PRIMARY_CALENDAR_ID", "")
LARK_CHAT_ID_DIGEST = os.environ.get("LARK_CHAT_ID_DIGEST", "")
DIGEST_SECRET = os.environ.get("DIGEST_SECRET", "")

# =============================================================================
# BOARDS TO EXCLUDE FROM DIGEST (exact match, case-insensitive)
# =============================================================================
DIGEST_EXCLUDED_BOARDS = {
    "hannah quotes",
    "lucy quotes",
    "brendan quotes",
    "master production tab",
    "quick links",
}

# =============================================================================
# VIEW FILTER - only use "ALL ORDERS" views to avoid duplicates
# =============================================================================
ALL_ORDERS_VIEW_KEYWORD = "all orders"

