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
# LARK GROUP CHATS — Notification channels (due date warnings sent here)
# =============================================================================
LARK_CHAT_ID_HANNAH        = os.environ.get("LARK_CHAT_ID_HANNAH", "")
LARK_CHAT_ID_LUCY          = os.environ.get("LARK_CHAT_ID_LUCY", "")
LARK_CHAT_ID_CHEN          = os.environ.get("LARK_CHAT_ID_CHEN", "")
LARK_CHAT_ID_MASTER        = os.environ.get("LARK_CHAT_ID_MASTER", "")

# Artwork approval channels
LARK_CHAT_ID_HANNAH_ARTWORK = os.environ.get("LARK_CHAT_ID_HANNAH_ARTWORK", "")
LARK_CHAT_ID_LUCY_ARTWORK   = os.environ.get("LARK_CHAT_ID_LUCY_ARTWORK", "")

# All other known chats (bot is a member — used for Q&A responses)
LARK_CHAT_ID_PRODUCTION_CHEN     = os.environ.get("LARK_CHAT_ID_CHEN", "")
LARK_CHAT_ID_SHIPMENTS_LUCY      = os.environ.get("LARK_CHAT_ID_SHIPMENTS_LUCY", "")
LARK_CHAT_ID_HLT_DESIGN          = os.environ.get("LARK_CHAT_ID_HLT_DESIGN", "")
LARK_CHAT_ID_ORDER_ISSUES_HANNAH = os.environ.get("LARK_CHAT_ID_ORDER_ISSUES_HANNAH", "")
LARK_CHAT_ID_ORDER_ISSUES_LUCY   = os.environ.get("LARK_CHAT_ID_ORDER_ISSUES_LUCY", "")
LARK_CHAT_ID_QUOTES_HANNAH       = os.environ.get("LARK_CHAT_ID_QUOTES_HANNAH", "")
LARK_CHAT_ID_QUOTES_LUCY         = os.environ.get("LARK_CHAT_ID_QUOTES_LUCY", "")
LARK_CHAT_ID_SAMPLES_LUCY        = os.environ.get("LARK_CHAT_ID_SAMPLES_LUCY", "")
LARK_CHAT_ID_HLT_CARLO           = os.environ.get("LARK_CHAT_ID_HLT_CARLO", "")
LARK_CHAT_ID_HLT_INBOUND         = os.environ.get("LARK_CHAT_ID_HLT_INBOUND", "")

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
# NETSUITE (Optional)
# =============================================================================
NETSUITE_ACCOUNT_ID      = os.environ.get("NETSUITE_ACCOUNT_ID", "")
NETSUITE_CONSUMER_KEY    = os.environ.get("NETSUITE_CONSUMER_KEY", "")
NETSUITE_CONSUMER_SECRET = os.environ.get("NETSUITE_CONSUMER_SECRET", "")
NETSUITE_TOKEN_ID        = os.environ.get("NETSUITE_TOKEN_ID", "")
NETSUITE_TOKEN_SECRET    = os.environ.get("NETSUITE_TOKEN_SECRET", "")

# =============================================================================
# FIELD NAMES (must match exactly as they appear in your Lark Base columns)
# =============================================================================
FIELD_ORDER_NUM       = "Order #"
FIELD_ORDER_DATE      = "Order Date"
FIELD_DUE_DATE        = "Due Date"
FIELD_STATUS          = "Status"
FIELD_DESCRIPTION     = "Description"
FIELD_ADDRESS         = "Address"
FIELD_QTY_ORDERED     = "Quantity Ordered"
FIELD_PRODUCTION_DRAWING = "Production Drawing"
ARTWORK_CONFIRMED_STATUS = "Artwork Confirmed"

# =============================================================================
# BOT SETTINGS
# =============================================================================
# Status that means a project is fully done — skip in due date warnings
DONE_STATUS = "Shipped"

# Warning thresholds in days before due date
WARNING_DAYS   = [21, 14, 7]   # 3 weeks, 2 weeks, 1 week
WARNING_LABELS = {
        21: "3 weeks",
        14: "2 weeks",
        7:  "1 week",
}

# =============================================================================
# CHAT ROUTING FOR DUE DATE WARNINGS
# Tables whose name contains a keyword get routed to that channel.
# Any table not matched goes to MASTER PRODUCTION (you + Carlo).
# NOTIFICATION_CHATS = channels that receive scheduled due date warnings.
# =============================================================================
CHAT_ROUTING = {
        "hannah": LARK_CHAT_ID_HANNAH,
        "lucy":   LARK_CHAT_ID_LUCY,
        "chen":   LARK_CHAT_ID_CHEN,
}

# Master production channel — receives warnings for unmatched tables
# and is always CC'd alongside the routed channel
NOTIFICATION_MASTER_CHAT = LARK_CHAT_ID_MASTER

# Channels that receive due date warning notifications
# Hannah tables  -> PRODUCTION (HANNAH)
# Lucy tables    -> PRODUCTION (LUCY)
# Chen tables    -> PRODUCTION (CHEN)
# All others     -> MASTER PRODUCTION
# MASTER PRODUCTION always gets a copy of every warning
NOTIFICATION_CHATS = [
        LARK_CHAT_ID_HANNAH,
        LARK_CHAT_ID_LUCY,
        LARK_CHAT_ID_CHEN,
        LARK_CHAT_ID_MASTER,
]

# =============================================================================
# LARK WEBHOOK VERIFICATION
# =============================================================================
LARK_VERIFICATION_TOKEN = os.environ.get("LARK_VERIFICATION_TOKEN", "")
LARK_ENCRYPT_KEY = os.environ.get("LARK_ENCRYPT_KEY", "")

# =============================================================================
# CARD CALLBACK VERIFICATION (for interactive message card action buttons)
# Set this in Lark Developer Console -> Features -> Bot -> Card callback URL
# =============================================================================
CARD_CALLBACK_VERIFICATION_TOKEN = os.environ.get("CARD_CALLBACK_VERIFICATION_TOKEN", "")

# =============================================================================
# CALENDAR (for morning digest / scheduling features)
# =============================================================================
LARK_PRIMARY_CALENDAR_ID = os.environ.get("LARK_PRIMARY_CALENDAR_ID", "")

# =============================================================================
# DIGEST CHANNEL
# =============================================================================
LARK_CHAT_ID_DIGEST = os.environ.get("LARK_CHAT_ID_DIGEST", "")
DIGEST_SECRET = os.environ.get("DIGEST_SECRET", "")
