"""
Lark API Client for HLT Production Bot

Handles authentication, auto-discovering all tables in a Lark Base,
reading records, and sending group chat notifications.
"""

import json
import logging
import time

import requests

from config import (
    LARK_APP_ID,
    LARK_APP_SECRET,
    LARK_BASE_URL,
    LARK_BASE_APP_TOKEN,
    FIELD_ORDER_NUM,
    FIELD_ORDER_DATE,
    FIELD_DUE_DATE,
    FIELD_STATUS,
    FIELD_DESCRIPTION,
    FIELD_ADDRESS,
    FIELD_QTY_ORDERED,
    DONE_STATUS,
)

logger = logging.getLogger(__name__)


class LarkClient:
    """Client for Lark Suite API (Base + Messaging)."""

    def __init__(self):
        self.base_url = LARK_BASE_URL.rstrip("/")
        self.token = None
        self.token_expires = 0

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------

    def _get_tenant_token(self) -> str:
        if self.token and time.time() < self.token_expires:
            return self.token
        resp = requests.post(
            f"{self.base_url}/open-apis/auth/v3/tenant_access_token/internal",
            json={
                "app_id": LARK_APP_ID,
                "app_secret": LARK_APP_SECRET,
            }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Lark auth failed: {data}")
        self.token = data["tenant_access_token"]
        self.token_expires = time.time() + data.get("expire", 7200) - 300
        logger.info("Lark tenant token acquired")
        return self.token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_tenant_token()}",
            "Content-Type": "application/json",
        }

    # -------------------------------------------------------------------------
    # Auto-discover all tables in the Base
    # -------------------------------------------------------------------------

    def get_all_tables(self, app_token: str = None) -> list:
        """Return all tables in the given Lark Base."""
        token = app_token or LARK_BASE_APP_TOKEN
        url = f"{self.base_url}/open-apis/bitable/v1/apps/{token}/tables"
        tables = []
        page_token = None
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise Exception(f"Failed to list tables: {data}")
            items = data.get("data", {}).get("items", [])
            tables.extend([
                {"table_id": t["table_id"], "name": t.get("name", "")}
                for t in items
            ])
            page_token = data.get("data", {}).get("page_token")
            if not data.get("data", {}).get("has_more"):
                break
        return tables

    # -------------------------------------------------------------------------
    # Record retrieval
    # -------------------------------------------------------------------------

    def get_all_records(self, app_token: str, table_id: str) -> list:
        """Retrieve all records from a table, handling pagination."""
        url = (f"{self.base_url}/open-apis/bitable/v1/apps/"
               f"{app_token}/tables/{table_id}/records")
        records = []
        page_token = None
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise Exception(f"Failed to list records: {data}")
            records.extend(data.get("data", {}).get("items", []))
            page_token = data.get("data", {}).get("page_token")
            if not data.get("data", {}).get("has_more"):
                break
        return records

    def get_table_records(self, table_id: str, app_token: str = None) -> list:
        token = app_token or LARK_BASE_APP_TOKEN
        return self.get_all_records(token, table_id)

    def parse_record(self, record: dict) -> dict:
        """Extract relevant fields from a raw Lark Base record."""
        fields = record.get("fields", {})

        def get_text(field_name: str) -> str:
            val = fields.get(field_name, "")
            if isinstance(val, list):
                return " ".join(
                    part.get("text", "") if isinstance(part, dict) else str(part)
                    for part in val
                ).strip()
            return str(val).strip() if val else ""

        def get_date_ms(field_name: str):
            val = fields.get(field_name)
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return int(val)
            return None

        def get_status(field_name: str) -> str:
            val = fields.get(field_name, "")
            if isinstance(val, list):
                return " ".join(
                    item.get("text", str(item)) if isinstance(item, dict) else str(item)
                    for item in val
                ).strip()
            return str(val).strip() if val else ""

        return {
            "record_id": record.get("record_id", ""),
            "order_num": get_text(FIELD_ORDER_NUM),
            "order_date_ms": get_date_ms(FIELD_ORDER_DATE),
            "due_date_ms": get_date_ms(FIELD_DUE_DATE),
            "status": get_status(FIELD_STATUS),
            "description": get_text(FIELD_DESCRIPTION),
            "address": get_text(FIELD_ADDRESS),
            "qty_ordered": get_text(FIELD_QTY_ORDERED),
        }

    # -------------------------------------------------------------------------
    # Messaging
    # -------------------------------------------------------------------------

    def send_response(self, message: str, chat_id: str = None):
        """Send a response message to the chat where the question came from."""
        return self.send_group_message(message, chat_id=chat_id)

    def send_group_message(self, message: str, chat_id: str = None):
        """Send an interactive card message to a Lark group chat."""
        if not chat_id:
            logger.warning("No chat_id provided, skipping message")
            return
        url = f"{self.base_url}/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        body = {
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": self._build_card(message),
        }
        resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to send message: {data}")
        logger.info(f"Message sent to chat {chat_id}")

    def _build_card(self, text_content: str) -> str:
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "\ud83e\udd16 IRON BOT"},
                "template": "blue",
            },
            "elements": [{"tag": "markdown", "content": text_content}],
        }
        return json.dumps(card)

    def send_alert_card(self, message: str, chat_id: str = None):
        """Send an alert card (red header) to a Lark group chat."""
        if not chat_id:
            logger.warning("No chat_id provided, skipping alert")
            return
        url = f"{self.base_url}/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        body = {
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": self._build_alert_card(message),
        }
        resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to send alert: {data}")
        logger.info(f"Alert sent to chat {chat_id}")

    def _build_alert_card(self, text_content: str) -> str:
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Shipment Alert"},
                "template": "red",
            },
            "elements": [{"tag": "markdown", "content": text_content}],
        }
        return json.dumps(card)

    # -------------------------------------------------------------------------
    # Artwork Approval Methods
    # -------------------------------------------------------------------------

    def find_record_by_order_num(self, order_num: str, app_token: str = None) -> dict:
        """Search all tables for a record matching the given order number."""
        token = app_token or LARK_BASE_APP_TOKEN
        tables = self.get_all_tables(token)
        order_num_clean = order_num.strip().upper()
        for table in tables:
            tid = table["table_id"]
            tname = table.get("name", "")
            try:
                records = self.get_table_records(tid, token)
                for rec in records:
                    fields = rec.get("fields", {})
                    # Check Order # field
                    raw = fields.get("Order #", "")
                    val = ""
                    if isinstance(raw, list):
                        val = " ".join(p.get("text", str(p)) if isinstance(p, dict) else str(p) for p in raw).strip()
                    elif isinstance(raw, (str, int, float)):
                        val = str(raw).strip()
                    if val.upper() == order_num_clean:
                        logger.info(f"Found order {order_num} in table {tname}, record {rec.get('record_id')}")
                        return {"table_id": tid, "table_name": tname, "record_id": rec.get("record_id"), "fields": fields}
            except Exception as e:
                logger.error(f"Error searching table {tname}: {e}")
        return {}

    def update_record_fields(self, table_id: str, record_id: str, fields: dict, app_token: str = None) -> bool:
        """Update specific fields on a Lark Base record."""
        token = app_token or LARK_BASE_APP_TOKEN
        url = (f"{self.base_url}/open-apis/bitable/v1/apps/"
               f"{token}/tables/{table_id}/records/{record_id}")
        body = {"fields": fields}
        resp = requests.put(url, headers=self._headers(), json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to update record: {data}")
        logger.info(f"Updated record {record_id} in table {table_id}")
        return True

    def get_recent_file_from_chat(self, chat_id: str, limit: int = 20) -> dict:
        """Fetch the most recent file/image message from a chat."""
        url = f"{self.base_url}/open-apis/im/v1/messages"
        params = {
            "container_id_type": "chat",
            "container_id": chat_id,
            "page_size": limit,
            "sort_type": "ByCreateTimeDesc",
        }
        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to fetch messages: {data}")
        messages = data.get("data", {}).get("items", [])
        for msg in messages:
            msg_type = msg.get("msg_type", "")
            if msg_type in ("file", "image"):
                try:
                    content = json.loads(msg.get("body", {}).get("content", "{}"))
                    return {
                        "message_id": msg.get("message_id"),
                        "msg_type": msg_type,
                        "file_key": content.get("file_key") or content.get("image_key"),
                        "file_name": content.get("file_name", f"artwork.{msg_type}"),
                    }
                except Exception as e:
                    logger.error(f"Error parsing file message: {e}")
        return {}

    def download_file_from_message(self, message_id: str, file_key: str) -> bytes:
        """Download a file from a Lark message."""
        url = f"{self.base_url}/open-apis/im/v1/messages/{message_id}/resources/{file_key}"
        params = {"type": "file"}
        resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        resp.raise_for_status()
        return resp.content

    def upload_file_to_record(self, table_id: str, record_id: str, field_name: str,
                               file_bytes: bytes, file_name: str, app_token: str = None) -> bool:
        """Upload a file attachment to a specific field on a Lark Base record."""
        token = app_token or LARK_BASE_APP_TOKEN
        # Step 1: Upload file to Lark drive to get a file token
        upload_url = f"{self.base_url}/open-apis/drive/v1/medias/upload_all"
        headers = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        files = {
            "file": (file_name, file_bytes, "application/octet-stream"),
            "file_name": (None, file_name),
            "parent_type": (None, "bitable_file"),
            "parent_node": (None, token),
            "size": (None, str(len(file_bytes))),
        }
        resp = requests.post(upload_url, headers=headers, files=files, timeout=60)
        resp.raise_for_status()
        upload_data = resp.json()
        if upload_data.get("code") != 0:
            raise Exception(f"File upload failed: {upload_data}")
        file_token = upload_data.get("data", {}).get("file_token")
        logger.info(f"File uploaded, token: {file_token}")
        # Step 2: Attach file token to the record field
        return self.update_record_fields(table_id, record_id, {
            field_name: [{"file_token": file_token, "name": file_name}]
        }, app_token)

    def update_record_status(self, record: dict, new_status: str, field_name: str = None, app_token: str = None) -> bool:
        """Update the status field on a Lark Base record. Called by bot_server for artwork approval."""
        table_id = record.get("table_id", "")
        record_id = record.get("record_id", "")
        if not table_id or not record_id:
            raise Exception(f"update_record_status: missing table_id or record_id in record dict: {record}")
        status_field = field_name or "Status"
        return self.update_record_fields(table_id, record_id, {status_field: new_status}, app_token)

    # -------------------------------------------------------------------------
    # Record Comments
    # -------------------------------------------------------------------------
    def get_record_comments(self, table_id: str, record_id: str, app_token: str = None) -> list:
        """Fetch all comments on a Lark Bitable record.

        Uses:  GET /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}/comments
        Returns a list of comment dicts, each with keys:
            comment_id, user_name, user_open_id, content (plain text), create_time (ms)
        """
        token = app_token or LARK_BASE_APP_TOKEN
        url = (
            f"{self.base_url}/open-apis/bitable/v1/apps/"
            f"{token}/tables/{table_id}/records/{record_id}/comments"
        )
        comments = []
        page_token = None
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise Exception(f"Failed to fetch comments: {data}")
            items = data.get("data", {}).get("items", [])
            for item in items:
                body = item.get("body", {})
                text_parts = []
                for seg in body.get("content", []):
                    for run in seg.get("runs", []):
                        text_parts.append(run.get("text", ""))
                plain_text = "".join(text_parts).strip()
                user = item.get("user_info", {})
                comments.append({
                    "comment_id":   item.get("comment_id", ""),
                    "user_name":    user.get("name", ""),
                    "user_open_id": user.get("open_id", ""),
                    "content":      plain_text,
                    "create_time":  item.get("create_time", 0),
                })
            page_token = data.get("data", {}).get("page_token")
            if not data.get("data", {}).get("has_more"):
                break
        logger.info(f"Fetched {len(comments)} comments for record {record_id}")
        return comments

    def get_comments_for_order(self, order_num: str, app_token: str = None) -> list:
        """Find a record by order number and return its comments."""
        record = self.find_record_by_order_num(order_num, app_token)
        if not record:
            raise Exception(f"Order {order_num} not found in any table")
        return self.get_record_comments(
            table_id=record["table_id"],
            record_id=record["record_id"],
            app_token=app_token,
        )

