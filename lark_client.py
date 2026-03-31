"""
Lark API Client for HLT IRON BOT - Full Integration

Covers ALL Lark Open Platform Server APIs:
  1. Authentication
  2. Messaging (send, reply, edit, recall, forward, reactions, pins, buzz, cards)
  3. Group Chat (create, update, members, tabs, announcements)
  4. Base / Bitable (full CRUD: apps, tables, views, records, fields, forms)
  5. Docs (documents read/write)
  6. Sheets (read/write spreadsheets)
  7. Wiki (spaces, nodes, read/write)
  8. Calendar (calendars, events, attendees, meeting rooms)
  9. Tasks v2 (create, update, complete, lists)
  10. Approval (definitions, instances, tasks)
  11. Contacts (users, departments, groups)
  12. Drive (files, folders, upload/download)
  13. Search (cross-product search)
  14. Email (mailbox)
  15. Bot management
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
    """All-in-one client for every Lark Suite Server API."""

    def __init__(self):
        self.base_url = LARK_BASE_URL.rstrip("/")
        self.token = None
        self.token_expires = 0

    # =========================================================================
    # 1. AUTHENTICATION
    # =========================================================================
    def _get_tenant_token(self) -> str:
        if self.token and time.time() < self.token_expires:
            return self.token
        resp = requests.post(
            f"{self.base_url}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Lark auth failed: {data}")
        self.token = data["tenant_access_token"]
        self.token_expires = time.time() + data.get("expire", 7200) - 300
        logger.info("Lark tenant token acquired")
        return self.token

    def _headers(self, content_type="application/json") -> dict:
        h = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        if content_type:
            h["Content-Type"] = content_type
        return h

    def _get(self, path, params=None, timeout=30):
        resp = requests.get(f"{self.base_url}{path}", headers=self._headers(), params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path, body=None, params=None, timeout=30):
        resp = requests.post(f"{self.base_url}{path}", headers=self._headers(), json=body, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path, body=None, params=None, timeout=30):
        resp = requests.put(f"{self.base_url}{path}", headers=self._headers(), json=body, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path, body=None, params=None, timeout=30):
        resp = requests.patch(f"{self.base_url}{path}", headers=self._headers(), json=body, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path, body=None, params=None, timeout=30):
        resp = requests.delete(f"{self.base_url}{path}", headers=self._headers(), json=body, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path, params=None, items_key="items", data_key="data"):
        params = dict(params or {})
        params.setdefault("page_size", 100)
        all_items = []
        page_token = None
        while True:
            if page_token:
                params["page_token"] = page_token
            data = self._get(path, params)
            if data.get("code") != 0:
                raise Exception(f"API error on {path}: {data}")
            inner = data.get(data_key, {})
            items = inner.get(items_key, [])
            all_items.extend(items)
            page_token = inner.get("page_token")
            if not inner.get("has_more"):
                break
        return all_items

    # =========================================================================
    # 2. MESSAGING - Send, Reply, Edit, Recall, Forward, Reactions, Pins, Buzz
    # =========================================================================

    def send_message(self, receive_id, msg_type, content, receive_id_type="chat_id", uuid=None):
        body = {"receive_id": receive_id, "msg_type": msg_type, "content": content}
        if uuid:
            body["uuid"] = uuid
        data = self._post("/open-apis/im/v1/messages", body=body, params={"receive_id_type": receive_id_type})
        if data.get("code") != 0:
            raise Exception(f"Send message failed: {data}")
        return data.get("data", {})

    def send_text(self, text, chat_id=None, user_id=None):
        rid = chat_id or user_id
        rid_type = "chat_id" if chat_id else "open_id"
        return self.send_message(rid, "text", json.dumps({"text": text}), rid_type)

    def send_rich_text(self, title, content_blocks, chat_id=None, user_id=None):
        rid = chat_id or user_id
        rid_type = "chat_id" if chat_id else "open_id"
        post = {"en_us": {"title": title, "content": content_blocks}}
        return self.send_message(rid, "post", json.dumps(post), rid_type)

    def send_image_msg(self, image_key, chat_id=None, user_id=None):
        rid = chat_id or user_id
        rid_type = "chat_id" if chat_id else "open_id"
        return self.send_message(rid, "image", json.dumps({"image_key": image_key}), rid_type)

    def send_file_msg(self, file_key, chat_id=None, user_id=None):
        rid = chat_id or user_id
        rid_type = "chat_id" if chat_id else "open_id"
        return self.send_message(rid, "file", json.dumps({"file_key": file_key}), rid_type)

    def send_share_chat(self, share_chat_id, chat_id):
        return self.send_message(chat_id, "share_chat", json.dumps({"chat_id": share_chat_id}))

    def send_card(self, card, chat_id=None, user_id=None):
        rid = chat_id or user_id
        rid_type = "chat_id" if chat_id else "open_id"
        return self.send_message(rid, "interactive", json.dumps(card) if isinstance(card, dict) else card, rid_type)

    def send_response(self, message, chat_id=None):
        return self.send_group_message(message, chat_id=chat_id)

    def send_group_message(self, message, chat_id=None):
        if not chat_id:
            logger.warning("No chat_id provided, skipping message")
            return
        return self.send_message(chat_id, "interactive", self._build_card(message))

    def _build_card(self, text_content):
        card = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": "\U0001f916 IRON BOT"}, "template": "blue"},
            "elements": [{"tag": "markdown", "content": text_content}],
        }
        return json.dumps(card)

    def send_alert_card(self, message, chat_id=None):
        if not chat_id:
            return
        card = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": "Shipment Alert"}, "template": "red"},
            "elements": [{"tag": "markdown", "content": message}],
        }
        return self.send_message(chat_id, "interactive", json.dumps(card))

    def send_action_card(self, title, content, actions, chat_id=None, template="blue"):
        button_elements = []
        for action in actions:
            button_elements.append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": action["text"]},
                "type": action.get("type", "default"),
                "value": {"action": action["value"]},
            })
        card = {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": template},
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "action", "actions": button_elements},
            ],
        }
        return self.send_card(card, chat_id=chat_id)

    def reply_message(self, message_id, msg_type, content):
        body = {"msg_type": msg_type, "content": content}
        data = self._post(f"/open-apis/im/v1/messages/{message_id}/reply", body=body)
        if data.get("code") != 0:
            raise Exception(f"Reply failed: {data}")
        return data.get("data", {})

    def reply_text(self, message_id, text):
        return self.reply_message(message_id, "text", json.dumps({"text": text}))

    def reply_card(self, message_id, card):
        content = json.dumps(card) if isinstance(card, dict) else card
        return self.reply_message(message_id, "interactive", content)

    def edit_message(self, message_id, msg_type, content):
        body = {"msg_type": msg_type, "content": content}
        data = self._put(f"/open-apis/im/v1/messages/{message_id}", body=body)
        if data.get("code") != 0:
            raise Exception(f"Edit failed: {data}")
        return data.get("data", {})

    def recall_message(self, message_id):
        data = self._delete(f"/open-apis/im/v1/messages/{message_id}")
        return data

    def forward_message(self, message_id, receive_id, receive_id_type="chat_id"):
        body = {"receive_id": receive_id}
        data = self._post(f"/open-apis/im/v1/messages/{message_id}/forward",
                          body=body, params={"receive_id_type": receive_id_type})
        if data.get("code") != 0:
            raise Exception(f"Forward failed: {data}")
        return data.get("data", {})

    def merge_forward_messages(self, message_ids, receive_id, receive_id_type="chat_id"):
        body = {"receive_id": receive_id, "message_id_list": message_ids}
        data = self._post("/open-apis/im/v1/messages/merge_forward",
                          body=body, params={"receive_id_type": receive_id_type})
        return data.get("data", {})

    def add_reaction(self, message_id, emoji_type):
        body = {"reaction_type": {"emoji_type": emoji_type}}
        data = self._post(f"/open-apis/im/v1/messages/{message_id}/reactions", body=body)
        return data.get("data", {})

    def get_reactions(self, message_id):
        return self._paginate(f"/open-apis/im/v1/messages/{message_id}/reactions")

    def delete_reaction(self, message_id, reaction_id):
        return self._delete(f"/open-apis/im/v1/messages/{message_id}/reactions/{reaction_id}")

    def pin_message(self, message_id):
        data = self._post("/open-apis/im/v1/pins", body={"message_id": message_id})
        return data.get("data", {})

    def unpin_message(self, message_id):
        return self._delete(f"/open-apis/im/v1/pins/{message_id}")

    def get_pinned_messages(self, chat_id):
        return self._paginate("/open-apis/im/v1/pins", params={"chat_id": chat_id})

    def buzz_message(self, message_id, user_ids):
        body = {"user_id_list": user_ids}
        data = self._patch(f"/open-apis/im/v1/messages/{message_id}/urgent_app",
                           body=body, params={"user_id_type": "open_id"})
        return data

    def get_read_users(self, message_id):
        return self._paginate(f"/open-apis/im/v1/messages/{message_id}/read_users",
                              params={"user_id_type": "open_id"})

    def get_chat_history(self, container_id, start_time=None, end_time=None, limit=50):
        params = {"container_id_type": "chat", "container_id": container_id, "page_size": min(limit, 50)}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._paginate("/open-apis/im/v1/messages", params=params)

    def get_message(self, message_id):
        data = self._get(f"/open-apis/im/v1/messages/{message_id}")
        return data.get("data", {})

    def upload_image(self, image_bytes, image_type="message"):
        headers = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        files = {"image": ("image.png", image_bytes, "image/png")}
        resp = requests.post(f"{self.base_url}/open-apis/im/v1/images",
                             headers=headers, files=files, data={"image_type": image_type}, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise Exception(f"Upload image failed: {result}")
        return result["data"]["image_key"]

    def upload_file_for_msg(self, file_bytes, file_name, file_type="stream"):
        headers = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        files = {"file": (file_name, file_bytes, "application/octet-stream")}
        resp = requests.post(f"{self.base_url}/open-apis/im/v1/files",
                             headers=headers, files=files,
                             data={"file_type": file_type, "file_name": file_name}, timeout=120)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise Exception(f"Upload file failed: {result}")
        return result["data"]["file_key"]

    def get_message_resource(self, message_id, file_key, resource_type="file"):
        resp = requests.get(
            f"{self.base_url}/open-apis/im/v1/messages/{message_id}/resources/{file_key}",
            headers=self._headers(None), params={"type": resource_type}, timeout=60)
        resp.raise_for_status()
        return resp.content

    def get_recent_file_from_chat(self, chat_id, limit=20):
        messages = self.get_chat_history(chat_id, limit=limit)
        for msg in messages:
            if msg.get("msg_type") in ("file", "image"):
                try:
                    content = json.loads(msg.get("body", {}).get("content", "{}"))
                    return {"message_id": msg.get("message_id"), "msg_type": msg["msg_type"],
                            "file_key": content.get("file_key") or content.get("image_key"),
                            "file_name": content.get("file_name", "file")}
                except Exception as e:
                    logger.error(f"Error parsing file message: {e}")
        return {}

    def download_file_from_message(self, message_id, file_key):
        return self.get_message_resource(message_id, file_key, "file")

    # =========================================================================
    # 3. GROUP CHAT - Create, Update, Members, Tabs, Announcements
    # =========================================================================

    def create_chat(self, name, description="", user_ids=None):
        body = {"name": name, "description": description}
        if user_ids:
            body["user_id_list"] = user_ids
        data = self._post("/open-apis/im/v1/chats", body=body, params={"user_id_type": "open_id"})
        if data.get("code") != 0:
            raise Exception(f"Create chat failed: {data}")
        logger.info(f"Created chat: {data.get('data', {}).get('chat_id', '')}")
        return data.get("data", {})

    def update_chat(self, chat_id, name=None, description=None):
        body = {}
        if name:
            body["name"] = name
        if description:
            body["description"] = description
        data = self._put(f"/open-apis/im/v1/chats/{chat_id}", body=body)
        return data.get("data", {})

    def delete_chat(self, chat_id):
        return self._delete(f"/open-apis/im/v1/chats/{chat_id}")

    def get_chat_info(self, chat_id):
        data = self._get(f"/open-apis/im/v1/chats/{chat_id}")
        return data.get("data", {})

    def list_chats(self, limit=100):
        return self._paginate("/open-apis/im/v1/chats", params={"page_size": min(limit, 100)})

    def search_chats(self, query):
        return self._paginate("/open-apis/im/v1/chats/search", params={"query": query})

    def add_chat_members(self, chat_id, user_ids):
        body = {"id_list": user_ids}
        data = self._post(f"/open-apis/im/v1/chats/{chat_id}/members",
                          body=body, params={"member_id_type": "open_id"})
        return data.get("data", {})

    def remove_chat_members(self, chat_id, user_ids):
        body = {"id_list": user_ids}
        resp = requests.delete(f"{self.base_url}/open-apis/im/v1/chats/{chat_id}/members",
                               headers=self._headers(), json=body, params={"member_id_type": "open_id"}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_chat_members(self, chat_id):
        return self._paginate(f"/open-apis/im/v1/chats/{chat_id}/members",
                              params={"member_id_type": "open_id"})

    def is_chat_member(self, chat_id, user_id):
        members = self.get_chat_members(chat_id)
        return any(m.get("member_id") == user_id for m in members)

    def create_chat_tab(self, chat_id, tab_name, tab_type, tab_content):
        body = {"chat_tabs": [{"tab_name": tab_name, "tab_type": tab_type, "tab_content": tab_content}]}
        data = self._post(f"/open-apis/im/v1/chats/{chat_id}/chat_tabs", body=body)
        return data.get("data", {})

    def list_chat_tabs(self, chat_id):
        data = self._get(f"/open-apis/im/v1/chats/{chat_id}/chat_tabs")
        return data.get("data", {}).get("chat_tabs", [])

    def delete_chat_tab(self, chat_id, tab_ids):
        body = {"tab_ids": tab_ids}
        return self._delete(f"/open-apis/im/v1/chats/{chat_id}/chat_tabs", body=body)

    def set_chat_top_notice(self, chat_id, message_id):
        body = {"chat_top_notice": [{"action_type": "1", "message_id": message_id}]}
        data = self._put(f"/open-apis/im/v1/chats/{chat_id}/top_notice", body=body)
        return data

    def delete_chat_top_notice(self, chat_id):
        return self._delete(f"/open-apis/im/v1/chats/{chat_id}/top_notice")

    # =========================================================================
    # 4. BASE / BITABLE - Full CRUD
    # =========================================================================

    def get_all_tables(self, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._paginate(f"/open-apis/bitable/v1/apps/{token}/tables")

    def create_table(self, name, fields=None, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"table": {"name": name}}
        if fields:
            body["table"]["fields"] = fields
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create table failed: {data}")
        return data.get("data", {})

    def delete_table(self, table_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._delete(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}")

    def get_all_records(self, app_token, table_id, view_id=None):
                params = {"view_id": view_id} if view_id else None
              return self._paginate(f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records", params=params)

        def get_table_records(self, table_id, app_token=None, view_id=None):
        token = app_token or LARK_BASE_APP_TOKEN
                return self.get_all_records(token, table_id, view_id=view_id)

    def search_records(self, table_id, filter_expr=None, sort=None, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {}
        if filter_expr:
            body["filter"] = filter_expr
        if sort:
            body["sort"] = sort
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/search", body=body)
        if data.get("code") != 0:
            raise Exception(f"Search records failed: {data}")
        return data.get("data", {}).get("items", [])

    def get_record(self, table_id, record_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        data = self._get(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/{record_id}")
        return data.get("data", {}).get("record", {})

    def create_record(self, table_id, fields, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"fields": fields}
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create record failed: {data}")
        logger.info(f"Created record in table {table_id}")
        return data.get("data", {}).get("record", {})

    def batch_create_records(self, table_id, records_fields, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"records": [{"fields": r} for r in records_fields]}
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/batch_create", body=body)
        return data.get("data", {})

    def update_record_fields(self, table_id, record_id, fields, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"fields": fields}
        data = self._put(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/{record_id}", body=body)
        if data.get("code") != 0:
            raise Exception(f"Update record failed: {data}")
        logger.info(f"Updated record {record_id}")
        return True

    def batch_update_records(self, table_id, records, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"records": records}
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/batch_update", body=body)
        return data.get("data", {})

    def delete_record(self, table_id, record_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._delete(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/{record_id}")

    def batch_delete_records(self, table_id, record_ids, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"records": record_ids}
        return self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/batch_delete", body=body)

    # --- Fields ---
    def list_fields(self, table_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._paginate(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/fields")

    def create_field(self, table_id, field_name, field_type, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"field_name": field_name, "type": field_type}
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/fields", body=body)
        return data.get("data", {})

    def update_field(self, table_id, field_id, field_name=None, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {}
        if field_name:
            body["field_name"] = field_name
        data = self._put(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/fields/{field_id}", body=body)
        return data.get("data", {})

    def delete_field(self, table_id, field_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._delete(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/fields/{field_id}")

    # --- Views ---
    def list_views(self, table_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._paginate(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/views")

    def create_view(self, table_id, view_name, view_type="grid", app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"view_name": view_name, "view_type": view_type}
        data = self._post(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/views", body=body)
        return data.get("data", {})

    def delete_view(self, table_id, view_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._delete(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/views/{view_id}")

    # --- Forms ---
    def get_form_meta(self, table_id, form_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        data = self._get(f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/forms/{form_id}")
        return data.get("data", {})

    # --- Dashboards ---
    def list_dashboards(self, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        return self._paginate(f"/open-apis/bitable/v1/apps/{token}/dashboards")

    # --- Record Comments ---
    def get_record_comments(self, table_id, record_id, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        url = f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/{record_id}/comments"
        comments = []
        page_token = None
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            data = self._get(url, params)
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
                    "comment_id": item.get("comment_id", ""),
                    "user_name": user.get("name", ""),
                    "user_open_id": user.get("open_id", ""),
                    "content": plain_text,
                    "create_time": item.get("create_time", 0),
                })
            page_token = data.get("data", {}).get("page_token")
            if not data.get("data", {}).get("has_more"):
                break
        return comments

    def create_record_comment(self, table_id, record_id, text, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        body = {"body": {"content": [{"runs": [{"text": text}]}]}}
        data = self._post(
            f"/open-apis/bitable/v1/apps/{token}/tables/{table_id}/records/{record_id}/comments",
            body=body)
        return data.get("data", {})

    def get_comments_for_order(self, order_num, app_token=None):
        record = self.find_record_by_order_num(order_num, app_token)
        if not record:
            raise Exception(f"Order {order_num} not found in any table")
        return self.get_record_comments(
            table_id=record["table_id"], record_id=record["record_id"], app_token=app_token)

    # --- Legacy helpers for bot_server compatibility ---
    def parse_record(self, record):
        fields = record.get("fields", {})
        def get_text(fn):
            val = fields.get(fn, "")
            if isinstance(val, list):
                return " ".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in val).strip()
            return str(val).strip() if val else ""
        def get_date_ms(fn):
            val = fields.get(fn)
            if isinstance(val, (int, float)):
                return int(val)
            return None
        def get_status(fn):
            val = fields.get(fn, "")
            if isinstance(val, list):
                return " ".join(item.get("text", str(item)) if isinstance(item, dict) else str(item) for item in val).strip()
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

    def find_record_by_order_num(self, order_num, app_token=None):
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
                    raw = fields.get("Order #", "")
                    val = ""
                    if isinstance(raw, list):
                        val = " ".join(p.get("text", str(p)) if isinstance(p, dict) else str(p) for p in raw).strip()
                    elif isinstance(raw, (str, int, float)):
                        val = str(raw).strip()
                    if val.upper() == order_num_clean:
                        return {"table_id": tid, "table_name": tname,
                                "record_id": rec.get("record_id"), "fields": fields,
                                "__table_name__": tname}
            except Exception as e:
                logger.error(f"Error searching table {tname}: {e}")
        return {}

    def update_record_status(self, record, new_status, field_name=None, app_token=None):
        table_id = record.get("table_id", "")
        record_id = record.get("record_id", "")
        if not table_id or not record_id:
            raise Exception(f"Missing table_id or record_id: {record}")
        status_field = field_name or "Status"
        return self.update_record_fields(table_id, record_id, {status_field: new_status}, app_token)

    def upload_file_to_record(self, table_id, record_id, field_name, file_bytes, file_name, app_token=None):
        token = app_token or LARK_BASE_APP_TOKEN
        headers = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        files = {"file": (file_name, file_bytes, "application/octet-stream"),
                 "file_name": (None, file_name),
                 "parent_type": (None, "bitable_file"),
                 "parent_node": (None, token),
                 "size": (None, str(len(file_bytes)))}
        resp = requests.post(f"{self.base_url}/open-apis/drive/v1/medias/upload_all",
                             headers=headers, files=files, timeout=60)
        resp.raise_for_status()
        upload_data = resp.json()
        if upload_data.get("code") != 0:
            raise Exception(f"File upload failed: {upload_data}")
        file_token = upload_data.get("data", {}).get("file_token")
        return self.update_record_fields(table_id, record_id,
                                         {field_name: [{"file_token": file_token, "name": file_name}]}, app_token)

    # =========================================================================
    # 5. DOCS - Read/Write Documents
    # =========================================================================

    def create_document(self, title, folder_token=None):
        body = {"title": title}
        if folder_token:
            body["folder_token"] = folder_token
        data = self._post("/open-apis/docx/v1/documents", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create doc failed: {data}")
        return data.get("data", {}).get("document", {})

    def get_document_content(self, document_id):
        data = self._get(f"/open-apis/docx/v1/documents/{document_id}/raw_content")
        if data.get("code") != 0:
            raise Exception(f"Get doc content failed: {data}")
        return data.get("data", {}).get("content", "")

    def get_document_blocks(self, document_id):
        return self._paginate(f"/open-apis/docx/v1/documents/{document_id}/blocks", items_key="items")

    def create_document_block(self, document_id, block_type, body_content, index=-1):
        body = {"children": [{"block_type": block_type, "body": body_content}], "index": index}
        data = self._post(f"/open-apis/docx/v1/documents/{document_id}/blocks/batch_create", body=body)
        return data.get("data", {})

    def update_document_block(self, document_id, block_id, body_content):
        body = {"update_text_elements": {"elements": body_content}}
        data = self._patch(f"/open-apis/docx/v1/documents/{document_id}/blocks/{block_id}", body=body)
        return data.get("data", {})

    def delete_document_block(self, document_id, block_id, start_index=0, end_index=1):
        body = {"start_index": start_index, "end_index": end_index}
        data = self._delete(f"/open-apis/docx/v1/documents/{document_id}/blocks/batch_delete", body=body)
        return data

    # =========================================================================
    # 6. SHEETS - Read/Write Spreadsheets
    # =========================================================================

    def create_spreadsheet(self, title, folder_token=None):
        body = {"title": title}
        if folder_token:
            body["folder_token"] = folder_token
        data = self._post("/open-apis/sheets/v3/spreadsheets", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create spreadsheet failed: {data}")
        return data.get("data", {}).get("spreadsheet", {})

    def get_spreadsheet_info(self, spreadsheet_token):
        data = self._get(f"/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}")
        return data.get("data", {}).get("spreadsheet", {})

    def get_sheet_list(self, spreadsheet_token):
        data = self._get(f"/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query")
        return data.get("data", {}).get("sheets", [])

    def read_sheet_range(self, spreadsheet_token, sheet_range):
        data = self._get(f"/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_range}")
        return data.get("data", {}).get("valueRange", {})

    def write_sheet_range(self, spreadsheet_token, sheet_range, values):
        body = {"valueRange": {"range": sheet_range, "values": values}}
        data = self._put(f"/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values",
                         body=body)
        return data.get("data", {})

    def append_sheet_rows(self, spreadsheet_token, sheet_range, values):
        body = {"valueRange": {"range": sheet_range, "values": values}}
        data = self._post(f"/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append",
                          body=body)
        return data.get("data", {})

    # =========================================================================
    # 7. WIKI - Spaces, Nodes, Read/Write
    # =========================================================================

    def list_wiki_spaces(self):
        return self._paginate("/open-apis/wiki/v2/spaces")

    def get_wiki_space(self, space_id):
        data = self._get(f"/open-apis/wiki/v2/spaces/{space_id}")
        return data.get("data", {}).get("space", {})

    def list_wiki_nodes(self, space_id, parent_node_token=None):
        params = {"page_size": 50}
        if parent_node_token:
            params["parent_node_token"] = parent_node_token
        return self._paginate(f"/open-apis/wiki/v2/spaces/{space_id}/nodes", params=params)

    def get_wiki_node(self, space_id, node_token):
        data = self._get(f"/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}")
        return data.get("data", {}).get("node", {})

    def create_wiki_node(self, space_id, title, parent_node_token=None, node_type="doc"):
        body = {"title": title, "node_type": node_type}
        if parent_node_token:
            body["parent_node_token"] = parent_node_token
        data = self._post(f"/open-apis/wiki/v2/spaces/{space_id}/nodes", body=body)
        return data.get("data", {}).get("node", {})

    def move_wiki_node(self, space_id, node_token, target_parent_token):
        body = {"target_parent_token": target_parent_token}
        data = self._post(f"/open-apis/wiki/v2/spaces/{space_id}/nodes/{node_token}/move", body=body)
        return data.get("data", {})

    def get_wiki_node_content(self, node_token):
        data = self._get(f"/open-apis/docx/v1/documents/{node_token}/raw_content")
        if data.get("code") == 0:
            return data.get("data", {}).get("content", "")
        return ""

    def fetch_all_wiki_pages(self):
        all_pages = []
        try:
            spaces = self.list_wiki_spaces()
            for space in spaces:
                space_id = space.get("space_id", "")
                space_name = space.get("name", "")
                try:
                    nodes = self.list_wiki_nodes(space_id)
                    for node in nodes:
                        node_token = node.get("node_token", "")
                        node_title = node.get("title", "")
                        try:
                            content = self.get_wiki_node_content(node_token)
                            all_pages.append({
                                "space": space_name, "title": node_title,
                                "content": content[:3000]
                            })
                        except Exception as e:
                            logger.warning(f"Wiki page fetch error ({node_title}): {e}")
                except Exception as e:
                    logger.warning(f"Wiki space fetch error ({space_name}): {e}")
        except Exception as e:
            logger.error(f"Wiki fetch error: {e}")
        return all_pages

    # =========================================================================
    # 8. CALENDAR - Calendars, Events, Attendees, Meeting Rooms
    # =========================================================================

    def list_calendars(self):
        return self._paginate("/open-apis/calendar/v4/calendars", items_key="calendar_list")

    def get_primary_calendar(self):
        data = self._get("/open-apis/calendar/v4/calendars/primary")
        return data.get("data", {}).get("calendars", [])

    def create_calendar(self, summary, description=""):
        body = {"summary": summary, "description": description}
        data = self._post("/open-apis/calendar/v4/calendars", body=body)
        return data.get("data", {}).get("calendar", {})

    def get_calendar(self, calendar_id):
        data = self._get(f"/open-apis/calendar/v4/calendars/{calendar_id}")
        return data.get("data", {}).get("calendar", {})

    def delete_calendar(self, calendar_id):
        return self._delete(f"/open-apis/calendar/v4/calendars/{calendar_id}")

    def list_events(self, calendar_id, start_time=None, end_time=None):
        params = {"page_size": 50}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._paginate(f"/open-apis/calendar/v4/calendars/{calendar_id}/events", params=params)

    def create_event(self, calendar_id, summary, start_time, end_time,
                     description="", attendees=None, location=None):
        body = {
            "summary": summary,
            "description": description,
            "start_time": start_time,
            "end_time": end_time,
        }
        if attendees:
            body["attendees"] = attendees
        if location:
            body["location"] = {"name": location}
        data = self._post(f"/open-apis/calendar/v4/calendars/{calendar_id}/events", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create event failed: {data}")
        return data.get("data", {}).get("event", {})

    def get_event(self, calendar_id, event_id):
        data = self._get(f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}")
        return data.get("data", {}).get("event", {})

    def update_event(self, calendar_id, event_id, updates):
        data = self._patch(f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}", body=updates)
        return data.get("data", {}).get("event", {})

    def delete_event(self, calendar_id, event_id):
        return self._delete(f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}")

    def list_event_attendees(self, calendar_id, event_id):
        return self._paginate(f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees")

    def add_event_attendees(self, calendar_id, event_id, attendees):
        body = {"attendees": attendees}
        data = self._post(f"/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees", body=body)
        return data.get("data", {})

    def list_freebusy(self, time_min, time_max, user_ids):
        body = {"time_min": time_min, "time_max": time_max,
                "user_id": [{"user_id": uid, "type": "open_id"} for uid in user_ids]}
        data = self._post("/open-apis/calendar/v4/freebusy/list", body=body)
        return data.get("data", {})

    def list_meeting_rooms(self, building_id=None):
        params = {"page_size": 50}
        if building_id:
            params["building_id"] = building_id
        return self._paginate("/open-apis/meeting_room/room/list", params=params)

    def get_timeoff_events(self, calendar_id):
        return self._paginate(f"/open-apis/calendar/v4/calendars/{calendar_id}/events",
                              params={"page_size": 50})

    # =========================================================================
    # 9. TASKS v2 - Create, Update, Complete, Lists
    # =========================================================================

    def create_task(self, summary, description="", due=None, assignee_ids=None):
        body = {"summary": summary}
        if description:
            body["description"] = description
        if due:
            body["due"] = due
        if assignee_ids:
            body["members"] = [{"id": uid, "type": "user", "role": "assignee"} for uid in assignee_ids]
        data = self._post("/open-apis/task/v2/tasks", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create task failed: {data}")
        return data.get("data", {}).get("task", {})

    def get_task(self, task_id):
        data = self._get(f"/open-apis/task/v2/tasks/{task_id}")
        return data.get("data", {}).get("task", {})

    def update_task(self, task_id, updates):
        data = self._patch(f"/open-apis/task/v2/tasks/{task_id}", body=updates)
        return data.get("data", {}).get("task", {})

    def complete_task(self, task_id):
        return self.update_task(task_id, {"completed_at": str(int(time.time()))})

    def delete_task(self, task_id):
        return self._delete(f"/open-apis/task/v2/tasks/{task_id}")

    def list_tasks(self, page_size=50):
        return self._paginate("/open-apis/task/v2/tasks", params={"page_size": page_size})

    def add_task_members(self, task_id, member_ids, role="assignee"):
        body = {"members": [{"id": uid, "type": "user", "role": role} for uid in member_ids]}
        data = self._post(f"/open-apis/task/v2/tasks/{task_id}/members", body=body)
        return data.get("data", {})

    def create_tasklist(self, name):
        body = {"name": name}
        data = self._post("/open-apis/task/v2/tasklists", body=body)
        return data.get("data", {}).get("tasklist", {})

    def list_tasklists(self):
        return self._paginate("/open-apis/task/v2/tasklists")

    def add_task_to_tasklist(self, tasklist_id, task_id):
        body = {"task_id": task_id}
        data = self._post(f"/open-apis/task/v2/tasklists/{tasklist_id}/tasks", body=body)
        return data.get("data", {})

    def create_subtask(self, parent_task_id, summary, description=""):
        body = {"summary": summary, "description": description,
                "parent_task_id": parent_task_id}
        data = self._post("/open-apis/task/v2/tasks", body=body)
        return data.get("data", {}).get("task", {})

    def add_task_reminder(self, task_id, relative_fire_minute):
        body = {"reminder": {"relative_fire_minute": relative_fire_minute}}
        data = self._post(f"/open-apis/task/v2/tasks/{task_id}/reminders", body=body)
        return data.get("data", {})

    # =========================================================================
    # 10. APPROVAL - Definitions, Instances, Tasks
    # =========================================================================

    def list_approval_definitions(self):
        return self._paginate("/open-apis/approval/v4/approvals")

    def get_approval_definition(self, approval_code):
        data = self._get(f"/open-apis/approval/v4/approvals/{approval_code}")
        return data.get("data", {})

    def create_approval_instance(self, approval_code, form_data, user_id=None):
        body = {"approval_code": approval_code, "form": json.dumps(form_data)}
        if user_id:
            body["user_id"] = user_id
        data = self._post("/open-apis/approval/v4/instances", body=body)
        if data.get("code") != 0:
            raise Exception(f"Create approval failed: {data}")
        return data.get("data", {})

    def get_approval_instance(self, instance_id):
        data = self._get(f"/open-apis/approval/v4/instances/{instance_id}")
        return data.get("data", {})

    def list_approval_instances(self, approval_code, start_time=None, end_time=None):
        params = {"approval_code": approval_code, "page_size": 100}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        return self._paginate("/open-apis/approval/v4/instances", params=params)

    def approve_task(self, approval_code, instance_id, user_id, comment=""):
        body = {"approval_code": approval_code, "instance_code": instance_id,
                "user_id": user_id, "comment": comment, "action": "APPROVE"}
        data = self._post("/open-apis/approval/v4/tasks/approve", body=body)
        return data

    def reject_task(self, approval_code, instance_id, user_id, comment=""):
        body = {"approval_code": approval_code, "instance_code": instance_id,
                "user_id": user_id, "comment": comment, "action": "REJECT"}
        data = self._post("/open-apis/approval/v4/tasks/reject", body=body)
        return data

    # =========================================================================
    # 11. CONTACTS - Users, Departments, Groups
    # =========================================================================

    def get_user(self, user_id, id_type="open_id"):
        data = self._get(f"/open-apis/contact/v3/users/{user_id}",
                         params={"user_id_type": id_type})
        return data.get("data", {}).get("user", {})

    def search_users(self, query):
        body = {"query": query}
        data = self._post("/open-apis/search/v1/user", body=body,
                          params={"user_id_type": "open_id"})
        return data.get("data", {}).get("users", [])

    def list_department_users(self, department_id="0"):
        return self._paginate("/open-apis/contact/v3/users",
                              params={"department_id": department_id, "user_id_type": "open_id"})

    def list_departments(self, parent_department_id="0"):
        return self._paginate("/open-apis/contact/v3/departments",
                              params={"parent_department_id": parent_department_id})

    def get_department(self, department_id):
        data = self._get(f"/open-apis/contact/v3/departments/{department_id}")
        return data.get("data", {}).get("department", {})

    def list_user_groups(self):
        return self._paginate("/open-apis/contact/v3/group")

    def get_user_group_members(self, group_id):
        return self._paginate(f"/open-apis/contact/v3/group/{group_id}/member")

    # =========================================================================
    # 12. DRIVE - Files, Folders, Upload/Download
    # =========================================================================

    def list_drive_files(self, folder_token=None):
        params = {"page_size": 50}
        if folder_token:
            params["folder_token"] = folder_token
        return self._paginate("/open-apis/drive/v1/files", params=params)

    def create_folder(self, name, folder_token=None):
        body = {"name": name}
        if folder_token:
            body["folder_token"] = folder_token
        data = self._post("/open-apis/drive/v1/files/create_folder", body=body)
        return data.get("data", {})

    def get_file_meta(self, file_token, file_type="doc"):
        data = self._get(f"/open-apis/drive/v1/metas/batch_query",
                         params={"request_docs": json.dumps([{"doc_token": file_token, "doc_type": file_type}])})
        return data.get("data", {})

    def move_file(self, file_token, target_folder_token, file_type="doc"):
        body = {"type": file_type, "folder_token": target_folder_token}
        data = self._post(f"/open-apis/drive/v1/files/{file_token}/move", body=body)
        return data.get("data", {})

    def delete_file(self, file_token, file_type="doc"):
        data = self._delete(f"/open-apis/drive/v1/files/{file_token}",
                            params={"type": file_type})
        return data

    def upload_drive_file(self, file_bytes, file_name, parent_type="explorer",
                          parent_node=None):
        headers = {"Authorization": f"Bearer {self._get_tenant_token()}"}
        files_data = {
            "file": (file_name, file_bytes, "application/octet-stream"),
            "file_name": (None, file_name),
            "parent_type": (None, parent_type),
            "size": (None, str(len(file_bytes))),
        }
        if parent_node:
            files_data["parent_node"] = (None, parent_node)
        resp = requests.post(f"{self.base_url}/open-apis/drive/v1/medias/upload_all",
                             headers=headers, files=files_data, timeout=120)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") != 0:
            raise Exception(f"Drive upload failed: {result}")
        return result.get("data", {}).get("file_token", "")

    def download_drive_file(self, file_token):
        resp = requests.get(f"{self.base_url}/open-apis/drive/v1/medias/{file_token}/download",
                            headers=self._headers(None), timeout=120)
        resp.raise_for_status()
        return resp.content

    # =========================================================================
    # 13. SEARCH - Cross-Product Search
    # =========================================================================

    def search_messages(self, query, chat_id=None):
        body = {"query": query}
        if chat_id:
            body["chat_ids"] = [chat_id]
        data = self._post("/open-apis/search/v2/message", body=body,
                          params={"user_id_type": "open_id"})
        return data.get("data", {}).get("items", [])

    def search_docs(self, query):
        body = {"query": query}
        data = self._post("/open-apis/suite/docs-api/search/object", body=body)
        return data.get("data", {}).get("docs_entities", [])

    # =========================================================================
    # 14. EMAIL - Mailbox
    # =========================================================================

    def list_mailgroups(self):
        return self._paginate("/open-apis/mail/v1/mailgroups")

    def send_email(self, to, subject, body_html, from_address=None):
        body_data = {
            "to": [{"mail_address": addr} for addr in (to if isinstance(to, list) else [to])],
            "subject": subject,
            "body": {"content": body_html, "content_type": "text/html"},
        }
        data = self._post("/open-apis/mail/v1/user_mailboxes/me/messages/send", body=body_data)
        return data

    # =========================================================================
    # 15. BOT MANAGEMENT
    # =========================================================================

    def get_bot_info(self):
        data = self._get("/open-apis/bot/v3/info")
        if data.get("code") == 0:
            return data.get("bot", {})
        return {}

    def set_bot_menu(self, menu_items):
        body = {"menu": {"menu_items": menu_items}}
        data = self._post("/open-apis/bot/v3/menu", body=body)
        return data
