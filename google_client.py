"""
Google Calendar & Gmail client for Iron Bot.

Uses a service account with domain-wide delegation to read
Brendan's calendar events and flag important emails.
"""

import os
import json
import logging
import base64
import re
import traceback
from datetime import datetime, timezone, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# Scopes required (must match what's in Google Workspace Admin)
CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
ALL_SCOPES = CALENDAR_SCOPES + GMAIL_SCOPES

GOOGLE_SERVICE_ACCOUNT_CREDENTIALS = os.environ.get("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS", "")
GOOGLE_DELEGATED_USER = os.environ.get("GOOGLE_DELEGATED_USER", "brendan@highlifetech.co")


def _get_credentials():
    """Build delegated credentials from the service account JSON."""
    if not GOOGLE_SERVICE_ACCOUNT_CREDENTIALS:
        logger.error("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS env var is empty")
        return None
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_CREDENTIALS.strip())
        # Fix private key newlines (common issue with env var storage)
        if "private_key" in info:
            info["private_key"] = info["private_key"].replace("\\n", "\n")
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=ALL_SCOPES
        )
        delegated = creds.with_subject(GOOGLE_DELEGATED_USER)
        return delegated
    except Exception as e:
        logger.error(f"Failed to build Google credentials: {e}")
        logger.error(traceback.format_exc())
        return None

def get_todays_meetings():
    """Fetch today's Google Calendar events for the delegated user.

    Returns a list of dicts: [{summary, start, end, location, attendees}]
    """
    creds = _get_credentials()
    if not creds:
        return []
    try:
        service = build("calendar", "v3", credentials=creds)
        now = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-4)
        et_now = now + et_offset
        today_start = et_now.replace(hour=0, minute=0, second=0, microsecond=0) - et_offset
        today_end = et_now.replace(hour=23, minute=59, second=59, microsecond=0) - et_offset
        time_min = today_start.isoformat()
        time_max = today_end.isoformat()

        events_result = service.events().list(
            calendarId="primary",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
            maxResults=25,
        ).execute()

        events = events_result.get("items", [])
        meetings = []
        for event in events:
            start_raw = event.get("start", {})
            end_raw = event.get("end", {})
            start_str = start_raw.get("dateTime", start_raw.get("date", ""))
            end_str = end_raw.get("dateTime", end_raw.get("date", ""))
            try:
                if "T" in start_str:
                    st = datetime.fromisoformat(start_str)
                    start_fmt = st.strftime("%-I:%M %p")
                else:
                    start_fmt = "All day"
            except Exception:
                start_fmt = start_str
            try:
                if "T" in end_str:
                    et_time = datetime.fromisoformat(end_str)
                    end_fmt = et_time.strftime("%-I:%M %p")
                else:
                    end_fmt = ""
            except Exception:
                end_fmt = end_str

            attendee_names = []
            for att in event.get("attendees", []):
                name = att.get("displayName") or att.get("email", "")
                if att.get("self"):
                    continue
                attendee_names.append(name)

            meetings.append({
                "summary": event.get("summary", "(No title)"),
                "start": start_fmt,
                "end": end_fmt,
                "location": event.get("location", ""),
                "meet_link": event.get("hangoutLink", ""),
                "attendees": attendee_names,
                "status": event.get("status", ""),
            })

        logger.info(f"Google Calendar: fetched {len(meetings)} meetings for today")
        return meetings
    except Exception as e:
        logger.error(f"Google Calendar fetch error: {e}")
        return []

def get_recent_emails(hours_back=14):
    """Fetch recent emails from Gmail for the delegated user.

    Returns a list of dicts: [{subject, from, snippet, date, labels}]
    hours_back: how many hours to look back (14 for morning, 10 for evening)
    """
    creds = _get_credentials()
    if not creds:
        return []
    try:
        service = build("gmail", "v1", credentials=creds)
        after_ts = int((datetime.now(timezone.utc) - timedelta(hours=hours_back)).timestamp())
        query = f"in:inbox after:{after_ts}"

        results = service.users().messages().list(
            userId="me", q=query, maxResults=30
        ).execute()

        messages = results.get("messages", [])
        emails = []
        for msg_ref in messages:
            msg = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="metadata",
                metadataHeaders=["From", "Subject", "Date"]
            ).execute()
            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            label_ids = msg.get("labelIds", [])
            emails.append({
                "id": msg_ref["id"],
                "subject": headers.get("Subject", "(No subject)"),
                "from": headers.get("From", ""),
                "date": headers.get("Date", ""),
                "snippet": msg.get("snippet", ""),
                "labels": label_ids,
                "is_unread": "UNREAD" in label_ids,
            })

        logger.info(f"Gmail: fetched {len(emails)} recent emails")
        return emails
    except Exception as e:
        logger.error(f"Gmail fetch error: {e}")
        return []

def filter_important_emails(emails, anthropic_client=None):
    """Use Claude to identify which emails are project/business relevant.

    Returns a filtered list of important emails with reason.
    """
    if not emails or not anthropic_client:
        return emails[:10]

    email_summaries = []
    for i, e in enumerate(emails):
        email_summaries.append(
            f"{i+1}. From: {e['from']} | Subject: {e['subject']} | Preview: {e['snippet'][:120]}"
        )
    email_text = "\n".join(email_summaries)

    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{
                "role": "user",
                "content": (
                    "You are an executive assistant for a promotional products company (Highlifetech/HLT). "
                    "Review these emails and identify which ones are important or project-related.\n\n"
                    "Important = client emails, order updates, production issues, shipping, quotes, invoices, "
                    "vendor communications, urgent requests.\n"
                    "Not important = marketing newsletters, automated notifications, social media alerts, spam.\n\n"
                    f"Emails:\n{email_text}\n\n"
                    'Reply with ONLY a JSON array of the important email numbers and a short reason, like:\n'
                    '[{"num": 1, "reason": "Client asking about order status"}, '
                    '{"num": 3, "reason": "Vendor quote for new project"}]\n\n'
                    "If none are important, reply: []"
                )
            }]
        )
        result_text = response.content[0].text.strip()
        json_match = re.search(r'\[.*\]', result_text, re.DOTALL)
        if json_match:
            important_indices = json.loads(json_match.group())
            important_emails = []
            for item in important_indices:
                idx = item.get("num", 0) - 1
                if 0 <= idx < len(emails):
                    email = emails[idx].copy()
                    email["reason"] = item.get("reason", "")
                    important_emails.append(email)
            logger.info(f"Claude flagged {len(important_emails)}/{len(emails)} emails as important")
            return important_emails
        return []
    except Exception as e:
        logger.error(f"Claude email filter error: {e}")
        return [e for e in emails if e.get("is_unread")][:10]
