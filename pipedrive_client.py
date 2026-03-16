"""
Pipedrive CRM Client for Iron Bot
Handles deals, contacts, pipeline stages, and quote/activity data.
"""
import os
import logging
import requests

logger = logging.getLogger(__name__)

PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"


class PipedriveClient:
    """Client for Pipedrive CRM API."""

    def __init__(self):
        self.api_key = os.environ.get("PIPEDRIVE_API_KEY", "")
        self.configured = bool(self.api_key)
        if not self.configured:
            logger.warning("Pipedrive not configured — missing PIPEDRIVE_API_KEY")

    def _params(self, extra: dict = None) -> dict:
        p = {"api_token": self.api_key}
        if extra:
            p.update(extra)
        return p

    def _not_configured(self):
        return {"error": "Pipedrive not configured", "hint": "Add PIPEDRIVE_API_KEY to Railway."}

    def _get_all_pages(self, endpoint: str, params: dict = None) -> list:
        """Fetch all pages from a Pipedrive list endpoint."""
        results = []
        start = 0
        limit = 100
        while True:
            p = self._params({"start": start, "limit": limit})
            if params:
                p.update(params)
            resp = requests.get(f"{PIPEDRIVE_BASE}{endpoint}", params=p, timeout=30)
            if not resp.ok:
                logger.error(f"Pipedrive {endpoint} {resp.status_code}: {resp.text[:300]}")
                break
            data = resp.json()
            items = data.get("data") or []
            results.extend(items)
            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start += limit
        return results

    # -------------------------------------------------------------------------
    # DEALS
    # -------------------------------------------------------------------------
    def get_all_deals(self, status: str = "open") -> dict:
        """Get all deals, optionally filtered by status: open, won, lost, all_not_deleted."""
        if not self.configured:
            return self._not_configured()
        try:
            deals = self._get_all_pages("/deals", {"status": status})
            simplified = []
            for d in deals:
                simplified.append({
                    "id": d.get("id"),
                    "title": d.get("title"),
                    "status": d.get("status"),
                    "stage": d.get("stage_id"),
                    "stage_name": d.get("stage_order_nr"),
                    "value": d.get("value"),
                    "currency": d.get("currency"),
                    "person": d.get("person_name"),
                    "org": d.get("org_name"),
                    "owner": d.get("owner_name"),
                    "expected_close": d.get("expected_close_date"),
                    "add_time": d.get("add_time"),
                    "update_time": d.get("update_time"),
                    "won_time": d.get("won_time"),
                    "lost_time": d.get("lost_time"),
                    "lost_reason": d.get("lost_reason"),
                    "pipeline": d.get("pipeline_id"),
                })
            return {"deals": simplified, "count": len(simplified), "status_filter": status}
        except Exception as e:
            logger.error(f"Pipedrive get_all_deals error: {e}")
            return {"error": str(e)}

    def search_deals(self, term: str) -> dict:
        """Search deals by name, org, or person."""
        if not self.configured:
            return self._not_configured()
        try:
            resp = requests.get(
                f"{PIPEDRIVE_BASE}/deals/search",
                params=self._params({"term": term, "limit": 20}),
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data", {}).get("items", []) or []
            results = []
            for item in items:
                d = item.get("item", {})
                results.append({
                    "id": d.get("id"),
                    "title": d.get("title"),
                    "status": d.get("status"),
                    "value": d.get("value"),
                    "currency": d.get("currency"),
                    "person": d.get("person", {}).get("name") if d.get("person") else None,
                    "org": d.get("organization", {}).get("name") if d.get("organization") else None,
                    "stage": d.get("stage", {}).get("name") if d.get("stage") else None,
                })
            return {"results": results, "count": len(results), "search_term": term}
        except Exception as e:
            logger.error(f"Pipedrive search_deals error: {e}")
            return {"error": str(e)}

    def get_deal_details(self, deal_id: int) -> dict:
        """Get full details for a specific deal including activities and notes."""
        if not self.configured:
            return self._not_configured()
        try:
            resp = requests.get(
                f"{PIPEDRIVE_BASE}/deals/{deal_id}",
                params=self._params(),
                timeout=30
            )
            resp.raise_for_status()
            d = resp.json().get("data", {})
            return {
                "id": d.get("id"),
                "title": d.get("title"),
                "status": d.get("status"),
                "value": d.get("value"),
                "currency": d.get("currency"),
                "person": d.get("person_name"),
                "org": d.get("org_name"),
                "owner": d.get("owner_name"),
                "stage": d.get("stage_order_nr"),
                "expected_close": d.get("expected_close_date"),
                "add_time": d.get("add_time"),
                "won_time": d.get("won_time"),
                "lost_reason": d.get("lost_reason"),
                "next_activity": d.get("next_activity_subject"),
                "last_activity": d.get("last_activity_date"),
                "notes_count": d.get("notes_count"),
                "activities_count": d.get("activities_count"),
                "email_messages_count": d.get("email_messages_count"),
            }
        except Exception as e:
            logger.error(f"Pipedrive get_deal_details error: {e}")
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # PIPELINE & STAGES
    # -------------------------------------------------------------------------
    def get_pipeline_summary(self) -> dict:
        """Get a summary of all pipelines and their stages with deal counts."""
        if not self.configured:
            return self._not_configured()
        try:
            pipelines_resp = requests.get(
                f"{PIPEDRIVE_BASE}/pipelines",
                params=self._params(),
                timeout=30
            )
            pipelines_resp.raise_for_status()
            pipelines = pipelines_resp.json().get("data") or []

            stages_resp = requests.get(
                f"{PIPEDRIVE_BASE}/stages",
                params=self._params(),
                timeout=30
            )
            stages_resp.raise_for_status()
            stages = stages_resp.json().get("data") or []

            stage_map = {s["id"]: s["name"] for s in stages}

            result = []
            for p in pipelines:
                result.append({
                    "pipeline_id": p.get("id"),
                    "pipeline_name": p.get("name"),
                    "stages": [
                        {"stage_id": s["id"], "stage_name": s["name"]}
                        for s in stages if s.get("pipeline_id") == p.get("id")
                    ]
                })
            return {"pipelines": result}
        except Exception as e:
            logger.error(f"Pipedrive pipeline summary error: {e}")
            return {"error": str(e)}

    def get_deals_by_stage(self, stage_name: str) -> dict:
        """Get all open deals in a specific pipeline stage."""
        if not self.configured:
            return self._not_configured()
        try:
            stages_resp = requests.get(
                f"{PIPEDRIVE_BASE}/stages",
                params=self._params(),
                timeout=30
            )
            stages_resp.raise_for_status()
            stages = stages_resp.json().get("data") or []
            matched = [s for s in stages if stage_name.lower() in s.get("name", "").lower()]
            if not matched:
                return {"message": f"No stage found matching '{stage_name}'"}

            all_deals = []
            for stage in matched:
                deals_resp = requests.get(
                    f"{PIPEDRIVE_BASE}/stages/{stage['id']}/deals",
                    params=self._params({"status": "open"}),
                    timeout=30
                )
                deals_resp.raise_for_status()
                deals = deals_resp.json().get("data") or []
                for d in deals:
                    all_deals.append({
                        "title": d.get("title"),
                        "value": d.get("value"),
                        "currency": d.get("currency"),
                        "org": d.get("org_name"),
                        "owner": d.get("owner_name"),
                        "expected_close": d.get("expected_close_date"),
                        "stage": stage.get("name"),
                    })
            return {"stage": stage_name, "deals": all_deals, "count": len(all_deals)}
        except Exception as e:
            logger.error(f"Pipedrive get_deals_by_stage error: {e}")
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # PERSONS & ORGANISATIONS
    # -------------------------------------------------------------------------
    def search_contacts(self, term: str) -> dict:
        """Search persons and organisations by name."""
        if not self.configured:
            return self._not_configured()
        try:
            resp = requests.get(
                f"{PIPEDRIVE_BASE}/persons/search",
                params=self._params({"term": term, "limit": 10}),
                timeout=30
            )
            resp.raise_for_status()
            items = resp.json().get("data", {}).get("items", []) or []
            results = []
            for item in items:
                p = item.get("item", {})
                results.append({
                    "name": p.get("name"),
                    "org": p.get("organization", {}).get("name") if p.get("organization") else None,
                    "email": p.get("emails", [{}])[0].get("value") if p.get("emails") else None,
                    "phone": p.get("phones", [{}])[0].get("value") if p.get("phones") else None,
                    "open_deals": p.get("open_deals_count"),
                })
            return {"results": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Pipedrive search_contacts error: {e}")
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # ACTIVITIES
    # -------------------------------------------------------------------------
    def get_upcoming_activities(self, days: int = 7) -> dict:
        """Get all upcoming activities (calls, meetings, tasks) in the next N days."""
        if not self.configured:
            return self._not_configured()
        try:
            resp = requests.get(
                f"{PIPEDRIVE_BASE}/activities",
                params=self._params({"done": 0, "limit": 50}),
                timeout=30
            )
            resp.raise_for_status()
            activities = resp.json().get("data") or []
            simplified = []
            for a in activities:
                simplified.append({
                    "subject": a.get("subject"),
                    "type": a.get("type"),
                    "due_date": a.get("due_date"),
                    "due_time": a.get("due_time"),
                    "owner": a.get("owner_name"),
                    "person": a.get("person_name"),
                    "org": a.get("org_name"),
                    "deal": a.get("deal_title"),
                    "note": a.get("note"),
                    "done": a.get("done"),
                })
            return {"activities": simplified, "count": len(simplified)}
        except Exception as e:
            logger.error(f"Pipedrive get_upcoming_activities error: {e}")
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # REVENUE / STATS
    # -------------------------------------------------------------------------
    def get_won_deals_summary(self, period: str = "this_year") -> dict:
        """Get a summary of won deals for revenue reporting."""
        if not self.configured:
            return self._not_configured()
        try:
            deals = self._get_all_pages("/deals", {"status": "won"})
            total_value = sum(d.get("value") or 0 for d in deals)
            by_owner = {}
            for d in deals:
                owner = d.get("owner_name", "Unknown")
                by_owner[owner] = by_owner.get(owner, 0) + (d.get("value") or 0)
            return {
                "won_deals_count": len(deals),
                "total_value": total_value,
                "currency": deals[0].get("currency") if deals else "USD",
                "by_owner": by_owner,
            }
        except Exception as e:
            logger.error(f"Pipedrive won deals summary error: {e}")
            return {"error": str(e)}

    def is_configured(self) -> bool:
        return self.configured
