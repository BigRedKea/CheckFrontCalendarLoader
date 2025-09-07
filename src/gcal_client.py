# =============================
# src/gcal_client.py
# =============================
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Simple color choices (1..11). Adjust as you like.
COLOR_MAP = {
    "checkfront": "7",  # teal
}


class GCalClient:
    def __init__(self, sa_json_path: str, calendar_id: str):
        creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
        self.service = build("calendar", "v3", credentials=creds)
        self.calendar_id = calendar_id

    # --------- list / clear ---------
    def list_all_events(
        self,
        *,
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        show_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page_token = None
        if time_min is None:
            time_min = datetime(1970, 1, 1, tzinfo=timezone.utc)
        while True:
            res = self.service.events().list(
                calendarId=self.calendar_id,
                timeMin=time_min.isoformat(),
                timeMax=time_max.isoformat() if time_max else None,
                maxResults=2500,
                singleEvents=True,
                orderBy="startTime",
                pageToken=page_token,
                showDeleted=show_deleted,
            ).execute()
            items.extend(res.get("items", []))
            page_token = res.get("nextPageToken")
            if not page_token:
                break
        return items

    def clear(self):
        self.service.calendars().clear(calendarId=self.calendar_id).execute()

    # --------- upsert / delete range ---------
    def upsert(self, event_bodies: Iterable[Dict[str, Any]], *, time_min: Optional[datetime] = None, time_max: Optional[datetime] = None) -> Tuple[int, int]:
        existing = self.list_all_events(time_min=time_min, time_max=time_max, show_deleted=False)
        by_key: Dict[str, Dict[str, Any]] = {}
        for ev in existing:
            key = ev.get("extendedProperties", {}).get("private", {}).get("syncKey")
            if key:
                by_key[key] = ev
        inserted = updated = 0
        for body in event_bodies:
            key = body.get("extendedProperties", {}).get("private", {}).get("syncKey")
            if not key:
                self.service.events().insert(calendarId=self.calendar_id, body=body).execute()
                inserted += 1
                print(f"inserted {body.get("description")}")
                continue
            if key in by_key:
                ev_id = by_key[key]["id"]
                self.service.events().update(calendarId=self.calendar_id, eventId=ev_id, body=body).execute()
                print(f"updated{body.get("description")}")
                updated += 1
            else:
                self.service.events().insert(calendarId=self.calendar_id, body=body).execute()
                inserted += 1
                print(f"inserted {body.get("description")}")
        return inserted, updated

    def delete_range(self, start_dt: datetime, end_dt: datetime, *, filter_sync_prefix: Optional[str] = None) -> int:
        to_delete = self.list_all_events(time_min=start_dt, time_max=end_dt, show_deleted=False)
        deleted = 0
        for ev in to_delete:
            if filter_sync_prefix:
                key = ev.get("extendedProperties", {}).get("private", {}).get("syncKey", "")
                if not isinstance(key, str) or not key.startswith(filter_sync_prefix):
                    continue
            try:
                self.service.events().delete(calendarId=self.calendar_id, eventId=ev["id"]).execute()
                deleted += 1
            except HttpError as e:
                if not (getattr(e, "resp", None) and e.resp.status == 404):
                    raise
        return deleted


# ---------- Event body builder for Checkfront ----------

def event_body_from_cf(
    *,
    booking_code: str,
    title: str,
    start_iso: str,
    end_iso: str,
    timezone_str: str,
    location: Optional[str] = None,
    description: Optional[str] = None,
    color_id: Optional[str] = None,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "summary": title,
        "start": {"dateTime": start_iso, "timeZone": timezone_str},
        "end": {"dateTime": end_iso, "timeZone": timezone_str},
        "colorId": color_id or COLOR_MAP["checkfront"],
        "extendedProperties": {"private": {"syncKey": f"cf:{booking_code}"}},
        "transparency": "opaque",  # default busy
    }
    if location:
        body["location"] = location
    if description:
        body["description"] = description
    return body


# ---------- Recurrence helpers ----------

RFC5545_DAYS = ["MO","TU","WE","TH","FR","SA","SU"]

def rrule_weekly(*, byday: str, until: datetime) -> str:
    """Build a simple weekly RRULE string. `until` is converted to UTC RFC5545 (YYYYMMDDTHHMMSSZ)."""
    if byday not in RFC5545_DAYS:
        raise ValueError("byday must be one of MO,TU,WE,TH,FR,SA,SU")
    until_utc = until.astimezone(timezone.utc)
    return f"RRULE:FREQ=WEEKLY;BYDAY={byday};UNTIL={until_utc.strftime('%Y%m%dT%H%M%SZ')}"


def exdate_list(*, start_dt: datetime, until_dt: datetime, byday: int, have_dates: set) -> List[str]:
    """Return a list of RFC5545 EXDATE strings (UTC) for weekly series where some dates are missing.
    - byday: 0=Mon .. 6=Sun
    - have_dates: set of date() objects that actually have an occurrence
    """
    cur = start_dt
    out: List[str] = []
    # Align cur to the first occurrence day-of-week
    delta = (byday - cur.weekday()) % 7
    cur = cur + timedelta(days=delta)
    while cur < until_dt:
        if cur.date() not in have_dates and cur >= start_dt:
            out.append(cur.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        cur += timedelta(days=7)
    return out
