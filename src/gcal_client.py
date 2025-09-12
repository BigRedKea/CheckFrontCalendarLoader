# =============================
# src/gcal_client.py
# =============================
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import os

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


#---------- Event body builder for Checkfront ----------

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

def fetch_existing_synced_events(svc, calendar_id: str, time_min: datetime, time_max: datetime) -> Dict[str, dict]:
    """Return {eventId: normalized_event} for events we own (source=checkfront-sync) in [time_min, time_max]."""
    events_by_id: Dict[str, dict] = {}
    page_token = None
    while True:
        req = svc.events().list(
            calendarId=calendar_id,
            timeMin=time_min.isoformat(),
            timeMax=time_max.isoformat(),
            privateExtendedProperty="source=checkfront-sync",
            singleEvents=True,
            showDeleted=False,
            maxResults=2500,
            pageToken=page_token,
        )
        resp = req.execute()
        for ev in resp.get("items", []):
            events_by_id[ev["id"]] = _norm_event_view(ev)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return events_by_id

def sync_calendar(
    svc,
    cfg: Dict,
    calendar_id: str,
    # bookings_for_cal: iterable of tuples (event_id, desired_body)
    slots: {str,[]},
    time_min: datetime,
    time_max: datetime,
    send_updates: str = "none",   # "none" | "all"
    delete_orphans: bool = True
) -> Dict:
    """
    bookings_for_cal items are already mapped to this calendar_id and include:
      - event_id: stable per-calendar (e.g. _eid(f"{code}:{calendar_id}"))
      - desired_body: from _desired_body(...)
    """
    existing_map = fetch_existing_synced_events(svc, calendar_id, time_min, time_max)

    inserted = 0
    patched  = 0
    unchanged = 0
    deleted = 0
    results = []

    # ---- Upsert pass
    for event_id, desired in slots:
        desired_view = _norm_body_view(desired)
        current_view = existing_map.get(event_id)
        if current_view is None:
            # INSERT (with fixed id)
            body = dict(desired)
            body["id"] = event_id
            try:
                res = svc.events().insert(calendarId=calendar_id, body=body, sendUpdates=send_updates).execute()
                inserted += 1
                results.append({"event_id": event_id, "status": "inserted", "htmlLink": res.get("htmlLink")})
            except HttpError as e:
                # If someone created it concurrently, PATCH instead
                if getattr(e.resp, "status", None) == 409:
                    res = svc.events().patch(calendarId=calendar_id, eventId=event_id, body=desired, sendUpdates=send_updates).execute()
                    patched += 1
                    results.append({"event_id": event_id, "status": "patched", "htmlLink": res.get("htmlLink")})
                else:
                    raise
        else:
            # Compare → PATCH only when changed
            patch = _diff_for_patch(current_view, desired_view)
            if patch:
                res = svc.events().patch(calendarId=calendar_id, eventId=event_id, body=patch, sendUpdates=send_updates).execute()
                patched += 1
                results.append({"event_id": event_id, "status": "patched", "htmlLink": res.get("htmlLink")})
            else:
                unchanged += 1
                results.append({"event_id": event_id, "status": "unchanged", "htmlLink": None})

    # ---- Delete pass (orphans = previously-synced events not in current feed)
    if delete_orphans:
        active_ids = {eid for (eid, _) in slots}
        for existing_id in existing_map.keys():
            if existing_id not in active_ids:
                svc.events().delete(calendarId=calendar_id, eventId=existing_id, sendUpdates=send_updates).execute()
                deleted += 1
                results.append({"event_id": existing_id, "status": "deleted", "htmlLink": None})

    return {
        "calendar_id": calendar_id,
        "inserted": inserted,
        "patched": patched,
        "unchanged": unchanged,
        "deleted": deleted,
        "results": results,
    }


def _diff_for_patch(current_view: dict, desired_view: dict) -> dict:
    """
    Return a minimal patch dict with only changed fields.
    Empty dict => no patch needed.
    """
    patch = {}

    # Simple top-level fields
    for key in ("summary", "description", "location", "colorId", "reminders"):
        if current_view.get(key) != desired_view.get(key):
            patch[key] = desired_view.get(key)

    # Start/end objects
    for key in ("start", "end"):
        if (current_view.get(key) or {}) != (desired_view.get(key) or {}):
            patch[key] = desired_view.get(key)

    # Extended private props (only the ones we manage)
    cur_priv = (current_view.get("extendedProperties") or {}).get("private")
    des_priv = (desired_view.get("extendedProperties") or {}).get("private")
    if cur_priv != des_priv:
        patch["extendedProperties"] = {"private": des_priv or {}}

    # Attendees (compare as normalised list of {email})
    if current_view.get("attendees") != desired_view.get("attendees"):
        patch["attendees"] = desired_view.get("attendees")

    # Don’t send None unless you intend to clear a field
    return {k: v for k, v in patch.items() if v is not None}


def rrule_weekly(by_day: list[str], interval: int = 1, count: int | None = None, until: str | None = None) -> str:
    """
    Build a simple weekly RRULE.

    Parameters
    ----------
    by_day : list[str]
        Days of the week in iCal two-letter format: 
        ["MO","TU","WE","TH","FR","SA","SU"]
    interval : int
        Every N weeks (default 1 = every week).
    count : int | None
        Total number of recurrences. Mutually exclusive with `until`.
    until : str | None
        End date/time in UTC as YYYYMMDD or YYYYMMDDT000000Z.

    Returns
    -------
    str
        e.g. 'RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR'
    """
    parts = [f"FREQ=WEEKLY", f"INTERVAL={interval}"]
    if by_day:
        parts.append("BYDAY=" + ",".join(by_day))
    if count:
        parts.append(f"COUNT={count}")
    elif until:
        parts.append(f"UNTIL={until}")
    return "RRULE:" + ";".join(parts)

def _norm_event_view(e: dict) -> dict:
    """Project an event into just the fields we manage, normalised."""
    if not e: 
        return {}
    v = {
        "summary": (e.get("summary") or "").strip(),
        "description": (e.get("description") or "").rstrip(),
        "location": e.get("location") or None,
        "colorId": e.get("colorId") or None,
        "start": {
            "dateTime": (e.get("start") or {}).get("dateTime"),
            "timeZone": (e.get("start") or {}).get("timeZone"),
        },
        "end": {
            "dateTime": (e.get("end") or {}).get("dateTime"),
            "timeZone": (e.get("end") or {}).get("timeZone"),
        },
        "reminders": e.get("reminders") or None,
        "extendedProperties": {"private": (e.get("extendedProperties") or {}).get("private") or {}},
        "attendees": None,
    }
    if "attendees" in e and e["attendees"]:
        addrs = sorted({(a.get("email") or "").lower() for a in e["attendees"] if a.get("email")})
        v["attendees"] = [{"email": a} for a in addrs] if addrs else None
    return v

def _norm_body_view(b: dict) -> dict:
    """Same projection but for the body we’re about to send."""
    return _norm_event_view(b)


