# =============================
# src/gcal_client.py
# =============================
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from .adapters import _to_datetime

import os
import re, hashlib, base64

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

SAFE_RE = re.compile(r"[^a-z0-9_-]")
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

from datetime import datetime
from typing import Dict, List, Tuple, Any

# Assumes you have a fetch that pulls ONLY events you manage (e.g. source=checkfront-sync)
# and within the time window:
# fetch_existing_managed(svc, calendar_id, time_min, time_max) -> List[dict]

#def _updated(ev: dict) -> datetime:
#    # RFC3339 -> datetime
#   return datetime.fromisoformat(ev.get("updated"))

def _start(ev: dict) -> datetime:
    return ev.get("start")

def _event_key(ev: dict) -> str | None:
    return ((ev.get("extendedProperties") or {}).get("private") or {}).get("event_key")

def clean_and_bucket_existing(
    svc,
    calendar_id: str,
    time_min: datetime,
    time_max: datetime,
    tzid: str,
    send_updates: str = "none",
) -> Tuple[Dict[str, dict], Dict[str, int], List[Dict[str, Any]]]:
    """
    - Deletes events with no extendedProperties.private.event_key
    - For duplicate keys: keeps one survivor, deletes others
    Returns: (existing_map, stats, results)
      existing_map: {event_key: survivor_event}
      stats: {"deleted_no_key": int, "deleted_duplicates": int}
      results: list of action logs
    """
    # 1) Fetch events we manage (e.g., with privateExtendedProperty="source=checkfront-sync")
    existing_list: List[dict] = fetch_existing_synced_events(svc, calendar_id, time_min, time_max, tzid)


    deleted_no_key = 0
    deleted_dupes = 0
    results: List[Dict[str, Any]] = []

    # 2) First pass: delete events with NO event_key
    keyed_events: List[dict] = []
    for ev in existing_list.values():
        k = _event_key(ev)
        if not k:
            # delete & log
            ev_id = ev.get("id")
            if ev_id:
                svc.events().delete(calendarId=calendar_id, eventId=ev_id, sendUpdates=send_updates).execute()
                deleted_no_key += 1
                results.append({"status": "deleted_no_key", "event_id": ev_id})
            continue
        keyed_events.append(ev)

    # 3) Group remaining events by key
    buckets: Dict[str, List[dict]] = {}
    for ev in keyed_events:
        k = _event_key(ev)  # guaranteed non-empty now
        buckets.setdefault(k, []).append(ev)

    # 4) Choose one survivor per key (earliest start) and delete the others
    existing_map: Dict[str, dict] = {}
    for k, evs in buckets.items():
        survivor = evs[0]   # keep the earliest start event
        existing_map[k] = survivor

        for ev in (e for e in evs if e is not survivor and e.get("id")):
            svc.events().delete(calendarId=calendar_id, eventId=ev["id"], sendUpdates=send_updates).execute()
            deleted_dupes += 1
            results.append({"status": "deleted_duplicate", "event_key": k, "event_id": ev["id"]})

    stats = {"deleted_no_key": deleted_no_key, "deleted_duplicates": deleted_dupes}
    return existing_map, stats, results


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

def fetch_existing_synced_events(svc, calendar_id: str, time_min: datetime, time_max: datetime, tzid) -> Dict[str, dict]:
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
            events_by_id[ev["id"]] = _norm_event_view(ev, tzid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return events_by_id

from datetime import datetime
from typing import Dict, List, Tuple
from googleapiclient.errors import HttpError

# assumes you already have these helpers:
# - fetch_existing_synced_events(svc, calendar_id, time_min, time_max)
# - _norm_body_view(...)
# - _norm_event_view(...)
# - _diff_for_patch(...)

def fetch_existing_by_key(svc, calendar_id: str, tmin, tmax) -> dict[str, dict]:
    """Return {event_key: full_event} for all events we manage (source=checkfront-sync) in the window."""
    out, token = {}, None
    while True:
        resp = svc.events().list(
            calendarId=calendar_id,
            privateExtendedProperty="source=checkfront-sync",
            timeMin=tmin.isoformat(),
            timeMax=tmax.isoformat(),
            singleEvents=True,
            showDeleted=False,
            maxResults=2500,
            pageToken=token,
        ).execute()
        for ev in resp.get("items", []):
            priv = ((ev.get("extendedProperties") or {}).get("private") or {})
            k = priv.get("event_key")
            if k:
                out[k] = ev
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def _start_of_body(body: dict) -> datetime:
    return datetime.fromisoformat(body["start"]["dateTime"])

def _start_of_event(ev: dict) -> datetime:
    return datetime.fromisoformat(ev["start"]["dateTime"])

def sync_calendar(
    svc,
    cfg,
    calendar_id: str,
    bookings_for_cal: list[tuple[str, dict]],  # [(event_key, desired_body)], desired_body includes extendedProperties.private.event_key
    time_min,
    time_max,
    tzid,
    send_updates: str = "none",
    delete_orphans: bool = True,

):
    
    filtered_bookings: List[Tuple[str, dict]] = []
    for key, body in bookings_for_cal:
        start_dt = body.get("start", {}).get("dateTime")
        end_dt   = body.get("end", {}).get("dateTime")
        if not (start_dt and end_dt):
            continue
        start = datetime.fromisoformat(start_dt)
        end   = datetime.fromisoformat(end_dt)
        # keep if event overlaps [time_min, time_max]
        if end >= time_min and start <= time_max:
            filtered_bookings.append((key, body))

    # --- sort filtered bookings deterministically ---
    filtered_bookings.sort(key=lambda kv: (_start_of_body(kv[1]), kv[0]))
    


    existing, clean_stats, clean_logs = clean_and_bucket_existing(
        svc=svc,
        calendar_id=calendar_id,
        time_min=time_min,
        time_max=time_max,
        tzid = tzid,
        send_updates=send_updates,
    )


    
    # Map existing events we manage by their event_key
    existing = fetch_existing_by_key(svc, calendar_id, time_min, time_max)

    inserted = patched = unchanged = deleted = 0
    results = []

    # Upsert pass
    active_keys = set()
    for event_key, desired in filtered_bookings:
        active_keys.add(event_key)
        current = existing.get(event_key)
        desired_view = _norm_body_view(desired, tzid)

        if current is None:
            # INSERT (no custom 'id'; rely on event_key in extendedProperties)
            res = svc.events().insert(
                calendarId=calendar_id, body=desired, sendUpdates=send_updates
            ).execute()
            inserted += 1
            print(f"Inserted {desired_view.get("summary")} {desired_view.get("start")} ");
            results.append({"event_key": event_key, "status": "inserted", "htmlLink": res.get("htmlLink")})
        else:
            patch = _diff_for_patch(_norm_event_view(current,tzid), desired_view)
            if patch:
                res = svc.events().patch(
                    calendarId=calendar_id, eventId=current["id"], body=patch, sendUpdates=send_updates
                ).execute()
                patched += 1
                print(f"Patched {desired_view.get("summary")} {desired_view.get("start")} ");
                results.append({"event_key": event_key, "status": "patched", "htmlLink": res.get("htmlLink")})
            else:
                unchanged += 1
                results.append({"event_key": event_key, "status": "unchanged"})

    # Delete orphans: anything we previously synced (source=checkfront-sync)
    # but whose event_key is NOT present in the current feed
    if delete_orphans:
        existing_sorted = sorted(
            existing.items(),
            key=lambda kv: (_start_of_event(kv[1]), kv[0])  # (event_key, event)
        )

        for k, ev in existing_sorted:
            if k not in active_keys:
                svc.events().delete(
                    calendarId=calendar_id, eventId=ev["id"], sendUpdates=send_updates
                ).execute()
                print(f"Deleted {ev.get("summary")} {ev.get("start")} ");
                deleted += 1
                results.append({"event_key": k, "status": "deleted"})

    return {
        "calendar_id": calendar_id,
        "inserted": inserted,
        "patched": patched,
        "unchanged": unchanged,
        "deleted": deleted,
        "results": results,
    }



def eid(key: str, maxlen: int = 50) -> str:
    # Deterministic base32 id, always safe
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    return base64.b32encode(digest).decode("utf-8").lower().strip("=")[:maxlen]

def make_event_id_from_sku_start(sku: str, start_iso: str) -> str:
    # If you really want readable ids, sanitize aggressively and prefix with 'e'
    readable = f"{(sku or 'nosku').lower()}_{start_iso.replace(':','_')}"
    readable = SAFE_RE.sub("-", readable)
    if len(readable) < 5 or readable[0] in "-_":
        readable = "e-" + readable  # ensure good first char & min length
    return readable[:50]


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

def _norm_event_view(e: dict, tzid :str) -> dict:
    """Project an event into just the fields we manage, normalised."""
    if not e: 
        return {}
    v = {
        "summary": (e.get("summary") or "").strip(),
        "description": (e.get("description") or "").rstrip(),
        "location": e.get("location") or None,
        "colorId": e.get("colorId") or None,
        "start": {
            "dateTime": (e.get("start") or {}), #.get("dateTime")
            "timeZone": tzid,
        },
        "end": {
            "dateTime": (e.get("end") or {}),
            "timeZone": tzid,
        },
        "reminders": e.get("reminders") or None,
        "extendedProperties": {"private": (e.get("extendedProperties") or {}).get("private") or {}},
        "attendees": None,
    }
    if "attendees" in e and e["attendees"]:
        addrs = sorted({(a.get("email") or "").lower() for a in e["attendees"] if a.get("email")})
        v["attendees"] = [{"email": a} for a in addrs] if addrs else None
    return v

def _norm_body_view(b: dict, tzid) -> dict:
    """Same projection but for the body we’re about to send."""
    return _norm_event_view(b, tzid)


