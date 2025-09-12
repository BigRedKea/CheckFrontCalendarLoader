# gcal_sync.py
#import os, hashlib, base64, re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

from .helpers import _to_datetime, _safe_sku

SCOPES = ["https://www.googleapis.com/auth/calendar"]

# --- Auth ---
def get_calendar_service(sa_json_path: str):
    creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    return build("calendar", "v3", credentials=creds)

# --- Helpers ---
# def _eid(key: str) -> str:
#     """Stable eventId (lowercase base32 of SHA1)."""
#     h = hashlib.sha1(key.encode("utf-8")).digest()
#     return base64.b32encode(h).decode("ascii").strip("=").lower()[:50]



def _choose_color_id(booked: int, capacity: Optional[int]) -> Optional[str]:
    if capacity is None: return None
    if booked <= 0:      return "2"   # green
    if booked >= capacity: return "11" # red
    return "6"                         # orange

def _resolve_calendars_for_tags(tag_names: List[str], cfg: Dict) -> List[str]:
    """
    Calendar-centric resolution: return all calendarIds whose config.tags intersect tag_names.
    Deduped, preserves config order. Falls back to default_calendar_id (if set) when none match.
    """
    calendars = cfg.get("calendars", {}) or {}
    tag_set = set(tag_names)
    out: List[str] = []
    seen = set()
    for cal_id, cal_def in calendars.items():
        cal_tags = set((cal_def or {}).get("tags", []))
        calendarid =cal_def.get("calendarid")
        if tag_set & cal_tags and calendarid not in seen:
            out.append(calendarid)
            seen.add(calendarid)

    return out


# =========================
# Lookup by extended property
# =========================
def find_event_by_key(
    svc,
    calendar_id: str,
    key: str,
    time_window: Optional[Tuple[datetime, datetime]] = None
):
    """
    Returns the first event that has extendedProperties.private.event_key == key on this calendar.
    Optionally constrain with a (timeMin, timeMax) window to speed up search.
    """
    kwargs = dict(
        calendarId=calendar_id,
        privateExtendedProperty=f"event_key={key}",
        singleEvents=True,
        showDeleted=False,
        maxResults=5,
    )
    if time_window:
        tmin, tmax = time_window
        kwargs["timeMin"] = tmin.isoformat()
        kwargs["timeMax"] = tmax.isoformat()

    resp = svc.events().list(**kwargs).execute()
    items = resp.get("items", [])
    return items[0] if items else None

def to_dt(val, default_tz=None) -> Optional[datetime]:
    """
    Accepts a datetime or ISO8601 string and returns an aware datetime.
    - If string has an offset (e.g. +10:00), we keep it.
    - If string has no offset, we attach default_tz (if provided).
    - Returns None if val is falsy.
    """
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    # Handle trailing 'Z' (UTC) for older Pythons
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)  # understands '2025-09-07T15:00:00+10:00'
    if dt.tzinfo is None and default_tz is not None:
        dt = dt.replace(tzinfo=default_tz)
    return dt

def make_event_key(sku: str, start_val) -> str:
    """
    Stable key (not the event.id): <safe_sku>_YYYY_MM_DD_HH_mm
    Stored in extendedProperties.private.event_key and used to find/upsert.
    """
    dt = _to_datetime(start_val)
    if dt is None:
        raise ValueError("make_event_key requires a valid start datetime")
    dt_str = dt.strftime("%Y_%m_%d_%H_%M")
    return f"{_safe_sku(sku)}_{dt_str}"[:256] 

# =========================
# Lookup by extended property
# =========================
def find_event_by_key(
    svc,
    calendar_id: str,
    key: str,
    time_window: Optional[Tuple[datetime, datetime]] = None
):
    """
    Returns the first event that has extendedProperties.private.event_key == key on this calendar.
    Optionally constrain with a (timeMin, timeMax) window to speed up search.
    """
    kwargs = dict(
        calendarId=calendar_id,
        privateExtendedProperty=f"event_key={key}",
        singleEvents=True,
        showDeleted=False,
        maxResults=5,
    )
    if time_window:
        tmin, tmax = time_window
        kwargs["timeMin"] = _to_datetime(tmin).isoformat()
        kwargs["timeMax"] = tmax.isoformat()

    resp = svc.events().list(**kwargs).execute()
    items = resp.get("items", [])
    return items[0] if items else None

def _norm_event_view(e: dict) -> dict:
    """
    Extract only the fields we manage from an existing Google Calendar event.
    Normalises values so that comparisons are reliable.
    """
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
        "extendedProperties": {
            "private": (e.get("extendedProperties") or {}).get("private") or {}
        },
        "attendees": None,
    }

    # Normalise attendees to sorted list of lower-cased emails (ignoring RSVP status)
    if e.get("attendees"):
        addrs = sorted({(a.get("email") or "").lower()
                        for a in e["attendees"] if a.get("email")})
        v["attendees"] = [{"email": a} for a in addrs] if addrs else None

    return v

def _norm_body_view(b: dict) -> dict:
    """
    Apply the same normalisation to a desired event body as we do to a live event.
    This lets us compare apples to apples.
    """
    return _norm_event_view(b)

def _diff_for_patch(current_view: dict, desired_view: dict) -> dict:
    """
    Compare normalised current vs desired event views and return a patch dict
    containing only the fields that differ.  Returns {} if nothing changed.
    """
    patch = {}

    # Simple top-level scalar fields
    for key in ("summary", "description", "location", "colorId", "reminders"):
        if current_view.get(key) != desired_view.get(key):
            patch[key] = desired_view.get(key)

    # Start/end objects
    for key in ("start", "end"):
        if (current_view.get(key) or {}) != (desired_view.get(key) or {}):
            patch[key] = desired_view.get(key)

    # Extended private properties
    cur_priv = (current_view.get("extendedProperties") or {}).get("private")
    des_priv = (desired_view.get("extendedProperties") or {}).get("private")
    if cur_priv != des_priv:
        patch["extendedProperties"] = {"private": des_priv or {}}

    # Attendees (normalised list of {email})
    if current_view.get("attendees") != desired_view.get("attendees"):
        patch["attendees"] = desired_view.get("attendees")

    # Avoid sending nulls unless you explicitly want to clear those fields
    return {k: v for k, v in patch.items() if v is not None}



# =========================
# Upsert by key (no custom event.id on insert)
# =========================
def upsert_event_by_key(
    svc,
    calendar_id: str,
    event_key: str,
    summary: str,
    description: str,
    start_dt: datetime,
    end_dt: datetime,
    tzid: str,
    location: Optional[str],
    reminders: Optional[Dict],
    private_props: Dict[str, str],
    color_id: Optional[str],
):
    """
    - Looks up any existing event via privateExtendedProperty 'event_key'.
    - If found → PATCH (preserves attendees/RSVPs and other untouched fields).
    - If not found → INSERT (let Google assign event.id).
    """

    # Always ensure our key is present
    private_props = dict(private_props or {})
    private_props["event_key"] = event_key

    # Optional narrow search: same day window around start
    #day_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = (_to_datetime(start_dt) + timedelta(days=1)) - timedelta(microseconds=1)
    existing = find_event_by_key(svc, calendar_id, event_key, (start_dt, day_end))

    body = {
        "summary": summary,
        "description": description or "",
        "start": {"dateTime": _to_datetime(start_dt).isoformat(), "timeZone": tzid},
        "end":   {"dateTime": _to_datetime(end_dt).isoformat(),   "timeZone": tzid},
        "extendedProperties": {"private": private_props},
    }
    if location:
        body["location"] = location
    if reminders is not None:
        body["reminders"] = reminders
    if color_id:
        body["colorId"] = color_id

    if existing:
        eid = existing["id"]
        current_view = _norm_event_view(existing)
        desired_view = _norm_body_view(body)
        patch = _diff_for_patch(current_view, desired_view)

        if patch:
            print(f"Patching {start_dt} {summary}")
            res = svc.events().patch(
                calendarId=calendar_id,
                eventId=eid,
                body=patch,            # only send changed fields
                sendUpdates="none"     # or "all" if you want attendee emails
            ).execute()
            return {
                "calendar_id": calendar_id,
                "event_id": eid,
                "htmlLink": res.get("htmlLink"),
                "mode": "patch"
            }
        else:
            print(f"No changes for {start_dt} {summary}")
            return {
                "calendar_id": calendar_id,
                "event_id": eid,
                "htmlLink": existing.get("htmlLink"),
                "mode": "unchanged"
            }
    else:
        print (f"Inserting {start_dt} {summary}")
        res = svc.events().insert(calendarId=calendar_id, body=body).execute()
        return {"calendar_id": calendar_id, "event_id": res.get("id"), "htmlLink": res.get("htmlLink"), "mode": "insert"}

def push_calendarevent_by_tags(svc, cfg: Dict, calendar_event: Dict, tags: List[Dict]):

    tzid = cfg.get("TIMEZONE")
    defaults = cfg.get("event_defaults", {}) or {}

    tag_names   = tags
    calendars   = _resolve_calendars_for_tags(tag_names, cfg)
    code        = str(calendar_event["code"])
    title       = calendar_event.get("title")
    description = calendar_event.get("description") or ""
    location    = calendar_event.get("location") or defaults.get("location")
    reminders   = defaults.get("reminders")
    start_dt    = calendar_event["start"]
    end_dt      = calendar_event["end"]
    booked      = int(calendar_event.get("booked", 0))
    capacity    = calendar_event.get("capacity")
    capacity    = int(capacity) if capacity is not None else None
    color_id    = _choose_color_id(booked, capacity)

    results = []
    for cal_id in calendars:
        event_id = code
        props = {
            "source": "checkfront-sync",
            "booking_code": code.replace('-','_'),
            "tags": ",".join(tag_names),
            "booked": str(booked),
            "capacity": "" if capacity is None else str(capacity),
        }
        res = upsert_event_by_key(
            svc, cal_id, event_id, title, description,
            start_dt, end_dt, tzid,
            location, reminders, props, color_id
        )
        results.append({"calendar_id": cal_id, "event_id": event_id, "htmlLink": res.get("htmlLink")})
    return results
