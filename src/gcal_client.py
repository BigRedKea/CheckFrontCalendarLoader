# =============================
# src/gcal_client.py
# =============================
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
import re, hashlib, base64
from collections import defaultdict
from typing import Callable

from src.calendarevent import ExtractWindow

from .helpers import _to_datetime, _to_datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


SAFE_RE = re.compile(r"[^a-z0-9_-]")
SCOPES = ["https://www.googleapis.com/auth/calendar"]


class GCalClient:
    def __init__(self, sa_json_path: str, calendar_id: str):
        creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
        self.service = build("calendar", "v3", credentials=creds)
        self.calendar_id = calendar_id

    # def get_calendar_service(sa_json_path: str):
    #     creds = Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    #     return build("calendar", "v3", credentials=creds)

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

    def fetch_existing_synced_events(self, calendar_id: str, extract_window: ExtractWindow) -> Dict[str, dict]:
        """Return {eventId: normalized_event} for events we own (source=checkfront-sync) in [time_min, time_max]."""
        events_by_id: Dict[str, dict] = {}
        page_token = None
        while True:
            req = self.events().list(
                calendarId=calendar_id,
                timeMin=extract_window.window_start.isoformat(),
                timeMax=extract_window.window_end.isoformat(),
                privateExtendedProperty="source=checkfront-sync",
                singleEvents=True,
                showDeleted=False,
                maxResults=2500,
                pageToken=page_token,
            )
            resp = req.execute()
            for ev in resp.get("items", []):
                events_by_id[ev["id"]] = _norm_event_view(ev, extract_window.tz)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return events_by_id
    
    def fetch_existing_by_key(self, calendar_id: str, tmin, tmax) -> dict[str, dict]:
        """Return {event_key: full_event} for all events we manage (source=checkfront-sync) in the window."""
        out, token = {}, None
        while True:
            resp = self.service.events().list(
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
    
    def sync_calendar(
        self,
        calendar_id: str,
        bookings_for_cal: list[tuple[str, dict]],  # [(event_key, desired_body)], desired_body includes extendedProperties.private.event_key
        extract_window: ExtractWindow,
        send_updates: str = "none",
        delete_orphans: bool = True) -> list:
        
        filtered_bookings: List[Tuple[str, dict]] = []
        for key, body in bookings_for_cal:
            start_dt = body.get("start", {}).get("dateTime")
            end_dt   = body.get("end", {}).get("dateTime")
            if not (start_dt and end_dt):
                continue
            start = datetime.fromisoformat(start_dt)
            end   = datetime.fromisoformat(end_dt)
            # keep if event overlaps [time_min, time_max]
            if end >= extract_window.window_start and start <= extract_window.window_end:
                filtered_bookings.append((key, body))

        # --- sort filtered bookings deterministically ---
        filtered_bookings.sort(key=lambda kv: (_start_of_event(kv[1]), kv[0]))
        
        # Map existing events we manage by their event_key
        existing = self.fetch_existing_by_key(calendar_id, extract_window.window_start, extract_window.window_end)

        inserted = patched = unchanged = deleted = 0
        results = []

        # Upsert pass
        active_keys = set()
        for event_key, desired in filtered_bookings:
            active_keys.add(event_key)
            current = existing.get(event_key)
            desired_view = _norm_event_view(desired)

            if current is None:
                # INSERT (no custom 'id'; rely on event_key in extendedProperties)
                res = self.service.events().insert(
                    calendarId=calendar_id, body=desired, sendUpdates=send_updates
                ).execute()
                inserted += 1
                print(f"Inserted {event_key} ");
                results.append({"event_key": event_key, "status": "inserted", "htmlLink": res.get("htmlLink")})
            else:

                patch = _diff_for_patch(_norm_event_view(current), desired_view)


                if patch:
                    if (False):
                        res = self.service.events().update(
                            calendarId=calendar_id,
                            eventId=current["id"],
                            body=desired_view,              # the full resource with our merged changes
                            sendUpdates=send_updates       # or "all"/"externalOnly" as needed
                        ).execute()
                        print(f"updated {desired_view.get("summary")} {desired_view.get("start")} ")
                        results.append({"event_key": event_key, "status": "updated", "htmlLink": res.get("htmlLink")})
                    else:
                        try:
                            res = self.service.events().patch(
                                calendarId=calendar_id, eventId=current["id"], body=patch, sendUpdates=send_updates
                            ).execute()
                            print(f"Patched {desired_view.get("summary")} {desired_view.get("start")} ")
                            results.append({"event_key": event_key, "status": "patched", "htmlLink": res.get("htmlLink")})
                        except Exception as e: # Catching a general exception as a fallback
                            print(f"An unexpected error occurred: {e}")

                    patched += 1
                    
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
                    self.service.events().delete(
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
    
    def delete_duplicate_events(self,
        calendar_id: str,
        events: list[dict],
        key_func: Callable[[dict], str],) -> list[str]:
        """
        Delete duplicate events in a list based on key_func(event).
        Keeps the most recently updated event per key.

        Returns a list of deleted event IDs.
        """
        buckets: dict[str, list[dict]] = defaultdict(list)
        for ev in events:
            k = key_func(ev)
            if k:
                buckets[k].append(ev)

        deleted_ids: list[str] = []

        for k, evs in buckets.items():
            if len(evs) > 1:
                # Keep the one with the latest 'updated' timestamp
                evs.sort(key=lambda e: e.get("updated", ""))
                survivor = evs[-1]
                for ev in evs[:-1]:
                    self.service.events().delete(calendarId=calendar_id, eventId=ev["id"]).execute()
                    deleted_ids.append(ev["id"])

        return deleted_ids
    
    def delete_all_events(self,
        calendar_id: str,
        window_start:datetime, 
        window_end:datetime):
        """
        Delete duplicate events in a list based on key_func(event).
        Keeps the most recently updated event per key.

        Returns a list of deleted event IDs.
        """
        page_token = None
        while True:
            resp = self.service.events().list(
                calendarId=calendar_id,
                timeMin= window_start.isoformat(),
                timeMax= window_end.isoformat(),
                singleEvents=True,
                showDeleted=False,
                maxResults=2500,
                fields="items(id),nextPageToken",
                pageToken=page_token
            ).execute()

            for e in resp.get("items", []):
                self.service.events().delete(calendarId=calendar_id, eventId=e["id"]).execute()
                print(f"Deleted {e.get("id")}")

            page_token = resp.get("nextPageToken")
            if not page_token:
                break



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


# --- Auth ---


# def _start_of_body(body: dict) -> datetime:
#     return datetime.fromisoformat(body["start"]["dateTime"])

def _start_of_event(ev: dict) -> datetime:
    return datetime.fromisoformat(ev["start"]["dateTime"])

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
    patch: dict = {}

    # Simple top-level fields
    for key in ("summary", "description", "location", "colorId"): #, "reminders"
        if current_view.get(key) != desired_view.get(key):
            patch[key] = desired_view.get(key)

    # Start/end objects
    for key in ("start", "end"):
        if (current_view.get(key) or {}) != (desired_view.get(key) or {}):
            patch[key] = desired_view.get(key)

    # Extended private props (only the ones we manage)
    cur_priv = ((current_view.get("extendedProperties") or {}).get("private") or {})
    des_priv = ((desired_view.get("extendedProperties") or {}).get("private") or {})
    if cur_priv != des_priv:
        patch["extendedProperties"] = {"private": des_priv}

    # Attendees (compare as normalised list of {email})
    if current_view.get("attendees") != desired_view.get("attendees"):
        patch["attendees"] = desired_view.get("attendees")

    # Don’t send None unless you intend to clear a field
    diff = {k: v for k, v in patch.items() if v is not None}
    return diff





def _norm_event_view(e: dict) -> dict:
    """Project an event into just the fields we manage, normalised."""
    if not e: 
        return {}
    v = {
        "summary": (e.get("summary") or "").strip(),
        "description": (e.get("description") or "").rstrip(),
        "location": e.get("location") or None,
        "colorId": e.get("colorId") or None,
        "start": e.get("start"),
        "end": e.get("end"),
        "reminders": e.get("reminders") or None,
        "extendedProperties": {"private": (e.get("extendedProperties") or {}).get("private") or {}},
        "attendees": None,
    }
    if "attendees" in e and e["attendees"]:
        addrs = sorted({(a.get("email") or "").lower() for a in e["attendees"] if a.get("email")})
        v["attendees"] = [{"email": a} for a in addrs] if addrs else None
    return v


