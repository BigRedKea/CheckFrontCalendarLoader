from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import hashlib
import base64
from typing import List, Dict

def _to_dt(v) -> Optional[datetime]:
    if isinstance(v, datetime): return v
    if isinstance(v, str) and v: return datetime.fromisoformat(v)
    return None

def _find_calendar_def(cfg: dict, calendar_id: str) -> dict:
    for cal in cfg.get("calendars", []):
        if cal.get("calendarid") == calendar_id:
            return cal
    return {}

def slots_to_calendar_events_for(
    calendar_id: str,
    slots: List[Dict[str, Any]],
    cfg: Dict,
    tz,
) -> List[Tuple[str, Dict]]:
    """
    Convert flat slot dicts to (event_id, desired_body) tuples for one calendar,
    always including a stable 'event_key' in extendedProperties.private.
    """
    calendarevents: List[Tuple[str, Dict]] = []
    tzid = cfg.get("timezone") or "Australia/Brisbane"
    defaults = cfg.get("event_defaults", {})
    cal_def = _find_calendar_def(cfg, calendar_id)

    cal_attendees = cal_def.get("attendees")

    for slot in slots:
        
        try:
            
            # only push slots whose tags map to this calendar
            tag_names = list(slot.get("tags") or [])

            item= slot.get("item")

            start_dt = _to_dt(slot["start"])
            sku      = slot.get("sku") or ""
            event_key = eid_readable(sku, start_dt)  # or eid_from_sku_datetime(sku, start_dt)
            #event_id  = eid(event_key)   

            cal_ids = resolve_calendars_for_tags(tag_names, cfg)
            if calendar_id not in cal_ids:
                continue

            # start/end times
            start_dt = _to_dt(slot.get("start"))
            end_dt   = _to_dt(slot.get("end"))
            if not (start_dt and end_dt):
                raise ValueError(f"Missing start/end for slot {slot.get('code')} on {slot.get('date')}")

            # e.g. base32 hash safe for Google IDs

            # colour by availability
            total   = slot.get("total_places")
            booked  = int(slot.get("total_booked") or 0)
            unlimited = bool(slot.get("unlimited"))
            capacity  = None if unlimited else (int(total) if total is not None else None)

            if booked <= 0:
                color_id = "2"   # green
            elif not unlimited and booked >= capacity:
                color_id = "11"  # red
            else:
                color_id = "5"   # banana
                #color_id = "6"   # orange

            # build description
            description = (
                slot.get("description")
                or f"available {('∞' if unlimited else capacity - booked)} = total {total} - booked {booked}"
            )

            private_props = {
                "source": "checkfront-sync",
                "booking_code": str(slot.get("code")),
                "event_key": event_key,
                "date": slot.get("date"),
                "sku": slot.get("sku"),
                "tags": ",".join(tag_names),
                "booked": str(booked),
                "capacity": "" if capacity is None else str(capacity),
            }

            body = {
                "summary": slot.get("title") or (slot.get("item") or {}).get("name") or slot.get("sku") or "Booking",
                "description": description,
                "start": {"dateTime": start_dt.isoformat(), "timeZone": tzid},
                "end":   {"dateTime": end_dt.isoformat(),   "timeZone": tzid},
                "extendedProperties": {"private": private_props},   # <-- ensures key is present
            }

            if cal_attendees:
                body["attendees"] = [{"email": a} for a in cal_attendees]
            if color_id:
                body["colorId"] = color_id
            if defaults.get("reminders"):
                body["reminders"] = defaults["reminders"]
            if slot.get("location"):
                body["location"] = slot.get("location")

            calendarevents.append((event_key, body))

        except Exception as e:
            # Handle any other unspecific exception
            print(f"An unexpected error occurred: {slot.get("sku")} {e}")

    return calendarevents


def eid_readable(sku: str, start: datetime) -> str:
    """
    Return a human-readable event id like 'sku123_2025_09_07_08_00'.
    (Must still be at least 5 chars and only use [a-z0-9_-].)
    """
    safe_sku = (sku or "nosku").lower().replace(" ", "_")
    return f"{safe_sku}_{start.strftime('%Y_%m_%d_%H_%M')}"

def eid(key: str, maxlen: int = 50) -> str:
    # Deterministic base32 id, always safe
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    return base64.b32encode(digest).decode("utf-8").lower().strip("=")[:maxlen]

def resolve_calendars_for_tags(tag_names: List[str], cfg: Dict) -> List[str]:
    """
    Find all calendar IDs whose tag list overlaps with the given tags.

    cfg example:
    {
      "calendars": [
        {"name":"joey calendar",
         "calendarid":"<id>",
         "tags": ["Joey"]},
        ...
      ],
      "default_calendar_id": "primary"   # optional fallback
    }
    """
    wanted = {t.lower() for t in (tag_names or [])}
    matches: List[str] = []

    for cal in cfg.get("calendars", []):
        cal_tags = {t.lower() for t in cal.get("tags", [])}
        if wanted & cal_tags:                      # any overlap
            matches.append(cal["calendarid"])

    if not matches and cfg.get("default_calendar_id"):
        matches.append(cfg["default_calendar_id"])

    return matches


