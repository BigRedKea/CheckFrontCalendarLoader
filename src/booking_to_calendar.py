from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from typing import List, Dict
from zoneinfo import ZoneInfo

from src.calendarevent import CalendarEvent
from src.helpers import _to_dt

DEFAULT_TZ = "Australia/Brisbane"



def _find_calendar_def(cfg: dict, calendar_id: str) -> dict:
    for cal in cfg.get("calendars", []):
        if cal.get("calendarid") == calendar_id:
            return cal
    return {}

def calendarevents_to_googlecalendar(
    calendar_id: str,
    calendarEvents: list[CalendarEvent],
    cfg: Dict,
    tz,
) -> List[Tuple[str, Dict]]:
    """
    Convert flat slot dicts to (event_id, desired_body) tuples for one calendar,
    always including a stable 'event_key' in extendedProperties.private.
    """
    googleCalendarEvents: List[Tuple[str, Dict]] = []
    defaults = cfg.get("event_defaults", {})
    cal_def = _find_calendar_def(cfg, calendar_id)

    cal_attendees = cal_def.get("attendees")

    for calendarEvent in calendarEvents:
            
        try:

            # only push slots whose tags map to this calendar
            cal_ids = resolve_calendars_for_tags(calendarEvent.tags, cfg)
            if calendar_id not in cal_ids:
                continue

            if not (calendarEvent.startdatetime and calendarEvent.enddatetime):
                raise ValueError(f"Missing start/end for calendarEvent")

            # colour by availability
            total   = calendarEvent.total_places
            booked  = calendarEvent.total_booked()
            capacity  = None if calendarEvent.unlimited else (int(total) if total is not None else None)

            if booked <= 0:
                color_id = "2"   # green
                emoji = "🪫 "
            elif not calendarEvent.unlimited and booked >= capacity:
                color_id = "11"  # red
                emoji = "🔋 "
            else:
                color_id = "5"   # banana
                emoji = "🟨"
                #color_id = "6"   # orange


            # build description
            if calendarEvent.unlimited:
                description = ( f"∞ available - booked {booked}")
            else:
                description = ( f"available {(capacity - booked)} = total {total} - booked {booked}")

            private_props = {
                "source": "checkfront-sync",
                "event_key": calendarEvent.calendar_event_id,
                "sku": calendarEvent.sku,
                "tags": ",".join(calendarEvent.tags),
                "booked": str(booked),
                "capacity": "" if capacity is None else str(capacity),
            }

            body = {
                "summary": '[' + emoji + str(booked) + '] ' + (calendarEvent.checkfrontitem.get("name") or calendarEvent.get("sku")),
                "description": description,
                "start": _to_gcal_time(calendarEvent.startdatetime),
                "end":   _to_gcal_time(calendarEvent.enddatetime),
                "extendedProperties": {"private": private_props},   # <-- ensures key is present
            }

            if cal_attendees:
                body["attendees"] = [{"email": a} for a in cal_attendees]
            if color_id:
                body["colorId"] = color_id
            if defaults.get("reminders"):
                body["reminders"] = defaults["reminders"]
            # if calendarEvent.get("location"):
            #     body["location"] = calendarEvent.get("location")

            googleCalendarEvents.append((calendarEvent.calendar_event_id, body))

        except Exception as e:
            # Handle any other unspecific exception
            print(f"An unexpected error occurred: {calendarEvent.sku} {e}")

    return googleCalendarEvents


def _to_gcal_time(dt: datetime) -> dict:
    """GCal expects {'dateTime': ..., 'timeZone': ...} for timed events."""
    if dt.tzinfo is None:
        tz = DEFAULT_TZ
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    else:
        # ZoneInfo('Australia/Brisbane').key gives the Olson name
        tz = getattr(dt.tzinfo, "key", DEFAULT_TZ)
    return {"dateTime": _rfc3339(dt), "timeZone": tz}


def _rfc3339(dt: datetime) -> str:
    """Return RFC3339 string, ensuring it’s timezone-aware."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(DEFAULT_TZ))
    return dt.isoformat(timespec="seconds")  # e.g. 2025-09-21T08:00:00+10:00


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

    matches: List[str] = []
    if not tag_names:
        return matches

    wanted = {t.lower() for t in (tag_names or [])}
    
    for cal in cfg.get("calendars", []):
        cal_tags = {t.lower() for t in cal.get("tags", [])}
        if wanted & cal_tags:                      # any overlap
            matches.append(cal["calendarid"])

    if not matches and cfg.get("default_calendar_id"):
        matches.append(cfg["default_calendar_id"])

    return matches


