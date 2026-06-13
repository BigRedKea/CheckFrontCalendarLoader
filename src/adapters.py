from typing import Dict, List, Tuple
from datetime import datetime
#from .gcal_sync import push_calendarevent_by_tags
from .helpers import _to_datetime
#from .cf_middle_layer import SlotAggregate
#from .gcal_client import sync_calendar

import json

from datetime import datetime, timedelta, timezone
# from outputs.google_calendar.auth import service
# from outputs.google_calendar.sync_calendar import sync_calendar
# from app.mapping import resolve_calendars_for_tags
# from app.models import Slot


# We assume you already have:
# from yourmodule.time_rules import apply_time_rules_if_missing

def slot_to_booking_and_tags(slot: Dict, tz) -> Tuple[Dict, List[Dict]]:
    """Convert one Checkfront slot into (booking, tags) for gcal_sync."""
    # Use your existing rules only if start/end are missing
    start: datetime = slot.get("start")
    end: datetime = slot.get("end")
    #if not (isinstance(start, datetime) and isinstance(end, datetime)):
    #   start, end = apply_time_rules_if_missing(slot, tz)

    if start == None or end == None:
        return None, None
    description =  f"available {slot.get('available_places')} = total {slot.get('total_places')} - booked {slot.get('total_booked')}" + '\n\n'

    description += json.dumps(slot.get('param_totals'), indent=2)+ '\n\n'
    description += json.dumps(slot.get('group_totals'), indent=2)

    checkfront_event = {
        "code": f"{slot.get('sku')}_{_to_datetime(start).strftime("%Y_%m_%d_%H_%M")}",
        "title": slot.get("item", {}).get("name"),
        "start": start,
        "end": end,
        "location": "", #slot.get("location"),
        "description": description,
        "sku": slot.get("sku"),
        "booked": int(slot.get("total_booked")),
        "capacity": int(slot.get("total_places"))
    }

    # Normalize tags to [{'name': '...'}]
    raw_tags = slot.get("tags", [])
    #tags = [{"name": t if isinstance(t, str) else t["name"]} for t in raw_tags]

    return checkfront_event, raw_tags

from typing import List

def resolve_calendars_for_tags(tag_names: List[str], cfg: dict) -> List[str]:
    """
    Given a list of tag names and the full Google Calendar config,
    return all calendar IDs that should receive an event.

    Parameters
    ----------
    tag_names : List[str]
        Tags attached to a booking/slot.
    cfg : dict
        Google Calendar config (cfg["calendars"], cfg.get("default_calendar_id")).

    Returns
    -------
    List[str]
        All matching calendar IDs.  If nothing matches and a
        default_calendar_id is set, it will return [default_calendar_id].
    """
    tag_set = set(tag_names)
    matches: List[str] = []

    for cal in cfg.get("calendars"):
        # each cal_def is e.g. { "tags": ["Joey","Cub"], ... }
        cal_tags = set(cal.get("tags", []))
        if tag_set & cal_tags: 
            cal_id = cal.get("calendarid")         # any overlap
            matches.append(cal_id)

    return matches

def _tag_names_of(s) -> list[str]:
    """Accept flat dict {'tags': [...]} or Slot with .tags; return ['Joey','Cub',...]."""
    if isinstance(s, dict):
        return list(s.get("tags", []))
    if hasattr(s, "tags"):
        return [getattr(t, "name", str(t)) for t in (s.tags or [])]
    return []

def filter_slots_for_calendar(slots, cal: str, cfg: dict):
    """
    Return only the slots that belong on `cal_id`, based on tags.
    - `slots`: list of flat dicts (from slots_to_json_ready) OR Slot objects
    - `cfg`: google calendar config containing cfg['calendars'][cal_id]['tags']
    """
    cal_id = cal.get("calendarid")
    out = []
    for s in slots:
        tag_names = _tag_names_of(s)
        cal_ids = resolve_calendars_for_tags(tag_names, cfg)  # uses your mapping
        if cal_id in cal_ids:
            out.append(s)
    return out
