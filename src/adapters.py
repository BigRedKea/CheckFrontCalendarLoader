from typing import Dict, List, Tuple
from datetime import datetime
from .gcal_sync import push_booking_by_tags
from .cf_middle_layer import SlotAggregate
from .helpers import _to_datetime

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

    booking = {
        "code": f"{slot.get('sku')}_{_to_datetime(start).strftime("%Y_%M_%d_%H_%m")}",
        "title": slot.get("item", {}).get("name"),
        "start": start,
        "end": end,
        "location": "", #slot.get("location"),
        "description": "",#slot.get("notes"),
        "sku": slot.get("sku"),
        "booked": int(slot.get("total_booked")),
        "capacity": int(slot.get("total_places"))
    }

    # Normalize tags to [{'name': '...'}]
    raw_tags = slot.get("tags", [])
    #tags = [{"name": t if isinstance(t, str) else t["name"]} for t in raw_tags]

    return booking, raw_tags


def push_slots_to_calendars(svc, cfg: Dict, slots: Dict[datetime, List[Dict]], tz) -> List[Dict]:
    """Push all slots to calendars using calendar-centric config."""
    results: List[Dict] = []

    for dt, slot in slots.items():
        for item in slot:
            booking, tags = slot_to_booking_and_tags(item, tz)
            if booking == None:
                continue
            results.extend(push_booking_by_tags(svc, cfg, booking, tags))
    return results