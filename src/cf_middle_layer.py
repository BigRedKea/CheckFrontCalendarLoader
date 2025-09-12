# src/middle_layer.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict
import json
from datetime import timedelta

from .cf_client import CheckfrontClient
from .helpers import _datetime_or_none, _normalize_value, _normalize

with open("config.json", "r") as f:
    CONFIG = json.load(f)

# ---------- Data Model ----------

@dataclass(frozen=True)
class Customer:
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    group: Optional[str] = None  # e.g., "Scout Group"

@dataclass
class SlotAggregate:
    """A calendar 'slot' aggregate per (SKU, start_date)."""
    sku: str
    start_date: date
    start: datetime
    end: datetime
    total_places: Optional[int] = None
    unlimited: bool = False
    color_id: Optional[str] = None
    item: Optional[dict] = None

    booking_items: list[dict] = field(default_factory=list)
    customers: Dict[str, Customer] = field(default_factory=dict)
    item_event: list[dict] = field(default_factory=list)

# ---------- small Helpers for item events ----------


# def _event_duration(ev: Dict, tz: ZoneInfo) -> timedelta:
#     """Duration = base end - base start (fall back to 3h)."""
#     s = _datetime_or_none(ev.get("start_date"),tz)
#     e = _datetime_or_none(ev.get("end_date"),tz)
#     if s is not None and e is not None and e > s:
#         return e - s
#     return timedelta(hours=3)

def _event_applies_to_ids(ev: Dict) -> list[str]:
    """
    Return list of item IDs this event applies to.
    """
    appliestoids = list()

    applyto = ev.get("apply_to")

    if not applyto:
        return []
    applytoitems = applyto.get("item_id")
    if not applytoitems:
        return []
    appliestoids = list(str(x) for x in applytoitems if x is not None)

    # dedupe + drop blanks
    return sorted({i for i in appliestoids if i}) #, sorted({i for i in appliestocategoryids if i})

def _event_applies_to_categories(ev: Dict) -> list[str]:
    """
    Return list of item IDs this event applies to.
    """
    appliestocategoryids = list()

    applyto = ev.get("apply_to")
    if not applyto:
        return []
        
    applytocategories = (applyto.get("category_id"))
    if not applytocategories:
        return []
    appliestocategoryids = list(str(x) for x in applytocategories if x is not None)
    if not appliestocategoryids:
        return []
    # dedupe + drop blanks
    return sorted({i for i in appliestocategoryids if i})


def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return not (a_end <= b_start or b_end <= a_start)

# ---------------- Item-level recurrence pre-seed (no item events) ----------------

RFC5545_DAYS = ["mon","tue","wed","thu","fri","sat","sun"]

# def _parse_hhmm(s: str, default=(8,0)) -> tuple[int,int]:
#     if not s or not isinstance(s, str):
#         return default
#     try:
#         hh, mm = s.split(":")
#         return int(hh), int(mm)
#     except Exception:
#         return default

def _item_occurrences(item: dict, tz: ZoneInfo, window_start: datetime, window_end: datetime) -> list[tuple[datetime, datetime]]:
    #Build occurrences for an item based on item-level recurrence fields.
    out: list[tuple[datetime, datetime]] = []

    # Base anchor date
    base_s = item.get("start_date")
    s = _datetime_or_none(base_s, tz) if base_s else window_start
    base_e = item.get("end_date")
    itemend = _datetime_or_none(base_e, tz) 

    reps = (item.get("repeat"))
    if not reps:
        out.append((s, itemend))
        return out

    # align first occurrence per weekday
    for wd in reps:
        if wd not in RFC5545_DAYS:
            continue
        target_idx = RFC5545_DAYS.index(wd)
        # start on the first matching weekday >= window_start
        first_day = s + timedelta(days=(target_idx - s.weekday()) % 7)
        # set time window for that day
        first = first_day #.replace(hour=sh, minute=sm, second=0, microsecond=0)
        if first < window_start:
            # jump forward in steps of 'interval' weeks
            delta_days = (window_start - first).days
            jumps = (delta_days // (7 )) * (7 )
            first = first + timedelta(days=jumps)
            while first < window_start:
                first += timedelta(weeks=1)
        cur = first
        while cur < window_end:
            end = cur
            out.append((cur, end))
            cur += timedelta(weeks=1)

    out.sort(key=lambda se: se[0])
    return out

def _item_daily_occurrences(window_start: datetime, window_end: datetime) -> list[tuple[datetime, datetime]]:
    #Build daily occurrences for an item.
    out: list[tuple[datetime, datetime]] = []

    cur = window_start
    while cur < window_end:
        end = cur
        out.append((cur, end))
        cur += timedelta(days=1)

    out.sort(key=lambda se: se[0])
    return out

def build_buckets(
    *,
    cf: CheckfrontClient,
    tz: ZoneInfo,
    window_start: datetime,
    window_end: datetime,
    items_by_id: Dict[str, Dict]
    ) -> List[SlotAggregate]:
    """
    Build slots in three passes:
      1) Events: generate availability slots (exclude 'U' unavailable overlaps)
      2) Bookings: overlay bookings into slots (create new slots if necessary)
      3) Customers: fetch and attach customer info only for IDs referenced by bookings
    """

    item_events = cf.list_item_events()
    available_events = [e for e in item_events if e.get("enabled") and e.get("status") != "U"]
    unavailable_events = [e for e in item_events if e.get("enabled") and e.get("status") == "U"]

    # Map unavailable windows by item-id for fast checks
    unavail_by_item: Dict[str, List[Tuple[datetime, datetime]]] = {}
    unavail_by_category: Dict[str, List[Tuple[datetime, datetime]]] = {}

    for u in unavailable_events:
        u_start_s = _datetime_or_none(u.get("start_date"),tz)
        u_end_s = _datetime_or_none(u.get("end_date"),tz)
        u_end_s =u_end_s.replace(hour=23, minute=59, second=59, microsecond=9999)

        if u_start_s is None:
            continue

        # Treat missing/zero end as open-ended; cap at our window_end
        if not u_end_s or u_end_s == 0:
            u_end_s = int(window_end.timestamp())

        u_end_s =u_end_s.replace(hour=23, minute=59, second=59, microsecond=9999)              

        for applies_to_item_id in _event_applies_to_ids(u):
            unavail_by_item.setdefault(applies_to_item_id, []).append((u_start_s, u_end_s))

        for applies_to_item_id in _event_applies_to_categories(u):
            unavail_by_category.setdefault(applies_to_item_id, []).append((u_start_s, u_end_s))


    # Create slots from AVAILABLE item events, excluding overlaps with "U"
    buckets: Dict[Tuple[str, date], SlotAggregate] = {}

    # Which item_ids had *any* event reference (available or unavailable)
    events_seen_item_ids: set[str] = set()

    for ev in available_events + unavailable_events:
        for applies_to_item_id in _event_applies_to_ids(ev):
            events_seen_item_ids.add(str(applies_to_item_id))

    for available_event in available_events :
        u_start_s = _datetime_or_none(available_event.get("start_date"),tz)
        u_end_s = _datetime_or_none(available_event.get("end_date"),tz)

        if (available_event.get("start_date") =="0"):
            continue

        if (available_event.get("end_date")!="0"):
            if (u_end_s < window_start):
                continue

        unlimited = bool(available_event.get("unlimited") == 1)

        # expand occurrences from item-level repeat
        occs = _item_occurrences(available_event, tz, window_start, window_end)

        applies_to_item_ids =_event_applies_to_ids(available_event)

        for applies_to_item_id in applies_to_item_ids:
            # apply unavailability (from item events with status 'U') to this item

            item = items_by_id.get(applies_to_item_id)

            if item ==None:
                continue # May be an archived Item

            itemcategory = item.get("category")

            notavailableitem = unavail_by_item.get(applies_to_item_id, [])
            notavailablecategory = unavail_by_category.get(itemcategory, [])

            for (s, e) in occs:
                if any(_overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailableitem):
                    continue
                if any(_overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailablecategory):
                    continue
  
                sku = item.get("sku")
                total_places = item.get("stock")
                key = (sku, s.date())
                if key not in buckets:
                    buckets[key] = SlotAggregate(
                         sku=sku,
                         start_date=s.date(),
                         start=s,
                         end=e,
                         total_places=int(total_places) if total_places is not None else None,
                         unlimited=unlimited,
                         item= item
                     )
                    
        available_items_by_id: Dict[str, Dict] = {
            k: v for k, v in items_by_id.items() 
            if v.get("status") != "U" and v.get("unlimited") ==0 and v.get("visibility") =="*"}
                     
        for applies_to_item_id in available_items_by_id:

            item = items_by_id.get(applies_to_item_id)
            if item ==None:
                continue # May be an archived Item

            occs = _item_daily_occurrences(window_start, window_end)
            # apply unavailability (from item events with status 'U') to this item
            
            notavailableitem = unavail_by_item.get(applies_to_item_id, [])
            notavailablecategory = unavail_by_category.get(itemcategory, [])

            for (s, e) in occs:
                if any(_overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailableitem):
                    continue
                if any(_overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailablecategory):
                    continue

                sku = item.get("sku")
                total_places = item.get("stock")
                key = (sku, s.date())
                if key not in buckets:
                    buckets[key] = SlotAggregate(
                         sku=sku,
                         start_date=s.date(),
                         start=s,
                         end=e,
                         total_places=int(total_places) if total_places is not None else None,
                         unlimited=unlimited,
                         item= item
                     )
    return buckets

# def _flatten_tags(tags: List[Dict]) -> List[str]:
#     """Accept [{'name':'Cub'}, ...] or ['Cub', ...] and return ['Cub', ...]."""
#     out: List[str] = []
#     for t in tags or []:
#         if isinstance(t, dict) and isinstance(t.get("name"), str):
#             out.append(t["name"].strip())
#         elif isinstance(t, str):
#             out.append(t.strip())
#     return out

def extract_checkfront_data(
    *,
    cf: CheckfrontClient,
    tz: ZoneInfo,
    start_date_str: Optional[str],
    days: int
    ) -> List[SlotAggregate]:

    # Window
    sd = start_date_str or date.today().isoformat()
    window_start = datetime.fromisoformat(sd).replace(tzinfo=tz)
    window_end = window_start + timedelta(days=days)

    # ---------- 1) EVENTS ----------
    items_by_id: Dict[str, Dict] = {str(i.get("item_id")): i for i in list(cf.list_items())}
    items_by_sku: Dict[str, Dict] = {str(i.get("sku")): i for i in list(cf.list_items())}

    buckets = build_buckets( 
        cf=cf,
        tz=tz,
        window_start= window_start,
        window_end=window_end,
        items_by_id=items_by_id)

    # ---------- BOOKINGS ----------
    bookings = list(cf.list_bookings_index(
        start_date=window_start,
        end_date=window_end 
    ))

    for booking in bookings:
        booking_id = str(booking.get("booking_id"))
        customer_id = booking.get("customer_id")
        customer = cf.get_customer(str(customer_id)) if customer_id else None

        booking_detail = cf.get_booking(booking_id)
        booking_items  = booking_detail.get("items") or {}

        for bookinglineid, booking_item in booking_items.items():
            if not isinstance(booking_item, dict):
                continue

            if booking_item.get('status_id') == 'VOID':
                continue

            sku = booking_item.get("sku").strip()
            qty = int(booking_item.get("qty") )
            if not sku or qty <= 0:
                continue

            bookingstartdatetime = booking_item.get("start_date")
            bookingenddatetime = booking_item.get("end_date")
            if bookingstartdatetime is None or bookingenddatetime is None:
                continue

            booking_start_iso = datetime.fromtimestamp(int(bookingstartdatetime), tz=tz)
            booking_end_iso   = datetime.fromtimestamp(int(bookingenddatetime), tz=tz)
            if booking_end_iso <= window_start or booking_start_iso >= window_end:
                continue

            occs = _item_daily_occurrences(booking_start_iso, booking_end_iso)

            for (occurance_start, occurance_end) in occs:
            
                key = (sku, occurance_start.date())

                if key not in buckets:
                    print(f"Adding {key} to new bucket")
                    item = items_by_sku[sku]
                    buckets[key] = SlotAggregate(
                        sku=sku,
                        start_date=occurance_start.date(),
                        start=occurance_start,
                        end=occurance_end,
                        item= item
                    )
                slot = buckets[key]

                flat_booking = {
                    "booking_id": booking_id,
                    "customer_id": str(customer_id) if customer_id else None,
                    "line_id": str(bookinglineid),
                    "sku": sku,
                    "qty": qty,
                    "start": occurance_start,
                    "end": occurance_end
                }

                if isinstance(booking_item, dict):
                    for k, v in booking_item.items():
                        if k not in flat_booking:
                            flat_booking[k] = _normalize_value(v)

                slot.booking_items.append(flat_booking)
                
                # track customer for this slot (once per customer id)
                if customer:
                    cid = customer.get("id") or str(customer_id)
                    if cid:
                        slot.customers[cid] = customer

    return  slots_to_json_ready(buckets)


def slots_to_json_ready(slots):
    """
    Input:
      slots: dict keyed by (sku, date) -> slot object
    Output:
      flat: list of normalized slot dicts (one per (sku, date)), sorted by start
    """
    flat = []

    for (sku, d), slot in slots.items():
        bookings = []
        param_totals: dict[str, int] = {}
        group_totals: dict[str, dict] = {}

        total_places = slot.item.get("stock")
        unlimited = bool(slot.item.get("unlimited") == 1)
        total_booked = 0

        for bi in (slot.booking_items or []):
            row = {**bi}
            row["customer_id"] = bi.get("customer_id")
            bookings.append(_normalize(row))

            # aggregate param totals
            params = bi.get("param") or {}
            for key, p in params.items():
                qty = int(p.get("qty") or 0)
                param_totals[key] = param_totals.get(key, 0) + qty

            # quantity
            qty = int(bi.get("qty") or 0)
            total_booked += qty

            # customer & meta
            cid = bi.get("customer_id")
            cust = (slot.customers or {}).get(cid) if cid else None
            meta = (cust or {}).get("meta") or {}

            grp = meta.get("scout_group_booking") or "Unknown"
            email = meta.get("your_leaders_email_address") or None

            if grp not in group_totals:
                group_totals[grp] = {"total_qty": 0, "emails": set()}
            group_totals[grp]["total_qty"] += qty
            if email:
                group_totals[grp]["emails"].add(email)

        # convert email sets → lists
        for grp, data in group_totals.items():
            data["emails"] = sorted(list(data["emails"]))

        customers = slot.customers if getattr(slot, "customers", None) else {}

        # robust tag flattening (accept dicts or strings)
        raw_tags = (slot.item or {}).get("tags") or []
        flattened_tags = []
        for t in raw_tags:
            if isinstance(t, dict) and "name" in t and isinstance(t["name"], str):
                flattened_tags.append(t["name"])
            elif isinstance(t, str):
                flattened_tags.append(t)

        # ensure times are set
        apply_time_rule(slot)
        start_iso = slot.start.isoformat() if slot.start else None
        end_iso   = slot.end.isoformat() if slot.end else None

        # available places (respect unlimited)
        available_places = None if unlimited else (int(total_places or 0) - int(total_booked or 0))

        slot_dict = {
            "sku": sku,
            "date": str(d),
            "start": start_iso,
            "end": end_iso,
            "unlimited": unlimited,
            "total_places": total_places,
            "total_booked": total_booked,
            "available_places": available_places,
            "param_totals": param_totals,
            "group_totals": group_totals,
            "tags": flattened_tags,
            "item": slot.item,
            "bookings": bookings,
            "customers": _normalize(customers),
        }

        flat.append(_normalize(slot_dict))

    # sort the flat list chronologically by start
    flat.sort(key=lambda s: (s.get("start") or ""))

    return flat

# def slots_to_json_ready(slots):
#     grouped = defaultdict(list)

#     #for each slot
#     for (sku, d), slot in slots.items():
#         # build booking rows (each keeps its param, just add customer_id)
#         bookings = []
#         param_totals: dict[str, int] = {}
#         group_totals: dict[str, dict] = {}

#         total_places = slot.item.get("stock") 
#         unlimited = bool(slot.item.get("unlimited") == 1) 
#         total_booked = 0

#         for bi in slot.booking_items or []:
#             row = {**bi}  # copy
#             # ensure customer_id is present
#             row["customer_id"] = bi.get("customer_id")
#             bookings.append(_normalize(row))

#             # aggregate param totals across all booking_items
#             params = bi.get("param") or {}
#             for key, p in params.items():
#                 qty = int(p.get("qty") or 0)
#                 param_totals[key] = param_totals.get(key, 0) + qty

#             # get the quantuty
#             qty = int(bi.get("qty"))

#             total_booked += qty

#             # get the customer
#             cid = bi.get("customer_id")
#             cust = (slot.customers or {}).get(cid) if cid else None

#             # Pull the scout group from Meta Data
#             meta = (cust or {}).get("meta")
#             grp = (
#                 meta.get("scout_group_booking")
#                 or "Unknown"
#             )
#             email = (
#                 meta.get("your_leaders_email_address") or None
#             )

#             #group by the scoutgroup and included supplied leaders email addresses
#             if grp not in group_totals:
#                 group_totals[grp] = {"total_qty": 0, "emails": set()}

#             group_totals[grp]["total_qty"] += qty

#             #add scoutleaders email
#             if email:
#                 if not isinstance(group_totals[grp]["emails"], set):
#                     group_totals[grp]["emails"] = set(group_totals[grp]["emails"])
#                 group_totals[grp]["emails"].add(email)

#         # convert email sets → lists
#         for grp, data in group_totals.items():
#             data["emails"] = sorted(list(data["emails"]))

#         # Customers as a dictionary keyed by customer_id
#         customers = slot.customers if getattr(slot, "customers", None) else {}

#         flattened_tags = [t["name"] for t in slot.item.get("tags")]

#         apply_time_rule(slot)

#         slot_dict = {
#             "sku": sku,
#             "date": str(d),
#             "start": slot.start.isoformat() if slot.start else None,
#             "end": slot.end.isoformat() if slot.end else None,
#             "unlimited": unlimited,
#             "total_places": total_places,
#             "total_booked": total_booked,
#             "available_places": total_places - total_booked,
#             "param_totals": param_totals,
#             "group_totals": group_totals,
#             "tags": flattened_tags,
#             "item": slot.item,
#             "bookings": bookings,  
#             "customers": _normalize(customers),  
#         }

#         grouped[str(d)].append(_normalize(slot_dict))

#     #Return a new dict with keys (YYYY-MM-DD) sorted chronologically.
#     return {
#         k: grouped[k]
#         for k in sorted(grouped.keys(), key=lambda d: date.fromisoformat(d))
#     }




def _apply_times(dt, h, m):
    return dt.replace(hour=h, minute=m, second=0, microsecond=0)

def _get_rule_for(slot):
    item = getattr(slot, "item", {}) or {}
    category = (item.get("category") or "").strip()
    sku = (item.get("sku") or "").strip()

    rules = CONFIG["time_rules"]
    # SKU override wins if present (exact, case-insensitive)
    if sku:
        for key, rule in rules["sku_overrides"].items():
            if key.lower() == sku.lower():
                return rule

    # Otherwise fall back to category default
    return rules["default_by_category"].get(category)

def apply_time_rule(slot):
    """
    slot.start and slot.end must be datetime objects.
    slot.item must have at least: {"category": "...", "sku": "..."} (sku optional).
    """
    rule = _get_rule_for(slot)
    if not rule:
        return slot  # no change if nothing matches

    slot.start = _apply_times(slot.start, rule["start_hour"], rule.get("start_minute", 0))
    slot.end   = _apply_times(slot.end,   rule["end_hour"],   rule.get("end_minute", 0))

    # Handle overnight (end next day)
    if rule.get("overnight"):
        if slot.end <= slot.start:
            slot.end += timedelta(days=1)
    else:
        # If a same-day rule accidentally crosses midnight, normalize
        if slot.end <= slot.start:
            slot.end += timedelta(days=1)

    return slot