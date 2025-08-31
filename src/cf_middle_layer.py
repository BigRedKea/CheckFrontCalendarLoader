# src/middle_layer.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from .cf_client import CheckfrontClient

# ---------- Data Model ----------

@dataclass(frozen=True)
class Customer:
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    group: Optional[str] = None  # e.g., custom field like "Scout Group"

@dataclass(frozen=True)
class Booking:
    code: str
    start: datetime
    end: datetime
    sku: Optional[str]
    quantity: int
    status_id: Optional[str] = None
    status_name: Optional[str] = None
    customer_id: Optional[str] = None

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
    booking_items: list[dict] = field(default_factory=list)
    customers: Dict[str, Customer] = field(default_factory=dict)
    item_event: list[dict] = field(default_factory=list)


    @property
    def total_booked(self) -> int:
        return sum(int(it.get("qty", 0)) for it in self.booking_items)

# ---------- Helpers for item events ----------

def _datetime_or_none(v, tz) -> datetime:
    try:
        naive_dt = datetime.strptime(v, "%Y%m%d")
        return naive_dt.replace(tzinfo=tz)
    except Exception:
        return None

def _event_duration(ev: Dict, tz: ZoneInfo) -> timedelta:
    """Duration = base end - base start (fall back to 3h)."""
    s = _datetime_or_none(ev.get("start_date"),tz)
    e = _datetime_or_none(ev.get("end_date"),tz)
    if s is not None and e is not None and e > s:
        return e - s
    return timedelta(hours=3)

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

def _parse_hhmm(s: str, default=(8,0)) -> tuple[int,int]:
    if not s or not isinstance(s, str):
        return default
    try:
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except Exception:
        return default

def _item_occurrences(item: dict, tz: ZoneInfo, window_start: datetime, window_end: datetime) -> list[tuple[datetime, datetime]]:
    """
    Build occurrences for an item based on item-level recurrence fields.
    """
    out: list[tuple[datetime, datetime]] = []

    # Base anchor date
    base_s = item.get("start_date")
    base = _datetime_or_none(base_s, tz) if base_s else window_start
    base_e = item.get("end_date")
    itemend = _datetime_or_none(base_e, tz) 

    # Times-of-day
    sh, sm = _parse_hhmm(item.get("time_start"), default=(8,0))
    eh, em = _parse_hhmm(item.get("time_end"),   default=(15,0))

    reps = (item.get("repeat"))
    if not reps:
        first = base.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end = itemend.replace(hour=eh, minute=em, second=0, microsecond=0)
        out.append((first, end))
        return out

    # align first occurrence per weekday
    for wd in reps:
        if wd not in RFC5545_DAYS:
            continue
        target_idx = RFC5545_DAYS.index(wd)
        # start on the first matching weekday >= window_start
        first_day = base + timedelta(days=(target_idx - base.weekday()) % 7)
        # set time window for that day
        first = first_day.replace(hour=sh, minute=sm, second=0, microsecond=0)
        if first < window_start:
            # jump forward in steps of 'interval' weeks
            delta_days = (window_start - first).days
            jumps = (delta_days // (7 )) * (7 )
            first = first + timedelta(days=jumps)
            while first < window_start:
                first += timedelta(weeks=1)
        cur = first
        while cur < window_end:
            end = cur.replace(hour=eh, minute=em, second=0, microsecond=0)
            if end <= cur:  # ensure positive duration
                end = cur + timedelta(hours=1)
            out.append((cur, end))
            cur += timedelta(weeks=1)

    # keep in window
    out = [(s, e) for (s, e) in out if not (e <= window_start or s >= window_end)]
    out.sort(key=lambda se: se[0])
    return out

# ----- Use item-level recurrence for items with NO events -----


def build_slot_aggregates(
    *,
    cf: CheckfrontClient,
    tz: ZoneInfo,
    start_date_str: Optional[str],
    days: int,
    status_filter: Optional[str] = None,
    include_empty_slots: bool = True,  # create slots from item events even if no bookings  
    ) -> List[SlotAggregate]:
    """
    Build slots in three passes:
      1) Events: generate availability slots (exclude 'U' unavailable overlaps)
      2) Bookings: overlay bookings into slots (create new slots if necessary)
      3) Customers: fetch and attach customer info only for IDs referenced by bookings
    """
    # Window
    sd = start_date_str or date.today().isoformat()
    window_start = datetime.fromisoformat(sd).replace(tzinfo=tz)
    window_end = window_start + timedelta(days=365)

    # ---------- 1) EVENTS ----------
    items_by_id: Dict[str, Dict] = {str(i.get("item_id")): i for i in list(cf.list_items())}

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

        for iid in _event_applies_to_ids(u):
            unavail_by_item.setdefault(iid, []).append((u_start_s, u_end_s))

        for iid in _event_applies_to_categories(u):
            unavail_by_category.setdefault(iid, []).append((u_start_s, u_end_s))


    # Create slots from AVAILABLE item events, excluding overlaps with "U"
    buckets: Dict[Tuple[str, date], SlotAggregate] = {}

    # Which item_ids had *any* event reference (available or unavailable)
    events_seen_item_ids: set[str] = set()

    for ev in available_events + unavailable_events:
        for iid in _event_applies_to_ids(ev):
            events_seen_item_ids.add(str(iid))

    for it in available_events :   # throw away the key, keep the dict

        u_start_s = _datetime_or_none(it.get("start_date"),tz)
        u_end_s = _datetime_or_none(it.get("end_date"),tz)

        if (it.get("start_date") =="0"):
            continue

        if (it.get("end_date")!="0"):
            if (u_end_s < window_start):
                continue

        #total_places = it.get("stock")
        unlimited = bool(it.get("unlimited") == 1)
        print(it.get("name"))

        # expand occurrences from item-level repeat
        occs = _item_occurrences(it, tz, window_start, window_end)

        appliesto =_event_applies_to_ids(it)
        for iid in appliesto:
            # apply unavailability (from item events with status 'U') to this item
            blocks = unavail_by_item.get(iid, [])

            for (s, e) in occs:
                if any(_overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in blocks):
                    continue

                item = items_by_id[iid]
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
                     )
                #else:
                    #if s < buckets[key].start: buckets[key].start = s
                    #if e > buckets[key].end:   buckets[key].end   = e


    # ---------- BOOKINGS ----------
    bookings = list(cf.list_bookings_index(
        start_date=sd,
        end_date=window_end.date().isoformat()
    ))

    for booking in bookings:
        booking_id = str(booking.get("booking_id"))
        cust_id = booking.get("customer_id")
        customer = cf.get_customer(str(cust_id)) if cust_id else None

        bk = cf.get_booking(booking_id)
        b  = bk.get("items") or {}


        for lineid, itm in b.items():
            if not isinstance(itm, dict):
                continue

            sku = itm.get("sku").strip()
            qty = int(itm.get("qty") )
            if not sku or qty <= 0:
                continue

            s_ts = itm.get("start_date") or itm.get("start_time")
            e_ts = itm.get("end_date")   or itm.get("end_time")
            if s_ts is None or e_ts is None:
                continue

            start_iso = datetime.fromtimestamp(int(s_ts), tz=tz)
            end_iso   = datetime.fromtimestamp(int(e_ts), tz=tz)
            if end_iso <= window_start or start_iso >= window_end:
                continue

            key = (sku, start_iso.date())

            # optional: derive capacity/unlimited for this SKU
            item_meta = next((it for it in items_by_id.values() if it.get("sku") == sku), None)
            total_places = item_meta.get("stock") if item_meta else None
            unlimited = bool(item_meta.get("unlimited") == 1) if item_meta else False

            if key not in buckets:
                raise Exception("This should have already")
                buckets[key] = SlotAggregate(
                    sku=sku,
                    start_date=start_iso.date(),
                    start=start_iso,
                    end=end_iso,
                    total_places=total_places,
                    unlimited=unlimited,
                )
            slot = buckets[key]

            # # widen slot if needed
            # if start_iso < slot.start: slot.start = start_iso
            # if end_iso   > slot.end:   slot.end   = end_iso

            # ✅ append the *booking item* (line item) to the slot
            slot.booking_items.append({
                "booking_id": booking_id,
                "customer_id": str(cust_id) if cust_id else None,
                "line_id": str(lineid),
                "sku": sku,
                "qty": qty,
                "start": start_iso,
                "end": end_iso,
                "raw": itm,
            })

            # track customer for this slot (once per customer id)
            if customer:
                cid = customer.get("id") or str(cust_id)
                if cid:
                    slot.customers[cid] = customer
    return buckets