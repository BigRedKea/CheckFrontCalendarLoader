
# =============================
# src/main.py
# =============================
from __future__ import annotations
import argparse
from collections import defaultdict
import json
from pathlib import Path

from .cf_sync import CFToGCalSync, CFConfig

from datetime import datetime, date
from zoneinfo import ZoneInfo

from .cf_client import CFConfig, CheckfrontClient
from .cf_middle_layer import build_slot_aggregates, SlotAggregate
from pathlib import Path


def load_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def build_cli():
    p = argparse.ArgumentParser(description="Sync Checkfront bookings to Google Calendar")
    p.add_argument("command", choices=["upsert", "delete-range"], help="Action to run")
    p.add_argument("--config", dest="config", default="config.json", help="Path to config.json")
    p.add_argument("--start", dest="start", default=None, help="Start YYYY-MM-DD (default: today)")
    p.add_argument("--days", dest="days", type=int, default=7, help="Window length in days")
    return p



def _normalize(value):
    """Recursively normalize to JSON-safe types."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def sort_json_by_date(json_ready: dict) -> dict:
    """Return a new dict with keys (YYYY-MM-DD) sorted chronologically."""
    return {
        k: json_ready[k]
        for k in sorted(json_ready.keys(), key=lambda d: date.fromisoformat(d))
    }



def run_middle_layer():
    # Load Checkfront credentials
    cf_config = CFConfig.from_json("C:/secrets/checkfront_credentials.json")
    cf = CheckfrontClient(cf_config)

    # Pick your timezone
    tz = ZoneInfo("Australia/Brisbane")

    # Choose start + days
    start_date = date.today().isoformat()
    days = 356

    # Call the builder
    slots = build_slot_aggregates(
        cf=cf,
        tz=tz,
        start_date_str=start_date,
        days=days,
        status_filter=None,      # or None to include all
        include_empty_slots=True,  # also include slots created by item events
    )
    if (slots != None):

        # Usage:
        json_ready = buckets_to_json_ready(slots)

        # assuming json_ready = buckets_to_json_ready(buckets)
        sorted_json = sort_json_by_date(json_ready)

        # Pretty print
        print(json.dumps(sorted_json , indent=2))

        
        out_path = Path.cwd() / "output" / "slots.json"

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(sorted_json, f, indent=2, ensure_ascii=False)


        print(f" finished ")

        # # Group by date
        # grouped: dict[date, list[SlotAggregate]] = defaultdict(list)
        # for (sku, d), slot in slots.items():
        #     grouped[d].append(slot)

        # # Pretty-print
        # for d, slots in sorted(grouped.items()):
        #     print(f"\n=== {d} ===")
        #     for slot in slots:
        #         print(f"  {slot.sku}: {slot.start:%H:%M} "
        #             f"places={slot.total_places}, unlimited={slot.unlimited}, "
        #             f"booked={slot.total_booked}")
                


# def buckets_to_json_ready(buckets):
#     grouped = defaultdict(list)

#     for (sku, d), slot in buckets.items():
#         grouped[str(d)].append(_normalize({
#             "sku": sku,
#             "start": slot.start.isoformat(),
#             "end": slot.end.isoformat() if slot.end else None,
#             "total_places": slot.total_places,
#             "unlimited": slot.unlimited,
#             "total_booked": getattr(slot, "total_booked", None),
#             "booking_items": slot.booking_items,   # already a list of dicts
#             "customers": list(slot.customers.values()),  # flatten dict to list
#         }))



        

#     # return as normal dict (defaultdict won’t serialize)
#     return dict(grouped)

def buckets_to_json_ready(slots):
    grouped = defaultdict(list)

    #for each slot
    for (sku, d), slot in slots.items():
        # build booking rows (each keeps its param, just add customer_id)
        bookings = []
        param_totals: dict[str, int] = {}
        group_totals: dict[str, dict] = {}

        for bi in slot.booking_items or []:
            row = {**bi}  # copy
            # ensure customer_id is present
            row["customer_id"] = bi.get("customer_id")
            bookings.append(_normalize(row))

            # aggregate param totals across all booking_items
            params = bi.get("param") or {}
            for key, p in params.items():
                qty = int(p.get("qty") or 0)
                param_totals[key] = param_totals.get(key, 0) + qty

            # get the quantuty
            qty = int(bi.get("qty"))

            # get the customer
            cid = bi.get("customer_id")
            cust = (slot.customers or {}).get(cid) if cid else None

            # Pull the scout group from Meta Data
            meta = (cust or {}).get("meta")
            grp = (
                meta.get("scout_group_booking")
                or "Unknown"
            )
            email = (
                meta.get("your_leaders_email_address") or None
            )

            #group by the scoutgroup and included supplied leaders email addresses
            if grp not in group_totals:
                group_totals[grp] = {"total_qty": 0, "emails": set()}

            group_totals[grp]["total_qty"] += qty

            #add scoutleaders email
            if email:
                if not isinstance(group_totals[grp]["emails"], set):
                    group_totals[grp]["emails"] = set(group_totals[grp]["emails"])
                group_totals[grp]["emails"].add(email)

        # convert email sets → lists
        for grp, data in group_totals.items():
            data["emails"] = sorted(list(data["emails"]))

        # Customers as a dictionary keyed by customer_id
        customers = slot.customers if getattr(slot, "customers", None) else {}

        slot_dict = {
            "sku": sku,
            "date": str(d),
            "start": slot.start.isoformat() if slot.start else None,
            "end": slot.end.isoformat() if slot.end else None,
            "unlimited": slot.unlimited,
            "total_places": slot.total_places,
            "total_booked": getattr(slot, "total_booked", 0),
            "available_places": slot.total_places - getattr(slot, "total_booked", 0),
            "param_totals": param_totals,
            "group_totals": group_totals,
            "bookings": bookings,  
            "customers": _normalize(customers),  
        }
        grouped[str(d)].append(_normalize(slot_dict))

    return dict(grouped)


if __name__ == "__main__":
    run_middle_layer()




        # Times-of-day - decorate the event start end times last not currently uising timeslots
    #sh, sm = _parse_hhmm(item.get("time_start"), default=(8,0))
    #eh, em = _parse_hhmm(item.get("time_end"),   default=(15,0))
