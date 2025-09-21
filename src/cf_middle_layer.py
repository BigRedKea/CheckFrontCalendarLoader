from __future__ import annotations

from datetime import datetime,  timedelta
from typing import Dict, List

import json

from src.timerules import TimeRules

from .cf_client import CheckfrontClient
from .calendarevent import CalendarEvent, ExtractWindow

with open("config.json", "r") as f:
    CONFIG = json.load(f)


def extract_checkfront_data(
    *,
    checkfrontClient: CheckfrontClient,
    extract_window: ExtractWindow
) -> List[CalendarEvent]:
    """
    Build CalendarEvent list by grouping flattened booking lines
    (expanded into daily occurrences) into (sku, occurrence_date) buckets.
    """

    rules = CONFIG["time_rules"]
    timerulescalculator = TimeRules(rules)

    # Bucket of CalendarEvents keyed by (sku, occurrence_date)
    #events_by_key: Dict[Tuple[str, datetime.date], CalendarEvent] = {}
    calendarevents= checkfrontClient.extract_calendar_events(extract_window, timerulescalculator)
    
    # Pull everything booking related in one pass
    bookings, booking_items, customers = checkfrontClient.extract_bookings_items_customers(extract_window)

    # All Checkfront items keyed by SKU
    #items_by_sku = checkfrontClient.items_by_sku

    for bookingitem in booking_items:
        sku = (bookingitem.get("sku") or "").strip()
        category = bookingitem.get("category") 

        rule = timerulescalculator._get_rule_for(category, sku)

        (bookingitem_start_datetime, bookingitem_end_datetime) = timerulescalculator.apply_time_rule(rule, bookingitem.get("start"), bookingitem.get("end"))

        if not sku or bookingitem_start_datetime is None or bookingitem_end_datetime is None:
            continue

        key = (sku, bookingitem_start_datetime)

        sku_events = {d: ev for (s, d), ev in calendarevents.items() if s == sku}

        # check date
        if not sku_events:
            print(f"Warning No calendar events defined for SKU '{sku}' ")
   
        if sku_events and key not in calendarevents:
            print (f"Warning No calendar event defined for {key}")

        if key not in calendarevents:
            items_by_sku = checkfrontClient.items_by_sku()
            item_meta = items_by_sku[sku]
            calendarevents[key] = CalendarEvent(
                    checkfrontitem=item_meta,
                    checkfrontitemevent=None,
                    startdatetime=bookingitem_start_datetime,
                    enddatetime=bookingitem_end_datetime
                )

        ev = calendarevents[key]

        # clone flat booking with occurrence times
        occ_flat = dict(bookingitem)
        #occ_flat.update({"start": occ_start, "end": occ_end})
        ev.booking_items.append(occ_flat)

        # attach customer (once per id)
        cid = bookingitem.get("customer_id")
        if cid:
            cust = customers.get(str(cid))
            if cust and str(cid) not in ev.customers:
                ev.customers[str(cid)] = cust

    return list(calendarevents.values())


