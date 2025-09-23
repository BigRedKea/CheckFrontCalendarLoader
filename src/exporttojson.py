from src.helpers import _normalize


def slots_to_json_ready(calendarEvents):
    """
    Input:
      slots: dict keyed by (sku, date) -> slot object
    Output:
      flat: list of normalized slot dicts (one per (sku, date)), sorted by start
    """
    flat = []

    for (sku, d), calendarEvent in calendarEvents.items():
        bookings = []
        param_totals: dict[str, int] = {}
        group_totals: dict[str, dict] = {}

        total_places = calendarEvent.total_places
        total_booked = 0

        for bi in (calendarEvent.booking_items or []):
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
            cust = (calendarEvent.customers or {}).get(cid) if cid else None
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

        customers = calendarEvent.customers if getattr(calendarEvent, "customers", None) else {}

        # robust tag flattening (accept dicts or strings)
        raw_tags = (calendarEvent.item or {}).get("tags") or []
        flattened_tags = []
        for t in raw_tags:
            if isinstance(t, dict) and "name" in t and isinstance(t["name"], str):
                flattened_tags.append(t["name"])
            elif isinstance(t, str):
                flattened_tags.append(t)

        # ensure times are set
        start_iso = calendarEvent.startdatetime.isoformat() if calendarEvent.startdatetime else None
        end_iso   = calendarEvent.enddatetime.isoformat() if calendarEvent.enddatetime else None

        # available places (respect unlimited)
        available_places = None if calendarEvent.unlimited else (int(total_places or 0) - int(total_booked or 0))

        slot_dict = {
            "sku": sku,
            "date": str(d),
            "start": start_iso,
            "end": end_iso,
            "unlimited": calendarEvent.unlimited,
            "event_id": calendarEvent.calendar_event_id,
            "total_places": total_places,
            "total_booked": total_booked,
            "available_places": available_places,
            "param_totals": param_totals,
            "group_totals": group_totals,
            "tags": flattened_tags,
            "item": calendarEvent.item,
            "bookings": bookings,
            "customers": _normalize(customers),
        }

        flat.append(_normalize(slot_dict))

    # sort the flat list chronologically by start
    flat.sort(key=lambda s: (s.get("start") or ""))

    return flat