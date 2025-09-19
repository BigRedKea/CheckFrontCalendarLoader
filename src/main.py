# =============================
# src/main.py
# =============================
from __future__ import annotations
import argparse

import json
from pathlib import Path
from typing import Dict, List, Tuple

from .cf_sync import CFConfig

from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo

from .cf_client import CFConfig, CheckfrontClient
from .cf_middle_layer import extract_checkfront_data, SlotAggregate
from .gcal_sync import get_calendar_service
from .gcal_client import sync_calendar
from .adapters import  filter_slots_for_calendar

from .booking_to_calendar import slots_to_calendar_events_for, resolve_calendars_for_tags

def build_cli():
    p = argparse.ArgumentParser(description="Sync Checkfront bookings to Google Calendar")
    p.add_argument("command", choices=["upsert", "delete-range"], help="Action to run")
    p.add_argument("--config", dest="config", default="config.json", help="Path to config.json")
    p.add_argument("--start", dest="start", default=None, help="Start YYYY-MM-DD (default: today)")
    p.add_argument("--days", dest="days", type=int, default=7, help="Window length in days")
    return p


def run_middle_layer():

    # Load Config
    config = json.loads(Path("config.json").read_text(encoding="utf-8"))
    tz = ZoneInfo(config.get("TIMEZONE"))
    checkfrontpath = config.get("Checkfront_Path")

    # Load Checkfront credentials
    checkfront_config = CFConfig.from_json(checkfrontpath)
    cf = CheckfrontClient(checkfront_config)

    # Choose start + days
    start_date = date.today().isoformat()
        # --- Get slots from Checkfront ---
    tz = timezone(timedelta(hours=10))  # AEST (adjust if needed)
    start_date = "2025-09-07"           # example, set dynamically
    days = 365                            # how many days ahead

    # Call the builder
    slots = extract_checkfront_data(
        cf=cf,
        tz=tz,
        start_date_str=start_date,
        days=days
    )

    

     
    out_path = Path.cwd() / "output" / "slots.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(slots, f, indent=2, ensure_ascii=False)

    # --- Load config ---
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # --- Auth ---
    sa_path = config.get("SA_JSON_PATH")
    if not sa_path:
        raise RuntimeError("Please set SA_JSON_PATH env var to your service account JSON file")
    svc = get_calendar_service(sa_path)

    # --- Push to calendars ---
    results = push_slots_to_calendars(svc, cfg, slots, tz, days)

    print(f" Finished ")



def push_slots_to_calendars(svc, cfg: Dict, slots: List[Dict], tz ,days) -> List[Dict]:
    """Push all slots to calendars using calendar-centric config."""
    results: List[Dict] = []

    # for dt, slot in slots.items():
    #     for item in slot:
    #         booking, tags = slot_to_booking_and_tags(item, tz)
    #         if booking == None:
    #             continue
    #         results.extend(push_calendarevent_by_tags(svc, cfg, booking, tags))
    # return results

    # Time window for sync
    tz = timezone(timedelta(hours=10))        # or from cfg["timezone"]
    tmin = datetime.now(tz) - timedelta(days=1)
    tmax = datetime.now(tz) + timedelta(days=days)
    tzid = cfg["TIMEZONE"]


    for cal in cfg.get("calendars"):

        #slots_for_calendar = filter_slots_for_calendar(slots, cal, cfg)
        
        cal_id = cal["calendarid"]

        slots_for_cal = [
            s for s in slots
            if cal_id in resolve_calendars_for_tags(s.get("tags", []), cfg)
        ]
        if not slots_for_cal:
            continue

        bookings_for_cal = slots_to_calendar_events_for(cal_id, slots_for_cal, cfg, tz)
        print(f"Updating {cal.get("name")}")

        summary = sync_calendar(
            svc=svc,
            cfg=cfg,
            calendar_id=cal_id,
            bookings_for_cal=bookings_for_cal,
            time_min=tmin,
            time_max=tmax,
            tzid=cfg.get("timezone", "Australia/Brisbane"),
            send_updates="none",
            delete_orphans=True,
        )

        print(f"{cal['name']}: +{summary['inserted']} ~{summary['patched']} = "
            f"{summary['unchanged']} -{summary['deleted']}")


if __name__ == "__main__":
    run_middle_layer()




