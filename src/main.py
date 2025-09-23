# =============================
# src/main.py
# =============================
from __future__ import annotations
import argparse

import json
from pathlib import Path
from typing import Dict, List, Tuple

from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo

from .calendarevent import CalendarEvent, ExtractWindow

from .cf_client import CFConfig, CheckfrontClient
from .cf_middle_layer import extract_checkfront_data

from .booking_to_calendar import calendarevents_to_googlecalendar, resolve_calendars_for_tags

from .gcal_client import GCalClient


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

    extract_window = ExtractWindow(tz, 365)

    # Load Checkfront credentials
    checkfront_config = CFConfig.from_json(checkfrontpath)
    checkfrontClient = CheckfrontClient(checkfront_config)

    # Call the builder
    calendarEvents = extract_checkfront_data(
        checkfrontClient=checkfrontClient,
        extract_window= extract_window
    )
   
    #out_path = Path.cwd() / "output" / "slots.json"

    #with out_path.open("w", encoding="utf-8") as f:
    #    json.dump(slots, f, indent=2, ensure_ascii=False)

    # --- Load config ---
    #with open("config.json", "r", encoding="utf-8") as f:
    #    config = json.load(f)

    # --- Auth ---

    """Push all slots to calendars using calendar-centric config."""
    for cal in config.get("calendars"):
       
        cal_id = cal["calendarid"]

        eventsforCalendar = [
            calendarEvent for calendarEvent in calendarEvents
            if cal_id in resolve_calendars_for_tags(calendarEvent.tags, config)
        ]
        if not eventsforCalendar:
            continue

        sa_path = config.get("SA_JSON_PATH")
        if not sa_path:
            raise RuntimeError("Please set SA_JSON_PATH to your service account JSON file")
        calendar_service = GCalClient(sa_path, calendar_id=cal_id)

        # --- Push to calendars ---
        #results = push_calendarevents_to_calendars(calendar_service, config, calendarEvents, extract_window)

        bookings_for_cal = calendarevents_to_googlecalendar(cal_id, eventsforCalendar, config, extract_window.tz)
        print(f"Updating {cal.get("name")}")

        summary = calendar_service.sync_calendar(
            calendar_id=cal_id,
            bookings_for_cal=bookings_for_cal,
            extract_window = extract_window,
            send_updates="none",
            delete_orphans=True
        )

        print(f"{cal['name']}: +{summary['inserted']} ~{summary['patched']} = "
            f"{summary['unchanged']} -{summary['deleted']}")
        
    print(f" Finished ")


if __name__ == "__main__":
    run_middle_layer()




