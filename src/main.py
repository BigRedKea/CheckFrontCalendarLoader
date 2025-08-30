
# =============================
# src/main.py
# =============================
from __future__ import annotations
import argparse
import json
from pathlib import Path

from .cf_sync import CFToGCalSync, CFConfig


def load_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def build_cli():
    p = argparse.ArgumentParser(description="Sync Checkfront bookings to Google Calendar")
    p.add_argument("command", choices=["upsert", "delete-range"], help="Action to run")
    p.add_argument("--config", dest="config", default="config.json", help="Path to config.json")
    p.add_argument("--start", dest="start", default=None, help="Start YYYY-MM-DD (default: today)")
    p.add_argument("--days", dest="days", type=int, default=7, help="Window length in days")
    return p

from datetime import date
from zoneinfo import ZoneInfo

from .cf_client import CFConfig, CheckfrontClient
from .cf_middle_layer import build_slot_aggregates

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

        # Print a quick summary
        for slot in slots:
            print(f"{slot.start}–{slot.end} SKU={slot.sku} "
                f"{slot.total_booked}/{slot.total_places or '∞'}")
            print(slot.render_description())
            print("----")

if __name__ == "__main__":
    run_middle_layer()
