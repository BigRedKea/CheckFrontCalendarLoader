# =============================
# src/cf_sync.py
# =============================
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .cf_client import CheckfrontClient, CFConfig
from .gcal_client import GCalClient, event_body_from_cf,  exdate_list, rrule_weekly

WEEKDAY_CODES = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]


class CFToGCalSync:
    """Aggregate Checkfront into Calendar events.

    - Singles: one-off bookings
    - Recurring: weekly series found by (item_id, weekday, HH:MM) ≥ N weeks
    - Blocks: times marked unavailable by status (e.g., STOP/HOLD)
    """

    def __init__(
        self,
        *,
        host: str,
        api_key: str,
        api_secret: str,
        calendar_private_id: str,
        calendar_public_id: str,
        sa_json_path: str,
        timezone_name: str = "UTC",
        account_id: str = "off",
        default_status: Optional[str] = None,
        color_id: Optional[str] = None,
        block_statuses: Optional[List[str]] = None,
        min_weeks_for_recurrence: int = 3,
    ):
        self.cf = CheckfrontClient(
            CFConfig(
                host=host,
                api_key=api_key,
                api_secret=api_secret
            )
        )
        self.gcal = GCalClient(sa_json_path, calendar_private_id)
        self.tz = self._load_tz(timezone_name)
        self.default_status = default_status
        self.color_id = color_id
        self.block_statuses = set((block_statuses or ["STOP"]))
        self.min_weeks_for_recurrence = max(2, int(min_weeks_for_recurrence))

    @staticmethod
    def _load_tz(name: str) -> ZoneInfo:
        try:
            return ZoneInfo(name)
        except ZoneInfoNotFoundError:
            from datetime import timezone, timedelta  # type: ignore

            if name.startswith("Etc/GMT"):
                sign = -1 if "+" in name else 1
                num = int(name.split("GMT")[-1])
                return timezone(timedelta(hours=sign * num))  # type: ignore
            raise

    # ---------- Data fetch ----------

    def _to_iso(self, ts: int) -> str:
        return datetime.fromtimestamp(int(ts), tz=self.tz).isoformat()

    def fetch_window(self, start_date_str: Optional[str], days: int) -> List[Dict[str, Any]]:
        sd_str = start_date_str or date.today().isoformat()
        ed_str = (datetime.fromisoformat(sd_str) + timedelta(days=days)).date().isoformat()
        return list(
            self.cf.list_bookings_index(start_date=sd_str, end_date=ed_str, status_id=self.default_status)
        )

    def booking_detail_times(self, booking_code: str) -> Optional[Tuple[str, str]]:
        data = self.cf.get_booking(booking_code)
        b = data.get("booking") or data.get("booking/booking") or {}
        start_ts = b.get("start_date")
        end_ts = b.get("end_date")
        if start_ts is None or end_ts is None:
            order = b.get("order") or {}
            if isinstance(order, dict):
                times: List[int] = []
                for _, item in (order.get("items") or {}).items():
                    s = item.get("start_date") or item.get("start_time")
                    e = item.get("end_date") or item.get("end_time")
                    if s is not None and e is not None:
                        times += [int(s), int(e)]
                if len(times) >= 2:
                    start_ts, end_ts = min(times), max(times)
        if start_ts is None or end_ts is None:
            return None
        return self._to_iso(int(start_ts)), self._to_iso(int(end_ts))

    # ---------- Aggregation ----------

    def _series_key(self, item_id: str, start_dt: datetime) -> str:
        return f"cfseries:{item_id}:{WEEKDAY_CODES[start_dt.weekday()]}:{start_dt.strftime('%H%M')}"

    def _block_key(self, start_iso: str, end_iso: str) -> str:
        return f"cfblock:{start_iso}|{end_iso}"

    def aggregate(
        self, rows: List[Dict[str, Any]], *, window_start: datetime, window_end: datetime
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        groups: DefaultDict[str, List[Tuple[datetime, datetime, Dict[str, Any]]]] = defaultdict(list)
        singles: List[Dict[str, Any]] = []
        blocks: List[Dict[str, Any]] = []

        items = {str(i.get("id")): i for i in self.cf.list_items()}
        item_events = self.cf.list_item_events()  # available for future use

        def color_for_item(item: Dict[str, Any]) -> Optional[str]:
            cat = (item.get("category_name") or item.get("category") or "").lower()
            if cat.startswith("accom"):
                return "7"
            if cat.startswith("activ"):
                return "8"
            return None

        for row in rows:
            code = str(row.get("code") or row.get("booking_id"))
            status_id = (row.get("status_id") or "").upper()
            times = self.booking_detail_times(code)
            if not times:
                continue
            start_iso, end_iso = times
            start_dt = datetime.fromisoformat(start_iso)
            end_dt = datetime.fromisoformat(end_iso)

            item_id = str(row.get("item_id") or row.get("inventory_id") or "item")
            item = items.get(item_id, {})

            if status_id in self.block_statuses:
                body = event_body_from_cf(
                    booking_code=self._block_key(start_iso, end_iso),
                    title="UNAVAILABLE",
                    start_iso=start_iso,
                    end_iso=end_iso,
                    timezone_str=str(getattr(self.tz, "key", "UTC")),
                    description=f"Blocked ({status_id})",
                    color_id="8",
                )
                body["extendedProperties"]["private"]["syncKey"] = self._block_key(start_iso, end_iso)
                body["transparency"] = "opaque"
                blocks.append(body)
                continue

            skey = self._series_key(item_id, start_dt)
            groups[skey].append((start_dt, end_dt, {**row, "item": item}))

        recurring_bodies: List[Dict[str, Any]] = []
        single_bodies: List[Dict[str, Any]] = []

        for skey, occs in groups.items():
            weeks = {(d.date().isocalendar().year, d.date().isocalendar().week) for d, _, _ in occs}
            if len(weeks) >= self.min_weeks_for_recurrence:
                first_start = min(d for d, _, _ in occs)
                first_end = min(e for _, e, _ in occs)
                byday = WEEKDAY_CODES[first_start.weekday()]
                rrule = rrule_weekly(byday=byday, until=window_end)

                row0 = sorted(occs, key=lambda x: x[0])[0][2]
                item = row0.get("item") or {}
                title = self._series_title(row0)

                body = event_body_from_cf(
                    booking_code=skey,
                    title=title,
                    start_iso=first_start.isoformat(),
                    end_iso=first_end.isoformat(),
                    timezone_str=str(getattr(self.tz, "key", "UTC")),
                    description="Aggregated recurring booking",
                    color_id=color_for_item(item) or None,
                )
                body["recurrence"] = [rrule]

                have_dates = {d.date() for d, _, _ in occs}
                exdates = exdate_list(
                    start_dt=first_start,
                    until_dt=window_end,
                    byday=first_start.weekday(),
                    have_dates=have_dates,
                )
                if exdates:
                    body.setdefault("extendedProperties", {}).setdefault("private", {})[
                        "EXDATE"
                    ] = ",".join(exdates)
                recurring_bodies.append(body)
            else:
                for start_dt, end_dt, row0 in occs:
                    item = row0.get("item") or {}
                    title = self._single_title(row0)
                    body = event_body_from_cf(
                        booking_code=str(row0.get("code") or row0.get("booking_id")),
                        title=title,
                        start_iso=start_dt.isoformat(),
                        end_iso=end_dt.isoformat(),
                        timezone_str=str(getattr(self.tz, "key", "UTC")),
                        description=f"Checkfront booking {row0.get('code') or row0.get('booking_id')}",
                        color_id=color_for_item(item) or None,
                    )
                    single_bodies.append(body)

        return recurring_bodies, single_bodies, blocks

    def _series_title(self, row: Dict[str, Any]) -> str:
        name = row.get("customer_name") or row.get("item_name") or "Booking"
        summary = row.get("summary") or row.get("item_name") or ""
        base = f"📆 {name} — {summary}" if summary else f"📆 {name}"
        return f"{base} (Weekly)"

    def _single_title(self, row: Dict[str, Any]) -> str:
        name = row.get("customer_name") or row.get("item_name") or "Booking"
        summary = row.get("summary") or row.get("item_name") or ""
        return f"📆 {name} — {summary}" if summary else f"📆 {name}"

    # ---------- Public ops ----------

    def upsert(self, start_date: Optional[str], days: int) -> Tuple[int, int, int]:
        sd = start_date or date.today().isoformat()
        window_start = datetime.fromisoformat(sd).replace(tzinfo=self.tz)
        window_end = window_start + timedelta(days=days)

        rows = self.fetch_window(sd, days)
        recurring_bodies, singles, blocks = self.aggregate(
            rows, window_start=window_start, window_end=window_end
        )

        ins_r, upd_r = self.gcal.upsert(recurring_bodies, time_min=window_start, time_max=window_end)
        ins_s, upd_s = self.gcal.upsert(singles, time_min=window_start, time_max=window_end)
        ins_b, upd_b = self.gcal.upsert(blocks, time_min=window_start, time_max=window_end)

        inserted = ins_r + ins_s + ins_b
        updated = upd_r + upd_s + upd_b
        skipped = 0
        return inserted, updated, skipped

    def delete_range(self, start_date: Optional[str], days: int, *, only_cf: bool = True) -> int:
        sd = datetime.fromisoformat((start_date or date.today().isoformat())).replace(tzinfo=self.tz)
        ed = sd + timedelta(days=days)
        prefix = "cf:" if only_cf else None
        return self.gcal.delete_range(sd, ed, filter_sync_prefix=prefix)
