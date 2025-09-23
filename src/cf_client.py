# =============================
# src/cf_client.py
# =============================
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, date as _date, timedelta
import base64
import json
import logging
import urllib.parse
from typing import Any, Dict
import urllib.parse
from functools import lru_cache

import requests
from pathlib import Path
from src.calendarevent import CalendarEvent, ExtractWindow
from src.helpers import _datetime_or_none, _normalize_value

from datetime import datetime
from typing import Dict, List, Tuple, Optional

from src.timerules import TimeRules


log = logging.getLogger(__name__)


class CheckfrontError(RuntimeError):
    pass


@dataclass
class CFConfig:
    host: str                  # e.g. "your-company.checkfront.com" or bookingplatform.app host
    api_key: str
    api_secret: str
    timeout: int = 30

    @staticmethod
    def from_json(path: str) -> "CFConfig":
        """Load Checkfront config (host + credentials) from a JSON file."""
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return CFConfig(
            host=data["host"],
            api_key=data["api_key"],
            api_secret=data["api_secret"]
        )

class CheckfrontClient:
    def __init__(self, cfg: CFConfig, session: Optional[requests.Session] = None):
        self.cfg = cfg
        self.base_url = cfg.host
        self.session = session or requests.Session()
        userpass = f"{cfg.api_key}:{cfg.api_secret}".encode()
        self._customer_cache: Dict[str, Dict[str, Any]] = {}  # in-memory store
        self._auth_header = {
            "Authorization": f"Basic {base64.b64encode(userpass).decode()}",
            "User-Agent": "cf-gcal-sync/1.0"
          # , "X-On-Behalf": cfg.account_id or "off",
        }

    # ---------- HTTP ----------
    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = urllib.parse.urljoin(self.base_url, path)
        resp = self.session.request(
            method,
            url,
            params=params,
            headers=self._auth_header,
            timeout=self.cfg.timeout,
            allow_redirects=False,
        )
        if resp.status_code in (301, 302) and "location" in resp.headers:
            new_url = resp.headers["location"]
            log.info("Redirecting to %s", new_url)
            resp = self.session.request(
                method, new_url, params=params, headers=self._auth_header, timeout=self.cfg.timeout
            )
        try:
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            raise CheckfrontError(f"HTTP {resp.status_code}: {resp.text}") from e
        except ValueError as e:
            raise CheckfrontError(f"Invalid JSON from Checkfront: {e}") from e
        

    # ---------- Items ----------
    def list_items(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/api/3.0/item")
        return list(data["items"].values())

    def list_item_events(self, *, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch all item events (/api/3.0/event) with paging.
        Returns a list of event dicts.
        """
        page = 1
        all_events: List[Dict[str, Any]] = []
        while True:
            params = {"limit": min(max(limit, 1), 1000), "page": page}
            data = self._request("GET", "/api/3.0/event", params=params)

            evs = None
            for key in ("events", "items", "item"):   # defensive
                v = data.get(key)
                if isinstance(v, dict):
                    evs = list(v.values())
                    break
                if isinstance(v, list):
                    evs = v
                    break
            if not evs:
                break

            all_events.extend(evs)

            pages = int((data.get("request") or {}).get("pages", 1))
            if page >= pages:
                break
            page += 1

        return all_events


    # ---------- Bookings index + details ----------
    def list_bookings_index(
        self, *, extract_window:ExtractWindow, status_id: Optional[str] = None, limit: int = 100
    ) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            params: Dict[str, Any] = {
                "start_date": extract_window.window_start,
                "end_date": extract_window.window_end,
                "limit": min(max(limit, 1), 100),
                "page": page,
            }
            if status_id:
                params["status_id"] = status_id
            data = self._request("GET", "/api/3.0/booking/index", params=params)
            req_meta = data.get("request", {})
            pages = int(req_meta.get("pages", 1))
            idx = data.get("booking/index", {})
            for _, row in sorted(idx.items(), key=lambda kv: int(str(kv[0]))):
                yield row
            if page >= pages:
                break
            page += 1

    def get_booking(self, booking_code: str) -> Dict[str, Any]:
        data = self._request("GET", f"/api/3.0/booking/{urllib.parse.quote(booking_code)}")      
        return data.get("booking")
    
    def get_categories(self):
        # Fetch all categories
        categories_resp = self._request("GET""/api/3.0/category")
        categories = categories_resp.get("category", {})
        # Map category_id → category_name
        cat_lookup = {
            str(cat["category_id"]): cat["name"]
            for cat in categories.values()
        }
        return cat_lookup
    

    @lru_cache(maxsize=1)
    def get_checkfront_categories(self) -> dict[str, str]:
        """
        Cached list of Checkfront categories (id and name).
        Example return: [ {"id": "1", "name": "Activities"}, ... ]
        """
        response = self._request("GET","/api/3.0/category")
        cats = response.get("category", {})
        return {
            str(cat["category_id"]): cat["name"]
            for cat in cats.values()
        }
    
    def get_customer(self, customer_id: str) -> Dict[str, Any]:
        """Fetch a customer, with a super-simple in-memory cache."""
        cid = str(customer_id)
        if cid in self._customer_cache:
            return self._customer_cache[cid]

        path = f"/api/3.0/customer/{urllib.parse.quote(cid)}"
        data = self._request("GET", path)

        # minimal, defensive extraction
        cust: Dict[str, Any] = {}
        if isinstance(data.get("customer"), dict):
            cust = data["customer"]

        self._customer_cache[cid] = cust
        return self._customer_cache[cid]

    def cache_customer(self, customer: Dict[str, Any]) -> None:
        cid = str(customer.get("id") or customer.get("customer_id") or "")
        if cid:
            self._customer_cache[cid] = customer


    def extract_calendar_events(self,
        extract_window: ExtractWindow,
        timerulescalculator: TimeRules
        ) -> List[CalendarEvent]:
        """
        Build slots in three passes:
        1) Events: generate availability slots (exclude 'U' unavailable overlaps)
        2) Bookings: overlay bookings into slots (create new slots if necessary)
        3) Customers: fetch and attach customer info only for IDs referenced by bookings
        """

        items_by_id: Dict[str, Dict] = {str(i.get("item_id")): i for i in list(self.list_items())}

        item_events = self.list_item_events()
        available_events = [e for e in item_events if e.get("enabled") and e.get("status") != "U"]
        unavailable_events = [e for e in item_events if e.get("enabled") and e.get("status") == "U"]

        # Map unavailable windows by item-id for fast checks
        unavail_by_item: Dict[str, List[Tuple[datetime, datetime]]] = {}
        unavail_by_category: Dict[str, List[Tuple[datetime, datetime]]] = {}

        for u in unavailable_events:
            u_start_s = _datetime_or_none(u.get("start_date"),extract_window.tz)
            u_end_s = _datetime_or_none(u.get("end_date"),extract_window.tz)
            u_end_s =u_end_s.replace(hour=23, minute=59, second=59, microsecond=9999)

            if u_start_s is None:
                continue

            # Treat missing/zero end as open-ended; cap at our window_end
            if not u_end_s or u_end_s == 0:
                u_end_s = int(extract_window.window_end.timestamp())

            u_end_s =u_end_s.replace(hour=23, minute=59, second=59, microsecond=9999)              

            for applies_to_item_id in self._event_applies_to_ids(u):
                unavail_by_item.setdefault(applies_to_item_id, []).append((u_start_s, u_end_s))

            for applies_to_item_id in self._event_applies_to_categories(u):
                unavail_by_category.setdefault(applies_to_item_id, []).append((u_start_s, u_end_s))


        # Create slots from AVAILABLE item events, excluding overlaps with "U"
        calendarEvents: Dict[Tuple[str, _date], CalendarEvent] = {}

        # Which item_ids had *any* event reference (available or unavailable)
        events_seen_item_ids: set[str] = set()

        for ev in available_events + unavailable_events:
            for applies_to_item_id in self._event_applies_to_ids(ev):
                events_seen_item_ids.add(str(applies_to_item_id))

        for available_event in available_events :
            u_start_s = _datetime_or_none(available_event.get("start_date"),extract_window.tz)
            u_end_s = _datetime_or_none(available_event.get("end_date"),extract_window.tz)

            if (available_event.get("start_date") =="0"):
                continue

            if (available_event.get("end_date")!="0"):
                if (u_end_s < extract_window.window_start):
                    continue

            # expand occurrences from item-level repeat
            occs = self._item_occurrences(available_event, extract_window)

            applies_to_item_ids = self._event_applies_to_ids(available_event)

            for applies_to_item_id in applies_to_item_ids:
                # apply unavailability (from item events with status 'U') to this item

                item = items_by_id.get(applies_to_item_id)

                if item ==None:
                    continue # May be an archived Item

                itemcategory = item.get("category")
                sku = item.get("sku")
                timerule = timerulescalculator._get_rule_for(itemcategory, sku)

                notavailableitem = unavail_by_item.get(applies_to_item_id, [])
                notavailablecategory = unavail_by_category.get(itemcategory, [])

                for (s, e) in occs:
                    if any(self._overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailableitem):
                        continue
                    if any(self._overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailablecategory):
                        continue

                    
                    (startdatetime, enddatetime) = timerulescalculator.apply_time_rule(timerule, s, e )


                    key = (sku, startdatetime)

                    if key not in calendarEvents:
                        calendarEvents[key] = CalendarEvent(
                            checkfrontitem = item,
                            checkfrontitemevent = available_event,
                            startdatetime=startdatetime,
                            enddatetime=enddatetime
                        )
                        
            available_items_by_id: Dict[str, Dict] = {
                k: v for k, v in items_by_id.items() 
                if v.get("status") != "U" and v.get("unlimited") ==0 and v.get("visibility") =="*"}
                        
            for applies_to_item_id in available_items_by_id:

                item = items_by_id.get(applies_to_item_id)
                if item ==None:
                    continue # May be an archived Item

                occs = self._item_occurrences(item, extract_window)
                
                notavailableitem = unavail_by_item.get(applies_to_item_id, [])
                notavailablecategory = unavail_by_category.get(itemcategory, [])

                for (s, e) in occs:
                    if any(self._overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailableitem):
                        continue
                    if any(self._overlaps(s, e, ub_s, ub_e) for (ub_s, ub_e) in notavailablecategory):
                        continue

                    rule = timerulescalculator._get_rule_for(itemcategory,sku)
                    (startdatetime, enddatetime) = timerulescalculator.apply_time_rule(rule, s, e )

                    sku = item.get("sku")
                    key = (sku, startdatetime)

                    if key not in calendarEvents:
                        calendarEvents[key] = CalendarEvent(
                            checkfrontitem= item,
                            checkfrontitemevent = available_event,
                            startdatetime=startdatetime,
                            enddatetime=enddatetime
                        )
        return calendarEvents
    

    
    def _item_occurrences(self, item: dict, extract_window: ExtractWindow) -> list[tuple[datetime, datetime]]:

        out: list[tuple[datetime, datetime]] = []

        RFC5545_DAYS = ["mon","tue","wed","thu","fri","sat","sun"]

        # Base anchor date
        base_s = item.get("start_date")
        itemstart = _datetime_or_none(base_s, extract_window.tz) if base_s else extract_window.window_start
        base_e = item.get("end_date")
        itemend = _datetime_or_none(base_e, extract_window.tz) 
        if not itemend:
            itemend = extract_window.window_end

        reps = (item.get("repeat"))
        if not reps:

             #Build daily occurrences for an item.
            out: list[tuple[datetime, datetime]] = []

            cur = itemstart
            while cur <= itemend:
                end = cur
                out.append((cur, end))
                cur += timedelta(days=1)

            out.sort(key=lambda se: se[0])
            return out
            #out.append((s, itemend))
            #return out

        # align first occurrence per weekday
        for wd in reps:
            if wd not in RFC5545_DAYS:
                continue
            target_idx = RFC5545_DAYS.index(wd)
            # start on the first matching weekday >= window_start
            first_day = itemstart + timedelta(days=(target_idx - itemstart.weekday()) % 7)
            # set time window for that day
            first = first_day #.replace(hour=sh, minute=sm, second=0, microsecond=0)
            if first < extract_window.window_start:
                # jump forward in steps of 'interval' weeks
                delta_days = (extract_window.window_start - first).days
                jumps = (delta_days // (7 )) * (7 )
                first = first + timedelta(days=jumps)
                while first < extract_window.window_start:
                    first += timedelta(weeks=1)
            cur = first
            while cur < extract_window.window_end:
                end = cur
                out.append((cur, end))
                cur += timedelta(weeks=1)

        out.sort(key=lambda se: se[0])
        return out


    def _event_applies_to_ids(self, ev: Dict) -> list[str]:
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

    def _event_applies_to_categories(self, ev: Dict) -> list[str]:
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


    def _overlaps(self, a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
        return not (a_end <= b_start or b_end <= a_start)
    
    def items_by_sku(self) -> Dict[str, Dict]:
        # All Checkfront items keyed by SKU
        items: Dict[str, Dict] = {
            str(i.get("sku")): i
            for i in self.list_items()
                if i and i.get("sku") is not None
            }
        return items
    

    def extract_bookings_items_customers(self, extract_window) -> Tuple[List[dict], List[dict], Dict[str, dict]]:
        """
        Returns:
        bookings:      List[dict]          - full booking dicts (from get_booking), one per booking_id included
        booking_items: List[dict]          - flattened booking-line records (one per booking item)
        customers:     Dict[str, dict]     - unique customers keyed by string id
        """

        bookings: List[dict] = []
        booking_items: List[dict] = []
        customers: Dict[str, dict] = {}
        category_lookup = self.get_checkfront_categories()

        index = list(self.list_bookings_index(extract_window=extract_window))

        for booking_idx in index:
            booking_id = str(booking_idx.get("booking_id"))
            if not booking_id:
                continue

            customer_id = booking_idx.get("customer_id")
            customer: Optional[dict] = None
            if customer_id:
                try:
                    customer = self.get_customer(str(customer_id))
                    cid = (customer.get("id") if isinstance(customer, dict) else None) or str(customer_id)
                    if cid and cid not in customers and isinstance(customer, dict):
                        customers[cid] = customer
                except Exception:
                    pass

            try:
                booking_detail = self.get_booking(booking_id)
            except Exception:
                continue

            if isinstance(booking_detail, dict):
                bd = dict(booking_detail)
                bd.setdefault("booking_id", booking_id)
                if customer_id is not None:
                    bd.setdefault("customer_id", customer_id)
                bookings.append(bd)

            items_map = (booking_detail.get("items") if isinstance(booking_detail, dict) else None) or {}
            if not isinstance(items_map, dict):
                continue
        


            for bookinglineid, item in items_map.items():
                if not isinstance(item, dict):
                    continue
                if item.get("status_id") == "VOID":
                    continue

                sku = (item.get("sku") or "").strip()
                try:
                    qty = int(item.get("qty") or 0)
                except Exception:
                    qty = 0
                if not sku or qty <= 0:
                    continue

                start_ts = item.get("start_date")
                end_ts = item.get("end_date")
                if start_ts is None or end_ts is None:
                    continue




                try:
                    start_dt = datetime.fromtimestamp(int(start_ts), extract_window.tz)
                    end_dt   = datetime.fromtimestamp(int(end_ts), extract_window.tz)
                except Exception:
                    continue

                # filter items completely outside window
                if end_dt <= extract_window.window_start or start_dt >= extract_window.window_end:
                    continue

                category_id = str(item.get("category_id"))

                category = category_lookup[category_id]

                flat = {
                    "booking_id": booking_id,
                    "line_id": str(bookinglineid),
                    "customer_id": str(customer_id) if customer_id else None,
                    "sku": sku,
                    "qty": qty,
                    "start": start_dt,
                    "end": end_dt,
                    "category":category
                }

                for k, v in item.items():
                    if k not in flat:
                        flat[k] = _normalize_value(v)

                # if sku in items_by_sku and isinstance(items_by_sku[sku], dict):
                #     for k, v in items_by_sku[sku].items():
                #         if k not in flat:
                #             flat[k] = _normalize_value(v)

                booking_items.append(flat)

        return bookings, booking_items, customers
