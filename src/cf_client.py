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
    # src/cf_client.py
from typing import Any, Dict
import urllib.parse

import requests
from pathlib import Path



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

    # ---------- Customers ----------
    # def list_customers(self) -> List[Dict[str, Any]]:
    #     data = self._request("GET", "/api/3.0/customer")
    #     return list(data["customers"].values())

    # ---------- Bookings index + details ----------
    def list_bookings_index(
        self, *, start_date: str, end_date: str, status_id: Optional[str] = None, limit: int = 100
    ) -> Iterable[Dict[str, Any]]:
        page = 1
        while True:
            params: Dict[str, Any] = {
                "start_date": start_date,
                "end_date": end_date,
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

    # ---------- Utilities similar to your C# helpers ----------

    # @staticmethod
    # def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    #     return not (a_end <= b_start or b_end <= a_start)

    # @staticmethod
    # def expand_weekly(start: datetime, *, until: datetime) -> List[datetime]:
    #     out: List[datetime] = []
    #     cur = start
    #     while cur < until:
    #         out.append(cur)
    #         cur += timedelta(days=7)
    #     return out
