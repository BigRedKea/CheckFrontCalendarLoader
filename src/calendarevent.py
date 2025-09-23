

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, Optional
from .helpers import _to_datetime

@dataclass
class ExtractWindow:
    def __init__ (self, tzid: str, days: int):
        sd = date.today().isoformat()
        self.tz = tzid
        self.window_start = datetime.fromisoformat(sd).replace(tzinfo=tzid)
        self.window_end = self.window_start + timedelta(days=days)

@dataclass(frozen=True)
class Customer:
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    group: Optional[str] = None  # e.g., "Scout Group"

@dataclass
class CalendarEvent:
    calendar_event_id: str
    sku: str
    start_date: date
    startdatetime: datetime
    enddatetime: datetime
    unlimited: bool = False
    color_id: Optional[str] = None
    checkfrontitem: Optional[dict] = None  
    item_event: list[dict] = field(default_factory=list)

    def __init__ (self, checkfrontitem, checkfrontitemevent, startdatetime: datetime, enddatetime:datetime):
        self.startdatetime = startdatetime
        self.enddatetime = enddatetime
        self.checkfrontitemevent  =checkfrontitemevent

        #Item Details
        self.sku = checkfrontitem.get("sku")
        self.total_places = checkfrontitem.get("stock")
        self.total_places=int(self.total_places) if self.total_places is not None else None
        self.calendar_event_id = f"{self.sku}_{_to_datetime(self.startdatetime).strftime("%Y_%m_%d_%H_%M")}"
        self.tags: list[str] = [t["name"] for t in checkfrontitem.get("tags") if "name" in t]
        self.checkfrontitem = checkfrontitem

        # Checkfront Item Events
        if checkfrontitemevent:
            self.unlimited = bool(checkfrontitemevent.get("unlimited") == 1)

        self.booking_items = []
        self.customers: Dict[str, Customer] ={}

    def total_booked(self) -> int:
        """Total quantity booked for this single event."""
        return sum(bi.get("qty", 0) for bi in self.booking_items)

        # def _event_duration(ev: Dict, tz: ZoneInfo) -> timedelta:
#     """Duration = base end - base start (fall back to 3h)."""
#     s = _datetime_or_none(ev.get("start_date"),tz)
#     e = _datetime_or_none(ev.get("end_date"),tz)
#     if s is not None and e is not None and e > s:
#         return e - s
#     return timedelta(hours=3)
    









    