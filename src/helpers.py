from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import os, hashlib, base64, re

def _datetime_or_none(v, tz) -> datetime:
    try:
        naive_dt = datetime.strptime(v, "%Y%m%d")
        return naive_dt.replace(tzinfo=tz)
    except Exception:
        return None
    
def _flatten_tags(tags: List[Dict] | List[str]) -> List[str]:
    """
    Accept [{'name':'Cub'}, ...] or ['Cub', ...] → ['Cub', ...]
    """
    out: List[str] = []
    for t in tags or []:
        if isinstance(t, dict) and isinstance(t.get("name"), str):
            out.append(t["name"].strip())
        elif isinstance(t, str):
            out.append(t.strip())
    return out
    
_ALLOWED_ID = re.compile(r'^[A-Za-z0-9_-]{5,1024}$')  # for sanity checks only
_ALLOWED_TAG_CHARS = re.compile(r'[^A-Za-z0-9_-]')  

def _safe_sku(sku: Optional[str]) -> str:
    return _ALLOWED_TAG_CHARS.sub("_", (sku or "sku").strip())

def _to_datetime(val, default_tz=None) -> Optional[datetime]:
    """
    Accepts a datetime or ISO8601 string and returns a tz-aware datetime.
    If string has 'Z' → convert to +00:00. If no tz, attach default_tz (if provided).
    Returns None if val is falsy.
    """
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)  # handles '+10:00' offsets
    if dt.tzinfo is None and default_tz is not None:
        dt = dt.replace(tzinfo=default_tz)
    return dt
    
# ----- Use item-level recurrence for items with NO events -----
def _normalize_value(v):
    """Convert numeric strings to int/float where possible."""
    if isinstance(v, str):
        try:
            if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
                return int(v)
            return float(v)
        except Exception:
            return v  # leave as string if not numeric
    return v

def _normalize(value):
    """Recursively normalize to JSON-safe types."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value