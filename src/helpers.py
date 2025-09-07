from datetime import datetime, date, timedelta

def _datetime_or_none(v, tz) -> datetime:
    try:
        naive_dt = datetime.strptime(v, "%Y%m%d")
        return naive_dt.replace(tzinfo=tz)
    except Exception:
        return None
    
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