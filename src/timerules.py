from datetime import timedelta

class TimeRules:

    def __init__ (self, rules):
        self.rules = rules
        
    def _apply_times(self, dt, h, m):
        return dt.replace(hour=h, minute=m, second=0, microsecond=0)

    def _get_rule_for(self, category: str, sku:str ):
        
        #category = (calendarEvent.item.get("category") or "").strip()
        #calendarEvent.sku

        
        # SKU override wins if present (exact, case-insensitive)
        if sku:
            for key, rule in self.rules["sku_overrides"].items():
                if key.lower() == sku.lower():
                    return rule

        # Otherwise fall back to category default
        categoryrule = self.rules["default_by_category"].get(category)
    
        if categoryrule:
            return categoryrule
        else:
            return None

    def apply_time_rule(self, rule, start, end):

        if not rule:
            return start, end  # no change if nothing matches

        start = self._apply_times(start, rule["start_hour"], rule.get("start_minute", 0))
        end   = self._apply_times(end,   rule["end_hour"],   rule.get("end_minute", 0))

        # Handle overnight (end next day)
        if rule.get("overnight"):
            if end <= start:
                end += timedelta(days=1)
        else:
            # If a same-day rule accidentally crosses midnight, normalize
            if end <= start:
                end += timedelta(days=1)

        return start,end