from dateutil import parser
from datetime import timezone

class DateUtils:
    @staticmethod
    def format_date_to_iso(date_str):
        dt_obj = parser.parse(date_str)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')