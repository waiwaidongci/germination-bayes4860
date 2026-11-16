from datetime import datetime, timezone
def now():
    return datetime.now(timezone.utc).isoformat()
def iso(dt):
    if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
def parse(value):
    # 兼容末尾Z与无时区的UTC ISO字符串
    if value is None:return None
    text=value.replace("Z","+00:00")
    dt=datetime.fromisoformat(text)
    if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
    return dt
def lease_expired(lease_until, at=None):
    if not lease_until:return True
    at=at or datetime.now(timezone.utc)
    return parse(lease_until)<=at
