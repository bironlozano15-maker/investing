from datetime import datetime, timezone


def from_iso_to_unix_time(iso_time: str):
    # Convert to a datetime object
    dt = datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)

    # Convert to Unix time
    return int(dt.timestamp())
