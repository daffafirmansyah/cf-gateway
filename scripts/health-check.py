#!/usr/bin/env python3
"""CF Gateway health check — auto-restart if stale or stuck."""
import json, subprocess, sys, time
from urllib.request import urlopen

HEALTH_URL = "http://127.0.0.1:8750/health"
STALE_THRESHOLD_SEC = 600  # 10 min no success = restart
STUCK_INFLIGHT_THRESHOLD = 3  # if in_flight >= max_concurrent for too long = stuck

def get_health():
    try:
        with urlopen(HEALTH_URL, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"HEALTH_CHECK: fetch failed — {e}")
        return None

def restart_service():
    print("HEALTH_CHECK: restarting cf-gateway.service")
    subprocess.run(["systemctl", "restart", "cf-gateway.service"], check=False)
    time.sleep(3)
    result = subprocess.run(["systemctl", "is-active", "cf-gateway.service"],
                          capture_output=True, text=True)
    print(f"HEALTH_CHECK: restart result — {result.stdout.strip()}")
    return result.stdout.strip() == "active"

def main():
    h = get_health()
    if not h:
        return restart_service()

    status = h.get("status", "unknown")
    last_success = h.get("last_success")
    inflight = h.get("concurrency", {}).get("in_flight", 0)
    max_concurrent = h.get("concurrency", {}).get("max", 4)
    queued = h.get("concurrency", {}).get("queued", 0)
    available = h.get("accounts", {}).get("available", 0)

    # If no accounts available — can't fix by restart, skip
    if available == 0:
        print(f"HEALTH_CHECK: 0 accounts available — CF side issue, no restart")
        return True

    # Check stale last_success
    if last_success:
        from datetime import datetime, timezone
        try:
            ls = datetime.fromisoformat(last_success.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            stale_sec = (now - ls).total_seconds()
            if stale_sec > STALE_THRESHOLD_SEC and inflight >= max_concurrent:
                print(f"HEALTH_CHECK: stale ({int(stale_sec)}s) + slots full ({inflight}/{max_concurrent}) — STUCK")
                return restart_service()
        except Exception:
            pass

    # Check all slots stuck
    if inflight >= max_concurrent and queued > 0:
        print(f"HEALTH_CHECK: all slots stuck ({inflight}/{max_concurrent}) + {queued} queued — STUCK")
        return restart_service()

    print(f"HEALTH_CHECK: OK — status={status} available={available} inflight={inflight}/{max_concurrent} queued={queued}")
    return True

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
