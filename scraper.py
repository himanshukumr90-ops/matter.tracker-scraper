"""
MatterTracker — PHHC Display Board Scraper
==========================================
This script runs 24/7 on Railway.app and does the following every 30 seconds:
1. Scrapes the Punjab & Haryana High Court display board
2. Parses all 69 courts and their current SR (item) numbers
3. Writes the data into Base44's CourtStatus table
4. Checks all tracked cases and logs notifications when thresholds are hit

Author: Built with Claude for MatterTracker
"""

import requests
import datetime
import time
import re

# ============================================================
# CONFIGURATION — Replace these with your real values
# on Railway.app you will set these as Environment Variables
# DO NOT paste your real keys here — use env variables
# ============================================================
import os

APP_ID  = os.environ.get("BASE44_APP_ID",  "YOUR_APP_ID_HERE")
API_KEY = os.environ.get("BASE44_API_KEY", "YOUR_API_KEY_HERE")

# Base44 API base URL
BASE44_URL = f"https://preview--matter-track-pro.base44.app/api/apps/{APP_ID}/entities"

# Court display board URL
DISPLAY_BOARD_URL = "https://livedb9010.digitalls.in/display_board/public/getRecords?skip=0&limit=500"

# How often to scrape (seconds) — board refreshes every 30s
SCRAPE_INTERVAL = 30

# Notification thresholds
THRESHOLDS = [15, 10, 5]

# Headers for Base44 API
HEADERS = {
    "api_key": API_KEY,
    "Content-Type": "application/json"
}


# ============================================================
# STEP 1 — SCRAPE THE DISPLAY BOARD
# ============================================================

def scrape_display_board():
    """
    Fetches the PHHC display board from the new JSON API.
    Returns a dict of:
    { court_number (int): { "current_item": int, "is_passover": bool,
                            "passover_current": int, "passover_total": int } }
    Returns None if fetch fails, empty dict if court not in session.
    """
    headers = {
        "Referer": "https://new.phhc.gov.in/",
        "Origin": "https://new.phhc.gov.in",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        response = requests.get(DISPLAY_BOARD_URL, headers=headers, timeout=15)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch display board: {e}")
        return None

    records = payload.get("data", [])

    if not records:
        print("[INFO] Court is not in session.")
        return {}

    court_data = {}
    for record in records:
        try:
            court_number = int(record["court_no"])
            sr_raw = str(record["sr_no"]).strip()

            is_passover = False
            passover_current = None
            passover_total = None
            current_item = 0

            if "-P(" in sr_raw:
                is_passover = True
                parts = sr_raw.split("-P(")
                current_item = int(parts[0])
                p_nums = parts[1].rstrip(")").split("/")
                passover_current = int(p_nums[0])
                passover_total = int(p_nums[1])
            elif "-S(" in sr_raw:
                is_passover = True
                parts = sr_raw.split("-S(")
                current_item = int(parts[0])
                p_nums = parts[1].rstrip(")").split("/")
                passover_current = int(p_nums[0])
                passover_total = int(p_nums[1])
            else:
                current_item = int(re.sub(r"[^\d]", "", sr_raw) or 0)

            court_data[court_number] = {
                "current_item": current_item,
                "is_passover": is_passover,
                "passover_current": passover_current,
                "passover_total": passover_total
            }
        except (ValueError, KeyError):
            continue

    print(f"[INFO] Scraped {len(court_data)} courts from display board.")
    return court_data


# ============================================================
# STEP 2 — GET EXISTING COURTSTATUS RECORDS FROM BASE44
# ============================================================

def get_existing_court_records():
    """
    Fetches all existing CourtStatus records from Base44.
    Returns a dict of { court_number (int): record_id (str) }
    so we know whether to POST (create) or PUT (update).
    """
    try:
        response = requests.get(
            f"{BASE44_URL}/CourtStatus",
            headers=HEADERS,
            timeout=15
        )
        response.raise_for_status()
        records = response.json()

        existing = {}
        for record in records:
            cn = record.get("court_number")
            rid = record.get("_id") or record.get("id")
            if cn and rid:
                existing[int(cn)] = rid

        return existing

    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch existing CourtStatus records: {e}")
        return {}


# ============================================================
# STEP 3 — WRITE COURT DATA TO BASE44
# ============================================================

def update_court_status(court_data, existing_records):
    """
    For each court in court_data:
    - If a record already exists for that court → PUT (update)
    - If no record exists yet → POST (create)
    Also marks all courts as inactive if court_data is empty (not in session).
    """
    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    is_session_active = len(court_data) > 0

    # If court is not in session — mark all existing records as inactive
    if not is_session_active:
        for court_number, record_id in existing_records.items():
            payload = {
                "is_active": False,
                "last_updated": now
            }
            try:
                r = requests.put(
                    f"{BASE44_URL}/CourtStatus/{record_id}",
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )
                if r.status_code != 200:
                    print(f"[WARN] Could not mark court {court_number} inactive: {r.text}")
            except requests.RequestException as e:
                print(f"[ERROR] Court {court_number} inactive update failed: {e}")
        print("[INFO] All courts marked as inactive (not in session).")
        return

    # Court is in session — update each court
    for court_number, data in court_data.items():
        payload = {
            "court_number": court_number,
            "current_item": data["current_item"],
            "is_passover": data["is_passover"],
            "passover_current": data["passover_current"],
            "passover_total": data["passover_total"],
            "court_date": today,
            "last_updated": now,
            "is_active": True
        }

        try:
            if court_number in existing_records:
                # Update existing record
                record_id = existing_records[court_number]
                r = requests.put(
                    f"{BASE44_URL}/CourtStatus/{record_id}",
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )
            else:
                # Create new record
                r = requests.post(
                    f"{BASE44_URL}/CourtStatus",
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )

            if r.status_code == 200:
                pass  # Silent success to avoid log spam
            else:
                print(f"[WARN] Court {court_number} write failed: {r.status_code} {r.text}")

        except requests.RequestException as e:
            print(f"[ERROR] Court {court_number} write error: {e}")

    print(f"[INFO] Updated {len(court_data)} court records in Base44.")


# ============================================================
# STEP 4 — CHECK TRACKED CASES AND LOG NOTIFICATIONS
# ============================================================

def get_tracked_cases():
    """
    Fetches all TrackedCase records from Base44 where status is 'pending'.
    Returns a list of case records.
    """
    try:
        response = requests.get(
            f"{BASE44_URL}/TrackedCase",
            headers=HEADERS,
            timeout=15
        )
        response.raise_for_status()
        all_cases = response.json()

        # Only process pending cases for today's date
        today = datetime.date.today().isoformat()
        active_cases = [
            c for c in all_cases
            if c.get("status") == "pending"
            and c.get("case_date", "") == today
            and c.get("notifications_enabled", True)
            and c.get("court_number") is not None
            and c.get("item_number") is not None
        ]

        return active_cases

    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch TrackedCase records: {e}")
        return []


def check_notifications(court_data, existing_records):
    """
    For each active tracked case:
    - Calculate how many items away the case is
    - If within a threshold (15, 10, 5) and notification not yet sent → log it
    - If court has passed the item → mark case as 'called'
    - If court is in passover mode → log a passover alert
    """
    if not court_data:
        return  # Court not in session, nothing to check

    cases = get_tracked_cases()
    if not cases:
        return

    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    for case in cases:
        court_number = case.get("court_number")
        item_number = case.get("item_number")
        case_id = case.get("_id") or case.get("id")
        user_id = case.get("user_id") or case.get("created_by")

        if court_number not in court_data:
            continue  # This court has no data yet

        court = court_data[court_number]
        current_item = court["current_item"]
        is_passover = court["is_passover"]

        # --- PASSOVER MODE ALERT ---
        if is_passover and case.get("notify_at_15", True):
            # Only send passover alert once per session
            # We check NotificationLog to avoid duplicates (simplified here)
            log_notification(
                user_id=user_id,
                case_id=case_id,
                notification_type="passover_alert",
                message=(
                    f"Court {court_number} is in Passover Mode. "
                    f"Your case (Item {item_number}) may be called soon. "
                    f"Please be on standby."
                ),
                now=now
            )
            continue  # Don't calculate distance during passover

        # --- ITEMS AWAY CALCULATION ---
        items_away = item_number - current_item

        # Case has already been called or passed
        if items_away <= 0:
            update_case_status(case_id, "called", now)
            log_notification(
                user_id=user_id,
                case_id=case_id,
                notification_type="case_called",
                message=(
                    f"Your case (Item {item_number}) in Court {court_number} "
                    f"is being called or has been called."
                ),
                now=now
            )
            continue

        # Check each threshold
        for threshold in THRESHOLDS:
            flag_field = f"notify_at_{threshold}"
            if items_away <= threshold and case.get(flag_field, True):
                log_notification(
                    user_id=user_id,
                    case_id=case_id,
                    notification_type=f"{threshold}_away",
                    message=(
                        f"Your case in Court {court_number} is {items_away} items away. "
                        f"Court is currently on Item {current_item}. "
                        f"Your case is Item {item_number}."
                    ),
                    now=now
                )
                # Mark this threshold as sent so it doesn't fire again
                mark_notification_sent(case_id, flag_field, now)
                break  # Only fire the most urgent threshold per cycle


def log_notification(user_id, case_id, notification_type, message, now):
    """
    Writes a notification record to Base44's NotificationLog table.
    The app and webtoapp.design will read these to send push notifications.
    """
    payload = {
        "user_id": user_id,
        "case_id": case_id,
        "notification_type": notification_type,
        "message": message,
        "sent_at": now
    }
    try:
        r = requests.post(
            f"{BASE44_URL}/NotificationLog",
            headers=HEADERS,
            json=payload,
            timeout=15
        )
        if r.status_code == 200:
            print(f"[NOTIFICATION] {notification_type} logged for case {case_id}")
        else:
            print(f"[WARN] Notification log failed: {r.status_code} {r.text}")
    except requests.RequestException as e:
        print(f"[ERROR] Notification log error: {e}")


def mark_notification_sent(case_id, flag_field, now):
    """
    Sets the notification flag to False on the TrackedCase record
    so the same threshold doesn't fire twice in one day.
    """
    payload = {
        flag_field: False,
        "last_updated": now
    }
    try:
        r = requests.put(
            f"{BASE44_URL}/TrackedCase/{case_id}",
            headers=HEADERS,
            json=payload,
            timeout=15
        )
    except requests.RequestException as e:
        print(f"[ERROR] Could not mark notification sent: {e}")


def update_case_status(case_id, status, now):
    """Updates a TrackedCase record's status field."""
    payload = {"status": status, "last_updated": now}
    try:
        requests.put(
            f"{BASE44_URL}/TrackedCase/{case_id}",
            headers=HEADERS,
            json=payload,
            timeout=15
        )
    except requests.RequestException as e:
        print(f"[ERROR] Could not update case status: {e}")


# ============================================================
# STEP 5 — RESET NOTIFICATION FLAGS EACH NEW COURT DAY
# ============================================================

def reset_daily_flags():
    """
    Called once at the start of each court day (first scrape after midnight).
    Resets all notify_at_15, notify_at_10, notify_at_5 flags to True
    and all case statuses back to 'pending' so notifications fire fresh.
    """
    print("[INFO] Resetting daily notification flags...")
    try:
        response = requests.get(
            f"{BASE44_URL}/TrackedCase",
            headers=HEADERS,
            timeout=15
        )
        response.raise_for_status()
        cases = response.json()

        now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'

        for case in cases:
            case_id = case.get("_id") or case.get("id")
            payload = {
                "notify_at_15": True,
                "notify_at_10": True,
                "notify_at_5": True,
                "status": "pending",
                "last_updated": now
            }
            requests.put(
                f"{BASE44_URL}/TrackedCase/{case_id}",
                headers=HEADERS,
                json=payload,
                timeout=15
            )

        print(f"[INFO] Reset {len(cases)} case flags for new day.")

    except requests.RequestException as e:
        print(f"[ERROR] Could not reset daily flags: {e}")


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    print("=" * 50)
    print("MatterTracker Scraper — Starting")
    print(f"App ID: {APP_ID[:8]}... (truncated for security)")
    print(f"Scraping every {SCRAPE_INTERVAL} seconds")
    print("=" * 50)

    last_run_date = None

    while True:
        try:
            current_date = datetime.date.today()

            # Reset flags at the start of each new day
            if last_run_date != current_date:
                reset_daily_flags()
                last_run_date = current_date

            # --- SCRAPE ---
            court_data = scrape_display_board()

            if court_data is None:
                # Network error — wait and retry
                print("[WARN] Scrape failed, retrying in 30 seconds...")
                time.sleep(SCRAPE_INTERVAL)
                continue

            # --- GET EXISTING RECORDS ---
            existing_records = get_existing_court_records()

            # --- UPDATE BASE44 ---
            update_court_status(court_data, existing_records)

            # --- CHECK NOTIFICATIONS ---
            check_notifications(court_data, existing_records)

            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Cycle complete. "
                  f"Waiting {SCRAPE_INTERVAL}s...")

        except Exception as e:
            # Catch-all so the script never crashes permanently
            print(f"[CRITICAL] Unexpected error: {e}. Continuing in 30s...")

        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
