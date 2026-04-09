"""
MatterTracker ‚Äî PHHC Display Board & Cause List Scraper
==========================================
This script runs 24/7 on Railway.app and does the following every 30 seconds:

1. Scrapes the Punjab & Haryana High Court display board
2. Parses all 69 courts and their current SR (item) numbers
3. Writes the data into Base44's CourtStatus table
4. Checks all tracked cases and logs notifications when thresholds are hit
5. Scrapes cause lists (ORDINARY/URGENT) for upcoming dates and stores entries

Author: Built with Claude for MatterTracker
"""

import requests
import datetime
import time
import re
import io
import pdfplumber
from datetime import timedelta, timezone

# ============================================================
# CONFIGURATION
# ============================================================

import os

APP_ID = os.environ.get("BASE44_APP_ID", "YOUR_APP_ID_HERE")
API_KEY = os.environ.get("BASE44_API_KEY", "YOUR_API_KEY_HERE")

BASE44_URL = f"https://preview--matter-track-pro.base44.app/api/apps/{APP_ID}/entities"
DISPLAY_BOARD_URL = "https://livedb9010.digitalls.in/display_board/public/getRecords?skip=0&limit=500"
SCRAPE_INTERVAL = 30
THRESHOLDS = [15, 10, 5]
HEADERS = {
    "api_key": API_KEY,
    "Content-Type": "application/json"
}

# Cause list config
IST = timezone(timedelta(hours=5, minutes=30))
CAUSELIST_SUMMARY_URL = "https://livedb9010.digitalls.in/cis_filing/public/getCauseListSummary"
CAUSELIST_PAGE_URL = "https://new.phhc.gov.in/cause/daily-cause-list"
CAUSELIST_PDF_URL = "https://new.phhc.gov.in/api/causelist/daily-cause-list-pdf"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ============================================================
# STEP 1 ‚Äî SCRAPE THE DISPLAY BOARD
# ============================================================

def scrape_display_board():
    """
    Fetches the PHHC display board from the new JSON API.
    Returns a dict of: { court_number (int): { ... } }
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
# STEP 2 ‚Äî GET EXISTING COURTSTATUS RECORDS FROM BASE44
# ============================================================

def get_existing_court_records():
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
# STEP 3 ‚Äî WRITE COURT DATA TO BASE44
# ============================================================

def update_court_status(court_data, existing_records):
    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    is_session_active = len(court_data) > 0

    if not is_session_active:
        for court_number, record_id in existing_records.items():
            payload = {"is_active": False, "last_updated": now}
            try:
                r = requests.put(
                    f"{BASE44_URL}/CourtStatus/{record_id}",
                    headers=HEADERS, json=payload, timeout=15
                )
                if r.status_code != 200:
                    print(f"[WARN] Could not mark court {court_number} inactive: {r.text}")
            except requests.RequestException as e:
                print(f"[ERROR] Court {court_number} inactive update failed: {e}")
        print("[INFO] All courts marked as inactive (not in session).")
        return

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
                record_id = existing_records[court_number]
                r = requests.put(
                    f"{BASE44_URL}/CourtStatus/{record_id}",
                    headers=HEADERS, json=payload, timeout=15
                )
            else:
                r = requests.post(
                    f"{BASE44_URL}/CourtStatus",
                    headers=HEADERS, json=payload, timeout=15
                )
            if r.status_code != 200:
                print(f"[WARN] Court {court_number} write failed: {r.status_code} {r.text}")
        except requests.RequestException as e:
            print(f"[ERROR] Court {court_number} write error: {e}")

    print(f"[INFO] Updated {len(court_data)} court records in Base44.")


# ============================================================
# STEP 4 ‚Äî CHECK TRACKED CASES AND LOG NOTIFICATIONS
# ============================================================

def get_tracked_cases():
    try:
        response = requests.get(
            f"{BASE44_URL}/TrackedCase",
            headers=HEADERS, timeout=15
        )
        response.raise_for_status()
        all_cases = response.json()
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
    if not court_data:
        return

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
            continue

        court = court_data[court_number]
        current_item = court["current_item"]
        is_passover = court["is_passover"]

        if is_passover and case.get("notify_at_15", True):
            log_notification(
                user_id=user_id, case_id=case_id,
                notification_type="passover_alert",
                message=(f"Court {court_number} is in Passover Mode. "
                         f"Your case (Item {item_number}) may be called soon. "
                         f"Please be on standby."),
                now=now
            )
            continue

        items_away = item_number - current_item

        if items_away <= 0:
            update_case_status(case_id, "called", now)
            log_notification(
                user_id=user_id, case_id=case_id,
                notification_type="case_called",
                message=(f"Your case (Item {item_number}) in Court {court_number} "
                         f"is being called or has been called."),
                now=now
            )
            continue

        for threshold in THRESHOLDS:
            flag_field = f"notify_at_{threshold}"
            if items_away <= threshold and case.get(flag_field, True):
                log_notification(
                    user_id=user_id, case_id=case_id,
                    notification_type=f"{threshold}_away",
                    message=(f"Your case in Court {court_number} is {items_away} items away. "
                             f"Court is currently on Item {current_item}. "
                             f"Your case is Item {item_number}."),
                    now=now
                )
                mark_notification_sent(case_id, flag_field, now)
                break


def log_notification(user_id, case_id, notification_type, message, now):
    payload = {
        "user_id": user_id, "case_id": case_id,
        "notification_type": notification_type,
        "message": message, "sent_at": now
    }
    try:
        r = requests.post(
            f"{BASE44_URL}/NotificationLog",
            headers=HEADERS, json=payload, timeout=15
        )
        if r.status_code == 200:
            print(f"[NOTIFICATION] {notification_type} logged for case {case_id}")
        else:
            print(f"[WARN] Notification log failed: {r.status_code} {r.text}")
    except requests.RequestException as e:
        print(f"[ERROR] Notification log error: {e}")


def mark_notification_sent(case_id, flag_field, now):
    payload = {flag_field: False, "last_updated": now}
    try:
        requests.put(
            f"{BASE44_URL}/TrackedCase/{case_id}",
            headers=HEADERS, json=payload, timeout=15
        )
    except requests.RequestException as e:
        print(f"[ERROR] Could not mark notification sent: {e}")


def update_case_status(case_id, status, now):
    payload = {"status": status, "last_updated": now}
    try:
        requests.put(
            f"{BASE44_URL}/TrackedCase/{case_id}",
            headers=HEADERS, json=payload, timeout=15
        )
    except requests.RequestException as e:
        print(f"[ERROR] Could not update case status: {e}")


# ============================================================
# STEP 5 ‚Äî RESET NOTIFICATION FLAGS EACH NEW COURT DAY
# ============================================================

def reset_daily_flags():
    print("[INFO] Resetting daily notification flags...")
    try:
        response = requests.get(
            f"{BASE44_URL}/TrackedCase",
            headers=HEADERS, timeout=15
        )
        response.raise_for_status()
        cases = response.json()
        now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        for case in cases:
            case_id = case.get("_id") or case.get("id")
            payload = {
                "notify_at_15": True, "notify_at_10": True,
                "notify_at_5": True, "status": "pending",
                "last_updated": now
            }
            requests.put(
                f"{BASE44_URL}/TrackedCase/{case_id}",
                headers=HEADERS, json=payload, timeout=15
            )
        print(f"[INFO] Reset {len(cases)} case flags for new day.")
    except requests.RequestException as e:
        print(f"[ERROR] Could not reset daily flags: {e}")


# ============================================================
# STEP 6 ‚Äî CAUSE LIST SCRAPING
# ============================================================

def get_csrt_token():
    """Obtain a fresh csrt token from the PHHC cause list page."""
    try:
        session = requests.Session()
        resp = session.get(CAUSELIST_PAGE_URL, headers=BROWSER_HEADERS, timeout=30)
        resp.raise_for_status()
        match = re.search(r'<input[^>]+name=["\']csrt["\'][^>]+value=["\']([^"\']+)["\']', resp.text)
        if not match:
            match = re.search(r'<input[^>]+value=["\']([^"\']+)["\'][^>]+name=["\']csrt["\']', resp.text)
        if match:
            csrt = match.group(1)
            print(f"[CAUSELIST] Got csrt token: {csrt[:10]}...")
            return session, csrt
        else:
            print("[CAUSELIST] ERROR: csrt token not found in page HTML")
            return None, None
    except Exception as e:
        print(f"[CAUSELIST] ERROR getting csrt token: {e}")
        return None, None


def get_cause_list_summary(cl_date):
    """Fetch cause list summary for a given date."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://new.phhc.gov.in",
        "Referer": "https://new.phhc.gov.in/",
    }
    try:
        resp = requests.get(
            CAUSELIST_SUMMARY_URL,
            params={"cl_date": cl_date, "skip": 0, "limit": 100},
            headers=headers, timeout=30,
        )
        if resp.status_code != 200:
            return []
        return resp.json()
    except Exception as e:
        print(f"[CAUSELIST] Summary API error for {cl_date}: {e}")
        return []


def download_cause_list_pdf(session, csrt, cl_date, list_type, main_suppl):
    """Download a cause list PDF. Returns bytes or None."""
    date_underscored = cl_date.replace("-", "_")
    params = {
        "cl_date": date_underscored,
        "list_type": list_type,
        "main_suppl": main_suppl,
        "csrt": csrt,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*",
        "Referer": "https://new.phhc.gov.in/cause/daily-cause-list",
    }
    try:
        resp = session.get(CAUSELIST_PDF_URL, params=params, headers=headers, timeout=120)
        if resp.status_code == 200 and len(resp.content) > 1000:
            print(f"[CAUSELIST] Downloaded PDF: {len(resp.content)} bytes")
            return resp.content
        else:
            print(f"[CAUSELIST] PDF download failed: status={resp.status_code}, size={len(resp.content)}")
            return None
    except Exception as e:
        print(f"[CAUSELIST] PDF download error: {e}")
        return None


def parse_cause_list_pdf(pdf_bytes, list_date, list_type_name):
    """Parse a cause list PDF and extract all case entries."""
    entries = []
    current_court = None

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split("\n")
                for i, raw_line in enumerate(lines):
                    line = raw_line.strip()

                    # Court header: "CR NO X" or "CR NO. X"
                    court_match = re.search(r'CR\s+NO\.?\s*(\d+)', line, re.IGNORECASE)
                    if court_match:
                        current_court = int(court_match.group(1))
                        continue

                    # Case entry (starts with 3-digit item number)
                    entry_match = re.match(r'^(\d{3})\s+', line)
                    if entry_match and current_court is not None:
                        item_number = int(entry_match.group(1))
                        if 100 <= item_number <= 999:
                            entry = parse_entry_line(
                                line, lines, i, item_number,
                                current_court, list_date, list_type_name
                            )
                            if entry:
                                entries.append(entry)
    except Exception as e:
        print(f"[CAUSELIST] PDF parse error: {e}")

    return entries


def parse_entry_line(line, lines, line_idx, item_number, court_number, list_date, list_type_name):
    """Parse a single cause list entry line."""
    try:
        remainder = re.sub(r'^\d{3}\s+', '', line)

        # Bench type
        bench_match = re.match(r'^(I{1,3}|IV|V)\s+', remainder)
        bench_type = ""
        if bench_match:
            bench_type = bench_match.group(1)
            remainder = remainder[bench_match.end():]

        # Case number: TYPE-NUMBER-YEAR
        case_match = re.search(r'([A-Z][A-Z0-9]*(?:-[A-Z]+)*)-(\d+)-(\d{4})', remainder)
        if not case_match:
            return None

        full_case_number = case_match.group(0)
        case_start = case_match.start()
        case_end = case_match.end()

        district = remainder[:case_start].strip().rstrip("-").strip()
        parties = remainder[case_end:].strip()

        # Gather continuation lines
        j = line_idx + 1
        while j < len(lines):
            next_line = lines[j].strip()
            if re.match(r'^\d{3}\s+', next_line):
                break
            if re.search(r'CR\s+NO\.?\s*\d+', next_line, re.IGNORECASE):
                break
            if re.match(r'^HON\'?BLE', next_line, re.IGNORECASE):
                break
            if re.match(r'^DAILY\s+', next_line, re.IGNORECASE):
                break
            if next_line and not next_line.startswith('[') and not next_line.startswith('('):
                parties += " " + next_line
            j += 1

        # Split case number
        parts = full_case_number.split("-")
        case_year = int(parts[-1])
        case_no = int(parts[-2])
        case_type = "-".join(parts[:-2])

        now = datetime.datetime.now(IST).isoformat(timespec='seconds')

        return {
            "case_number": full_case_number.upper(),
            "case_type": case_type,
            "case_no": case_no,
            "case_year": case_year,
            "court_number": court_number,
            "item_number": item_number,
            "list_date": list_date,
            "list_type": list_type_name,
            "bench_type": bench_type,
            "district": district,
            "parties": re.sub(r'\s+', ' ', parties).strip()[:500],
            "downloaded_at": now,
        }
    except Exception as e:
        return None


def check_existing_cause_list(list_date, list_type):
    """Check if CauseListEntry records already exist for this date+type."""
    try:
        resp = requests.get(
            f"{BASE44_URL}/CauseListEntry",
            headers=HEADERS,
            params={"list_date": list_date, "list_type": list_type},
            timeout=15,
        )
        if resp.status_code == 200:
            records = resp.json()
            if isinstance(records, list) and len(records) > 0:
                return True
        return False
    except Exception as e:
        print(f"[CAUSELIST] Check existing error: {e}")
        return False


def store_cause_list_entries(entries):
    """Write parsed entries to Base44 CauseListEntry entity."""
    stored = 0
    failed = 0
    for entry in entries:
        try:
            r = requests.post(
                f"{BASE44_URL}/CauseListEntry",
                headers=HEADERS, json=entry, timeout=15,
            )
            if r.status_code == 200:
                stored += 1
            else:
                failed += 1
                if failed <= 3:
                    print(f"[CAUSELIST] Store failed: {r.status_code} {r.text[:200]}")
        except Exception as e:
            failed += 1
            if failed <= 3:
                print(f"[CAUSELIST] Store error: {e}")
    print(f"[CAUSELIST] Stored {stored} entries, {failed} failures")
    return stored


def scrape_cause_lists():
    """Main cause list scraping function. Checks today through today+3."""
    print("[CAUSELIST] Starting cause list check...")
    now_ist = datetime.datetime.now(IST)
    dates_to_check = [(now_ist.date() + timedelta(days=d)).isoformat() for d in range(4)]

    session = None
    csrt = None

    for cl_date in dates_to_check:
        try:
            summary = get_cause_list_summary(cl_date)
            if not summary:
                continue

            for item in summary:
                list_type_name = item.get("list_type_name", "")
                main_suppl = item.get("main_suppl", "")

                if list_type_name not in ("ORDINARY", "URGENT"):
                    continue
                if main_suppl != "M":
                    continue

                if check_existing_cause_list(cl_date, list_type_name):
                    print(f"[CAUSELIST] {list_type_name} {cl_date} already stored, skipping")
                    continue

                if session is None or csrt is None:
                    session, csrt = get_csrt_token()
                    if not csrt:
                        print("[CAUSELIST] Cannot proceed without csrt token")
                        return

                lt = "o" if list_type_name == "ORDINARY" else "u"
                pdf_bytes = download_cause_list_pdf(session, csrt, cl_date, lt, "m")
                if not pdf_bytes:
                    continue

                entries = parse_cause_list_pdf(pdf_bytes, cl_date, list_type_name)
                if not entries:
                    print(f"[CAUSELIST] No entries parsed from {list_type_name} {cl_date}")
                    continue

                courts = set(e["court_number"] for e in entries)
                print(f"[CAUSELIST] {list_type_name} {cl_date}: {len(entries)} entries across {len(courts)} courts")
                store_cause_list_entries(entries)

        except Exception as e:
            print(f"[CAUSELIST] Error processing {cl_date}: {e}")
            continue

    print("[CAUSELIST] Cause list check complete")


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    print("=" * 50)
    print("MatterTracker Scraper ‚Äî Starting")
    print(f"App ID: {APP_ID[:8]}... (truncated for security)")
    print(f"Scraping every {SCRAPE_INTERVAL} seconds")
    print("=" * 50)

    last_run_date = None

    while True:
        try:
            current_date = datetime.date.today()

            if last_run_date != current_date:
                reset_daily_flags()
                last_run_date = current_date

            # --- SCRAPE DISPLAY BOARD ---
            court_data = scrape_display_board()
            if court_data is None:
                print("[WARN] Scrape failed, retrying in 30 seconds...")
                time.sleep(SCRAPE_INTERVAL)
                continue

            existing_records = get_existing_court_records()
            update_court_status(court_data, existing_records)
            check_notifications(court_data, existing_records)

            # --- SCRAPE CAUSE LISTS (once per cycle) ---
            try:
                scrape_cause_lists()
            except Exception as e:
                print(f"[CAUSELIST] Unexpected error: {e}")

            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Cycle complete. "
                  f"Waiting {SCRAPE_INTERVAL}s...")

        except Exception as e:
            print(f"[CRITICAL] Unexpected error: {e}. Continuing in 30s...")

        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
