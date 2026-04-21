"""
MatterTracker ‚Äö√Ñ√∂‚àö√ë‚àö√Ü PHHC Display Board & Cause List Scraper
==========================================
This script runs 24/7 on Railway.app and does the following every 30 seconds:
1. Scrapes the Punjab & Haryana High Court display board
2. Parses all 69 courts and their current SR (item) numbers
3. Writes the data into Base44's CourtStatus table
4. Checks all tracked cases and logs notifications when thresholds are hit
5. Scrapes cause lists via direct JSON API (no PDF, no Cloudflare) and stores entries

Author: Built with Claude for MatterTracker
"""
import requests
import datetime
import time
import re
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

# Cause list config ‚Äö√Ñ√∂‚àö√ë‚àö√Ü uses livedb9010.digitalls.in (no Cloudflare, accessible from Railway)
IST = timezone(timedelta(hours=5, minutes=30))
LIVEDB_BASE = "https://livedb9010.digitalls.in"
CAUSELIST_SUMMARY_URL = f"{LIVEDB_BASE}/cis_filing/public/getCauseListSummary"
CAUSELIST_ENTRIES_URL = f"{LIVEDB_BASE}/cis_filing/public/getCauseList"
ACTIVE_BENCH_URL = f"{LIVEDB_BASE}/cis/judges/active-bench"

LIVEDB_HEADERS = {
    "Accept": "application/json",
    "Origin": "https://new.phhc.gov.in",
    "Referer": "https://new.phhc.gov.in/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}

# Map cl_type codes to human-readable names
CL_TYPE_NAMES = {
    'U': 'URGENT',
    'O': 'ORDINARY',
    'K': 'LOK ADALAT',
    'S': 'SPECIAL',
    'E': 'ELECTION',
    'Q': 'LIQUIDATION (URGENT)',
    'L': 'LIQUIDATION (ORDINARY)',
    'F': 'COMMERCIAL (URGENT)',
    'G': 'COMMERCIAL (ORDINARY)',
    'T': 'TAKENUP',
    'Y': 'FOR-ORDER',
    'V': 'OLD-CASES',
    'M': 'MEDIATION DRIVE',
    'R': 'REGULAR',
    'A': 'PRE LOK ADALAT',
}

# In-memory cache for cause list entries. Built once from Base44 at first
# access and kept in sync with writes. We track two things:
#   * _cause_list_counts: count of records per (list_date, list_type).
#     Used for completeness detection - a (date, type) only counts as
#     "already stored" when it has at least MIN_CAUSE_LIST_RECORDS entries.
#     This protects against a prior buggy scraper having stored only a
#     handful of division-bench-only records, which a simple presence
#     check would mistake for a complete list.
#   * _cause_list_keys: set of (date, type, case_number, court_no, item_no)
#     tuples. Used for per-entry dedup when refetching an incomplete list
#     so we never POST a duplicate of an entry that's already in Base44.
_cause_list_counts = {}
_cause_list_keys = set()
_cache_initialized = False
# A (date, type) pair below this record count is treated as incomplete and
# will be refetched. A normal URGENT or ORDINARY day has hundreds of entries
# across 60+ court rooms, so 50 is well above the ~10-15 that the old buggy
# scraper was storing, and well below any real full list.
MIN_CAUSE_LIST_RECORDS = 50

# ============================================================
# STEP 1 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü SCRAPE THE DISPLAY BOARD
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
# STEP 2 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü GET EXISTING COURTSTATUS RECORDS FROM BASE44
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
# STEP 3 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü WRITE COURT DATA TO BASE44
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
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )
            else:
                r = requests.post(
                    f"{BASE44_URL}/CourtStatus",
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )
            if r.status_code != 200:
                print(f"[WARN] Court {court_number} write failed: {r.status_code} {r.text}")
        except requests.RequestException as e:
            print(f"[ERROR] Court {court_number} write error: {e}")

    print(f"[INFO] Updated {len(court_data)} court records in Base44.")


# ============================================================
# STEP 4 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü CHECK TRACKED CASES AND LOG NOTIFICATIONS
# ============================================================
def get_tracked_cases():
    try:
        response = requests.get(
            f"{BASE44_URL}/TrackedCase",
            headers=HEADERS,
            timeout=15
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
                user_id=user_id,
                case_id=case_id,
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
                user_id=user_id,
                case_id=case_id,
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
                    user_id=user_id,
                    case_id=case_id,
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
    payload = {flag_field: False, "last_updated": now}
    try:
        requests.put(
            f"{BASE44_URL}/TrackedCase/{case_id}",
            headers=HEADERS,
            json=payload,
            timeout=15
        )
    except requests.RequestException as e:
        print(f"[ERROR] Could not mark notification sent: {e}")


def update_case_status(case_id, status, now):
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
# STEP 5 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü RESET NOTIFICATION FLAGS EACH NEW COURT DAY
# ============================================================
def reset_daily_flags():
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
# STEP 6 ‚Äö√Ñ√∂‚àö√ë‚àö√Ü CAUSE LIST SCRAPING (JSON API ‚Äö√Ñ√∂‚àö√ë‚àö√Ü no PDF, no Cloudflare)
# ============================================================

def _load_cause_list_cache():
    """Load existing CauseListEntry records from Base44 into the in-memory
    counters and key set. Runs once per process start. We count records per
    (list_date, list_type) so check_existing_cause_list can detect an
    incomplete list and trigger a refetch, and we index every record by
    (date, type, case_number, court_no, item_no) so store_cause_list_entries
    can skip duplicates during a refetch."""
    global _cache_initialized
    if _cache_initialized:
        return
    try:
        resp = requests.get(
            f"{BASE44_URL}/CauseListEntry",
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code == 200:
            records = resp.json()
            if isinstance(records, list):
                for r in records:
                    ld = r.get("list_date")
                    lt = r.get("list_type")
                    if not (ld and lt):
                        continue
                    _cause_list_counts[(ld, lt)] = _cause_list_counts.get((ld, lt), 0) + 1
                    key = (
                        ld,
                        lt,
                        r.get("case_number") or "",
                        r.get("court_number"),
                        str(r.get("item_number") or ""),
                    )
                    _cause_list_keys.add(key)
                print(f"[CAUSELIST] Loaded cache: {len(records)} records across "
                      f"{len(_cause_list_counts)} (date, type) pairs from Base44")
        _cache_initialized = True
    except Exception as e:
        print(f"[CAUSELIST] Cache load error: {e}")


def check_existing_cause_list(list_date, list_type):
    """Return True only if Base44 already holds a COMPLETE (enough) set of
    entries for this (date, type). 'Complete enough' means at least
    MIN_CAUSE_LIST_RECORDS records. This prevents a partially-populated
    list left behind by an earlier buggy run from blocking a fresh pull."""
    _load_cause_list_cache()
    return _cause_list_counts.get((list_date, list_type), 0) >= MIN_CAUSE_LIST_RECORDS


def get_cause_list_summary(cl_date):
    """Fetch cause list summary for a given date. Returns list of dicts or []."""
    try:
        resp = requests.get(
            CAUSELIST_SUMMARY_URL,
            params={"cl_date": cl_date, "skip": 0, "limit": 100},
            headers=LIVEDB_HEADERS,
            timeout=30,
        )
        if resp.status_code != 200:
            return []
        return resp.json()
    except Exception as e:
        print(f"[CAUSELIST] Summary API error for {cl_date}: {e}")
        return []


def get_active_bench_ids():
    """Fetch all active bench/judge IDs from PHHC. Returns list of judge_code ints."""
    try:
        resp = requests.get(ACTIVE_BENCH_URL, headers=LIVEDB_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        ids = [item['judge_code'] for item in data if item.get('bench_active') == 'Y']
        print(f"[CAUSELIST] Found {len(ids)} active bench IDs")
        return ids
    except Exception as e:
        print(f"[CAUSELIST] Error fetching bench IDs: {e}")
        return []


def fetch_cause_list_for_bench(bench_judge_id, date_str):
    """
    Fetch cause list JSON for a specific bench on a given date.
    Returns a list of bench-section dicts (each has 'header' and 'records').

    Retries on HTTP 429 (rate limit) with exponential backoff so that we don't
    silently drop entire benches when PHHC throttles us partway through the
    64-bench sweep.
    """
    for attempt in range(5):
        try:
            resp = requests.get(
                CAUSELIST_ENTRIES_URL,
                params={
                    'cause_list_date': date_str,
                    'bench_judge_id': bench_judge_id,
                    'skip': 0,
                    'limit': 2000
                },
                headers=LIVEDB_HEADERS,
                timeout=30
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 2 + attempt * 2
                print(f"[CAUSELIST] Bench {bench_judge_id} rate-limited (429), retry in {wait}s (attempt {attempt+1}/5)")
                time.sleep(wait)
                continue
            print(f"[CAUSELIST] Bench {bench_judge_id} HTTP {resp.status_code}")
            return []
        except Exception as e:
            print(f"[CAUSELIST] Bench {bench_judge_id} {date_str} error: {e} (attempt {attempt+1}/5)")
            time.sleep(1 + attempt)
    print(f"[CAUSELIST] Bench {bench_judge_id} giving up after retries")
    return []


def fetch_all_cause_list_entries_for_date(date_str, bench_ids):
    """
    Fetch all cause list entries for a given date by iterating over all benches.

    Division-bench dedup: Division benches have two judges but one court room,
    so the same court's records come back from both judges. We track which
    bench first claimed a court and skip records coming from other benches
    for that same court. Records from the SAME bench (different envelopes
    like URGENT + DAILY for the same court) are always kept.

    Returns a flat list of entry dicts ready to POST to Base44.
    """
    all_entries = []
    # Maps court_no -> bench_id that first claimed that court. Ensures that
    # within a single bench we keep every envelope (URGENT + DAILY + etc.),
    # but across different benches we only keep one copy per court.
    court_owner = {}
    now_ist = datetime.datetime.now(IST).isoformat(timespec='seconds')

    for idx, bench_id in enumerate(bench_ids):
        bench_sections = fetch_cause_list_for_bench(bench_id, date_str)
        # Throttle inter-bench requests to stay under PHHC rate limits.
        # Without this the server 429s partway through and we silently lose
        # entire benches of records (confirmed on 2026-04-23).
        if idx < len(bench_ids) - 1:
            time.sleep(0.7)
        for section in bench_sections:
            records = section.get('records', [])
            if not records:
                continue

            # Use court_no from the first record to detect duplicates
            court_no = records[0].get('court_no')
            if court_no is None:
                continue

            owner = court_owner.get(court_no)
            if owner is not None and owner != bench_id:
                # Another bench (the other judge of a division bench) already
                # handled this court - skip to avoid duplicates.
                continue
            court_owner[court_no] = bench_id

            for rec in records:
                # NOTE: We deliberately do NOT filter on main_suppl here.
                # Despite the field name, at the record level main_suppl does
                # NOT mean "main vs supplementary" - most single-bench URGENT
                # and DAILY records come through with main_suppl='S', and the
                # old filter was silently dropping ~80% of all cause list
                # entries (including our test case RSA-1398-2026 in court 11).

                cl_type = rec.get('cl_type', '')
                list_type_name = CL_TYPE_NAMES.get(cl_type, cl_type)

                case_type = str(rec.get('case_type') or '')
                case_no_raw = str(rec.get('case_no') or '')
                case_year = str(rec.get('case_year') or '')
                case_number = f"{case_type}-{case_no_raw}-{case_year}"
                try:
                    case_no = int(case_no_raw)
                except (ValueError, TypeError):
                    case_no = None

                pet = (rec.get('pet_name') or '')[:200]
                res = (rec.get('res_name') or '')[:200]
                if pet and res:
                    parties = f"{pet} vs {res}"
                elif pet:
                    parties = pet
                elif res:
                    parties = res
                else:
                    parties = ''

                # Extract clean item number - sr_no can be "210 **", "101 ***", etc.
                sr_raw = str(rec.get('sr_no') or '')
                sr_match = re.search(r'\d+', sr_raw)
                item_number = sr_match.group() if sr_match else sr_raw

                entry = {
                    'case_number': case_number,
                    'case_type': case_type,
                    'case_no': case_no,
                    'case_year': case_year,
                    'court_number': court_no,
                    'item_number': item_number,
                    'list_date': date_str,
                    'list_type': list_type_name,
                    'bench_type': str(rec.get('bench_type') or ''),  # 'D'=division, 'S'=single
                    'district': '',  # not available in cause list API records
                    'parties': parties[:500],
                    'downloaded_at': now_ist,
                }
                all_entries.append(entry)

                # Parse connected_cases to also store entries for parent cases.
                # When CM-18870-CWP-2025 is listed "IN CWP-17995-2023", the parent case
                # appears with prefix='IN'. We store an extra entry for the parent so
                # users tracking the parent case (e.g. CWP-17995-2023) see it listed.
                for conn in (rec.get('connected_cases') or []):
                    if conn.get('prefix') != 'IN':
                        continue
                    p_type = str(conn.get('scase_type') or '').strip()
                    p_no_raw = str(conn.get('scase_no') or '').strip()
                    p_year = str(conn.get('scase_year') or '').strip()
                    if not p_type or not p_no_raw:
                        continue
                    parent_cn = f"{p_type}-{p_no_raw}-{p_year}"
                    if parent_cn == case_number:
                        continue
                    try:
                        p_no_int = int(p_no_raw)
                    except (ValueError, TypeError):
                        p_no_int = None
                    parent_entry = dict(entry)
                    parent_entry['case_number'] = parent_cn
                    parent_entry['case_type'] = p_type
                    parent_entry['case_no'] = p_no_int
                    parent_entry['case_year'] = p_year
                    all_entries.append(parent_entry)

    print(f"[CAUSELIST] Fetched {len(all_entries)} entries across {len(court_owner)} courts for {date_str}")
    return all_entries


def store_cause_list_entries(entries):
    """Write parsed entries to Base44 CauseListEntry entity.

    Skips any entry whose (date, type, case_number, court_no, item_no)
    key is already in the in-memory index - so a refetch against a
    partially-populated (date, type) pair never creates duplicates.
    Successful writes are added to the index and the per-pair counter
    so subsequent check_existing_cause_list() calls see the fresh state.
    """
    _load_cause_list_cache()
    stored = 0
    failed = 0
    skipped = 0
    for entry in entries:
        key = (
            entry.get("list_date"),
            entry.get("list_type"),
            entry.get("case_number") or "",
            entry.get("court_number"),
            str(entry.get("item_number") or ""),
        )
        if key in _cause_list_keys:
            skipped += 1
            continue
        try:
            r = requests.post(
                f"{BASE44_URL}/CauseListEntry",
                headers=HEADERS,
                json=entry,
                timeout=15,
            )
            if r.status_code == 200:
                stored += 1
                _cause_list_keys.add(key)
                pair = (entry.get("list_date"), entry.get("list_type"))
                _cause_list_counts[pair] = _cause_list_counts.get(pair, 0) + 1
                time.sleep(0.2)  # avoid Base44 rate limit (429)
            else:
                failed += 1
                if failed <= 3:
                    print(f"[CAUSELIST] Store failed: {r.status_code} {r.text[:200]}")
        except Exception as e:
            failed += 1
            if failed <= 3:
                print(f"[CAUSELIST] Store error: {e}")
    print(f"[CAUSELIST] Stored {stored} entries, skipped {skipped} dupes, {failed} failures")
    return stored


def scrape_cause_lists():
    """
    Main cause list function. Uses direct JSON API on livedb9010.digitalls.in ‚Äö√Ñ√∂‚àö√ë‚àö√Ü
    no PDF download, no csrt token, no Cloudflare.

    Checks today+0 through today+3 for available cause lists.
    - ORDINARY lists are published ~2 days before the hearing date
    - URGENT lists are published ~1 day before the hearing date
    """
    print("[CAUSELIST] Starting cause list check...")
    now_ist = datetime.datetime.now(IST)

    # Check today through today+3
    dates_to_check = [
        (now_ist.date() + timedelta(days=d)).isoformat()
        for d in range(4)
    ]

    bench_ids = None  # Fetched once and reused across all dates

    for cl_date in dates_to_check:
        try:
            # Check summary to see what list types are available for this date
            summary = get_cause_list_summary(cl_date)
            if not summary:
                continue  # No lists published for this date yet

            # Determine which list types are available (from main lists only)
            available_types = set()
            for item in summary:
                lt_name = item.get('list_type_name', '')
                ms = item.get('main_suppl', '')
                if ms == 'M' and lt_name in ('URGENT', 'ORDINARY'):
                    available_types.add(lt_name)

            if not available_types:
                continue

            # Which types do we still need to fetch?
            needed_types = {
                lt for lt in available_types
                if not check_existing_cause_list(cl_date, lt)
            }

            if not needed_types:
                print(f"[CAUSELIST] {cl_date}: all available types already stored, skipping")
                continue

            print(f"[CAUSELIST] {cl_date}: need to fetch {needed_types}")

            # Fetch bench IDs once per scrape_cause_lists() call
            if bench_ids is None:
                bench_ids = get_active_bench_ids()

            if not bench_ids:
                print("[CAUSELIST] No bench IDs ‚Äö√Ñ√∂‚àö√ë‚àö√Ü cannot fetch cause lists")
                return

            # Fetch ALL entries for this date (one pass over all benches)
            all_entries = fetch_all_cause_list_entries_for_date(cl_date, bench_ids)

            if not all_entries:
                print(f"[CAUSELIST] No entries returned for {cl_date}")
                continue

            # Store entries for each needed list type separately
            for lt in needed_types:
                entries_for_type = [e for e in all_entries if e['list_type'] == lt]
                if not entries_for_type:
                    print(f"[CAUSELIST] {cl_date}: no {lt} entries in API response")
                    continue

                courts = set(e['court_number'] for e in entries_for_type)
                print(f"[CAUSELIST] {lt} {cl_date}: {len(entries_for_type)} entries across {len(courts)} courts")

                stored = store_cause_list_entries(entries_for_type)
                # store_cause_list_entries updates _cause_list_counts and
                # _cause_list_keys on every successful POST, so no separate
                # cache-add step is needed here.
                if stored > 0:
                    total = _cause_list_counts.get((cl_date, lt), 0)
                    print(f"[CAUSELIST] Cached ({cl_date}, {lt}) -> {total} total records")

        except Exception as e:
            print(f"[CAUSELIST] Error processing {cl_date}: {e}")
            continue

    print("[CAUSELIST] Cause list check complete")


# ============================================================
# STEP 7 — SYNC TRACKED CASES FROM CAUSE LIST
# ============================================================
# Tracks when we last ran the sync so we don't hammer the API every 30s.
_last_sync_time = None
SYNC_INTERVAL_SECONDS = 600  # run at most once every 10 minutes


def sync_tracked_cases_from_cause_list():
    """
    After cause list entries are in Base44, find every TrackedCase whose
    case appears in an upcoming CauseListEntry and update it with:
        case_date    → the actual hearing date (so MyCases calendar shows
                        it on the right day, not on the day it was added)
        court_number → the court room it's listed in
        item_number  → its item/SR number on the cause list

    Matching logic:
        TrackedCase stores case_type="CRM", case_number="39427", case_year=2025.
        CauseListEntry stores case_number="CRM-39427-2025".
        We build the composite key from TrackedCase to match.

    We pick the EARLIEST upcoming date if a case appears on multiple days.
    Rate-limited: only called once every SYNC_INTERVAL_SECONDS.
    """
    print("[SYNC] Syncing TrackedCase hearing dates from CauseListEntry...")
    try:
        # --- 1. Fetch all TrackedCase records ---
        resp = requests.get(f"{BASE44_URL}/TrackedCase", headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"[SYNC] Could not fetch TrackedCase: {resp.status_code}")
            return
        all_cases = resp.json()
        if not all_cases:
            print("[SYNC] No TrackedCase records found.")
            return

        # Build composite → TrackedCase map
        # (composite = "CRM-39427-2025" built from separate fields)
        case_map = {}
        for tc in all_cases:
            ct = str(tc.get("case_type") or "").strip()
            cn = str(tc.get("case_number") or "").strip()
            cy = str(tc.get("case_year") or "").strip()
            if ct and cn and cy:
                composite = f"{ct}-{cn}-{cy}"
                # If a user added the same case twice, keep only one entry
                if composite not in case_map:
                    case_map[composite] = tc

        if not case_map:
            print("[SYNC] No valid TrackedCase composites to match.")
            return

        print(f"[SYNC] Looking for listings for {len(case_map)} unique tracked cases...")

        # --- 2. Check today + next 13 days of cause list entries ---
        now_ist = datetime.datetime.now(IST)
        today = now_ist.date().isoformat()
        dates_to_check = [
            (now_ist.date() + timedelta(days=d)).isoformat()
            for d in range(14)
        ]

        # earliest_match[composite] = { date, court_number, item_number }
        earliest_match = {}

        for date_str in dates_to_check:
            resp = requests.get(
                f"{BASE44_URL}/CauseListEntry",
                params={"list_date": date_str},
                headers=HEADERS,
                timeout=15
            )
            if resp.status_code != 200:
                continue
            entries = resp.json()
            if not isinstance(entries, list) or not entries:
                continue

            for entry in entries:
                cn = entry.get("case_number")
                if not cn or cn not in case_map:
                    continue
                # Only record the EARLIEST date (dates_to_check is ascending)
                if cn not in earliest_match:
                    earliest_match[cn] = {
                        "date": date_str,
                        "court_number": entry.get("court_number"),
                        "item_number": entry.get("item_number"),
                    }

        if not earliest_match:
            print("[SYNC] No CauseListEntry matches found for any tracked case.")
            return

        # --- 3. Update matched TrackedCase records ---
        now_utc = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        updated = 0
        for composite, match in earliest_match.items():
            tc = case_map[composite]
            tc_id = tc.get("_id") or tc.get("id")
            new_date = match["date"]
            new_court = match["court_number"]
            new_item = match["item_number"]

            # Skip if nothing has changed
            if (tc.get("case_date") == new_date and
                    tc.get("court_number") == new_court and
                    str(tc.get("item_number") or "") == str(new_item or "")):
                continue

            payload = {
                "case_date": new_date,
                "court_number": new_court,
                "item_number": new_item,
                "last_updated": now_utc,
            }
            r = requests.put(
                f"{BASE44_URL}/TrackedCase/{tc_id}",
                headers=HEADERS,
                json=payload,
                timeout=15
            )
            if r.status_code == 200:
                updated += 1
                print(f"[SYNC] {composite} → date={new_date}, "
                      f"court={new_court}, item={new_item}")
            else:
                print(f"[SYNC] Failed to update {composite}: {r.status_code} {r.text[:100]}")

        print(f"[SYNC] Done — updated {updated} / {len(earliest_match)} matched cases.")

    except Exception as e:
        print(f"[SYNC] Error: {e}")


def maybe_sync_tracked_cases():
    """Rate-limited wrapper — runs sync at most once every SYNC_INTERVAL_SECONDS."""
    global _last_sync_time
    now = time.time()
    if _last_sync_time is not None and (now - _last_sync_time) < SYNC_INTERVAL_SECONDS:
        return
    sync_tracked_cases_from_cause_list()
    _last_sync_time = now


# ============================================================
# MAIN LOOP
# ============================================================
def main():
    print("=" * 50)
    print("MatterTracker Scraper ‚Äö√Ñ√∂‚àö√ë‚àö√Ü Starting")
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

            # --- SYNC TRACKED CASE DATES FROM CAUSE LIST (rate-limited) ---
            try:
                maybe_sync_tracked_cases()
            except Exception as e:
                print(f"[SYNC] Unexpected error: {e}")

            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Cycle complete. "
                  f"Waiting {SCRAPE_INTERVAL}s...")

        except Exception as e:
            print(f"[CRITICAL] Unexpected error: {e}. Continuing in 30s...")

        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
