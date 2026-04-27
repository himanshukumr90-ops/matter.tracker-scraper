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
    # 'B' is used only by the OLD highcourtchd.gov.in site (the "Complete
    # List" published overnight). Our livedb-path scraper never produces
    # this code; it's emitted only by scrape_complete_lists() below. It
    # maps to list_type 'COMPLETE' which the sync function prioritises
    # above ORDINARY and URGENT.
    'B': 'COMPLETE',
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
# will be refetched.
#
# Threshold rationale (updated 2026-04-26):
#   A full ORDINARY list has 3,500-4,700 entries across 50-64 courts.
#   The old pre-429-fix scraper would hit rate limits after a handful of
#   benches, storing only 50-300 records — enough to satisfy the old
#   threshold of 50, which permanently blocked refetches for those dates.
#   Raising to 2,000 ensures any partial fetch gets retried while still
#   sitting comfortably below the minimum realistic full-list size.
#   The dedup key set (_cause_list_keys) prevents actual duplicates on refetch.
MIN_CAUSE_LIST_RECORDS = 2000

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


def get_court_queue(court_number, date_str):
    """
    Return the ordered list of item_numbers (ints, ascending) that a
    given court will work through on a given date. Drawn from the
    in-memory _cause_list_keys cache, so no extra HTTP calls.

    Preference order:
      1. COMPLETE list (the authoritative night-before PDF, which already
         interleaves URGENT and ORDINARY in the order the court calls them)
      2. URGENT + ORDINARY merged + sorted (fallback for dates where the
         Complete List PDF wasn't published / scraped)

    Returns [] if no list is available.
    """
    _load_cause_list_cache()

    def _items_for(list_type):
        out = set()
        for (ld, lt, _cn, court_no, item_no) in _cause_list_keys:
            if ld != date_str or lt != list_type or court_no != court_number:
                continue
            try:
                out.add(int(str(item_no).strip()))
            except (ValueError, TypeError):
                continue
        return sorted(out)

    complete = _items_for("COMPLETE")
    if complete:
        return complete

    urgent = _items_for("URGENT")
    ordinary = _items_for("ORDINARY")
    if not urgent and not ordinary:
        return []
    # Merge by item-number ascending. Court calls urgent items (typically
    # 100-series) before ordinary (200-series), so the natural numeric sort
    # already produces the right call order.
    return sorted(set(urgent) | set(ordinary))


def _compute_items_away(queue_cache, court_number, date_str, current_item, user_item):
    """
    Items remaining between the court's current_item and the user's case,
    using the actual list of items the court will call (so the gap between
    the Urgent block (e.g. 101-150) and the Ordinary block (e.g. 201+) is
    correctly skipped).

    Returns an integer items_away if the queue is known and the user's item
    is in it; otherwise returns None so the caller can fall back to naive
    arithmetic.
    """
    if court_number not in queue_cache:
        queue_cache[court_number] = get_court_queue(court_number, date_str)
    queue = queue_cache[court_number]
    if not queue:
        return None

    try:
        user_int = int(str(user_item).strip()) if user_item is not None else None
        current_int = int(str(current_item).strip()) if current_item is not None else 0
    except (ValueError, TypeError):
        return None

    if user_int is None or user_int not in queue:
        return None

    user_pos = queue.index(user_int)

    if current_int <= 0:
        # Court hasn't started (display board sentinel). User is user_pos
        # items away from the start.
        return user_pos

    if current_int in queue:
        return user_pos - queue.index(current_int)

    # current_int isn't a queue item (e.g. a passover item or off-list
    # motion). Treat the court's effective position as the largest queue
    # item less than current_int.
    smaller = [q for q in queue if q < current_int]
    if smaller:
        return user_pos - queue.index(smaller[-1])
    # Court is at an item below the entire queue → not yet reached.
    return user_pos


def check_notifications(court_data, existing_records):
    if not court_data:
        return
    cases = get_tracked_cases()
    if not cases:
        return

    today = datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    queue_cache = {}  # court_number -> sorted [int, ...]; cached per call

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

        # Prefer queue-aware computation that skips the gap between Urgent
        # (100s) and Ordinary (200s). Falls back to naive subtraction when
        # the queue isn't loaded yet (e.g. before the Complete / Urgent /
        # Ordinary list is published for that date).
        items_away = _compute_items_away(
            queue_cache, court_number, today, current_item, item_number
        )
        if items_away is None:
            try:
                items_away = int(item_number) - int(current_item)
            except (ValueError, TypeError):
                continue
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
                # Base44 schema requires a numeric case_no; rows whose case
                # number can't be parsed as int (e.g. "12345A") would be
                # rejected at write time. Skip them here to avoid log spam
                # and wasted API calls. Connected/parent cases are still
                # processed below since they have their own case_no.
                if case_no is not None:
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
                    if p_no_int is None:
                        continue  # Same reason as above for parent cases
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

            # Within this date, pick the single BEST entry per tracked
            # case by list-type priority: COMPLETE > ORDINARY > URGENT.
            # Complete List (from the old highcourtchd.gov.in site) is the
            # final, authoritative list published overnight before hearing;
            # it trumps Ordinary and Urgent whenever it's present.
            LIST_TYPE_PRIORITY = {"COMPLETE": 0, "ORDINARY": 1, "URGENT": 2}
            best_per_case = {}  # cn -> (priority, entry)
            for entry in entries:
                cn = entry.get("case_number")
                if not cn or cn not in case_map:
                    continue
                p = LIST_TYPE_PRIORITY.get(entry.get("list_type"), 99)
                cur = best_per_case.get(cn)
                if cur is None or p < cur[0]:
                    best_per_case[cn] = (p, entry)

            for cn, (_, entry) in best_per_case.items():
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
# STEP 8 — PERIODIC WEBSITE REFRESH OF TRACKED CASES
# ============================================================
# Re-fetches case details from the PHHC lookup API (Vercel) for every
# TrackedCase and updates case_date/next_hearing_date when PHHC has
# posted a new date. Cause-list sync runs right after this in the main
# loop, so if a case is on a published cause list that value will still
# overwrite whatever came from the website — cause list trumps website.
_last_website_refresh_time = None
WEBSITE_REFRESH_INTERVAL_SECONDS = 6 * 3600  # every 6 hours
CASE_LOOKUP_API = "https://mattertracker-api.vercel.app/case"


def _parse_phhc_date(raw):
    """
    Parse a date string coming back from the PHHC lookup API into a
    YYYY-MM-DD string, or return None if it can't be parsed / is empty.
    Accepts YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY, 'D Mon YYYY', etc.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    fmts = [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
        "%d %b %Y", "%d %B %Y",
        "%d-%b-%Y", "%d-%B-%Y",
    ]
    for f in fmts:
        try:
            return datetime.datetime.strptime(s, f).date().isoformat()
        except ValueError:
            continue
    return None


def _pick_future_date(*candidates):
    """
    From a list of raw date strings, return the EARLIEST parsed date
    that is today or in the future, as YYYY-MM-DD, or None.
    """
    today = datetime.datetime.now(IST).date()
    parsed = []
    for c in candidates:
        d = _parse_phhc_date(c)
        if not d:
            continue
        try:
            dt = datetime.date.fromisoformat(d)
        except ValueError:
            continue
        if dt >= today:
            parsed.append(d)
    if not parsed:
        return None
    return min(parsed)


def refresh_tracked_cases_from_website():
    """
    Walk every TrackedCase and re-fetch case details from the PHHC lookup
    API. Update case_date when PHHC has a newer/future hearing date.

    Conflict rule: website values are a best-guess starting point. The
    cause-list sync runs immediately after this in the main loop and
    will overwrite case_date with the actual cause-list date when a
    match exists, so cause list always takes precedence.
    """
    print("[WEBREFRESH] Refreshing tracked case details from PHHC website...")
    try:
        resp = requests.get(f"{BASE44_URL}/TrackedCase", headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"[WEBREFRESH] Could not fetch TrackedCase: {resp.status_code}")
            return
        all_cases = resp.json()
        if not all_cases:
            print("[WEBREFRESH] No TrackedCase records found.")
            return

        now_utc = datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
        today = datetime.datetime.now(IST).date()
        updated = 0
        checked = 0

        for tc in all_cases:
            ct = str(tc.get("case_type") or "").strip()
            cn = str(tc.get("case_number") or "").strip()
            cy = str(tc.get("case_year") or "").strip()
            if not (ct and cn and cy):
                continue
            tc_id = tc.get("_id") or tc.get("id")
            if not tc_id:
                continue

            # Throttle the Vercel API
            time.sleep(0.3)
            checked += 1
            try:
                r = requests.get(
                    CASE_LOOKUP_API,
                    params={"type": ct, "no": cn, "year": cy},
                    timeout=20
                )
                if r.status_code != 200:
                    continue
                data = r.json()
            except Exception as e:
                print(f"[WEBREFRESH] Lookup failed for {ct}-{cn}-{cy}: {e}")
                continue

            if not data.get("found"):
                continue

            new_date = _pick_future_date(
                data.get("next_hearing_date"),
                data.get("listing_date"),
            )

            payload = {}

            # Only update case_date if
            #   (a) we have a future/today date from PHHC, AND
            #   (b) the current case_date is empty, in the past, or
            #       different from the PHHC-derived date.
            # If current case_date is today-or-future AND came from the
            # cause list sync, that sync will just overwrite our value
            # on its next run anyway, so this is safe.
            current_date = tc.get("case_date")
            if new_date:
                should_update = False
                if not current_date:
                    should_update = True
                else:
                    try:
                        cur_dt = datetime.date.fromisoformat(current_date)
                        if cur_dt < today or current_date != new_date:
                            should_update = True
                    except ValueError:
                        should_update = True
                if should_update:
                    payload["case_date"] = new_date

            # Keep next_hearing_date raw field in sync with API (if API
            # populates it). Never clobber an existing non-empty value
            # with an empty one — PHHC sometimes blanks this between
            # refreshes.
            api_nhd = _parse_phhc_date(data.get("next_hearing_date"))
            if api_nhd and api_nhd != tc.get("next_hearing_date"):
                payload["next_hearing_date"] = api_nhd

            if not payload:
                continue

            payload["last_updated"] = now_utc
            try:
                u = requests.put(
                    f"{BASE44_URL}/TrackedCase/{tc_id}",
                    headers=HEADERS,
                    json=payload,
                    timeout=15
                )
                if u.status_code == 200:
                    updated += 1
                    print(f"[WEBREFRESH] {ct}-{cn}-{cy} → {payload}")
                else:
                    print(f"[WEBREFRESH] Update failed for {ct}-{cn}-{cy}: "
                          f"{u.status_code} {u.text[:100]}")
            except Exception as e:
                print(f"[WEBREFRESH] Update error for {ct}-{cn}-{cy}: {e}")

        print(f"[WEBREFRESH] Done — checked {checked}, updated {updated}.")

    except Exception as e:
        print(f"[WEBREFRESH] Error: {e}")


def maybe_refresh_tracked_cases_from_website():
    """Rate-limited: runs at most once every WEBSITE_REFRESH_INTERVAL_SECONDS."""
    global _last_website_refresh_time
    now = time.time()
    if (_last_website_refresh_time is not None
            and (now - _last_website_refresh_time) < WEBSITE_REFRESH_INTERVAL_SECONDS):
        return
    refresh_tracked_cases_from_website()
    _last_website_refresh_time = now


# ============================================================
# STEP 9 — COMPLETE LIST (from old highcourtchd.gov.in site)
# ============================================================
# The old site publishes the Complete List as a PDF ~10pm-12am the
# night before the hearing date. It's the final, authoritative list
# with court numbers and item numbers for every case (ordinary, urgent,
# commercial, etc. merged). The sync function prioritises COMPLETE
# over ORDINARY and URGENT whenever it exists.
import subprocess
import tempfile

OLD_SITE_BASE = "https://highcourtchd.gov.in"
OLD_SITE_FORM_URL = f"{OLD_SITE_BASE}/view_causeList.php"
OLD_SITE_PDF_URL = f"{OLD_SITE_BASE}/show_cause_list.php"
# Static CSRF token the site accepts — it's just 'phhc-team' hex-encoded.
OLD_SITE_CSRF = "706868632d7465616d"
OLD_SITE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Referer": f"{OLD_SITE_BASE}/?mod=causelist",
}

COMPLETE_LIST_POLL_INTERVAL_SECONDS = 15 * 60  # every 15 minutes
MIN_COMPLETE_LIST_RECORDS = 1000  # a real Complete List day has 3k-5k+
_last_complete_poll = {}  # date_str -> epoch seconds
# Throttle between old-site HTTP calls — user asked for an extra-patient
# runner. 3s between requests is friendly and still fast enough.
OLD_SITE_THROTTLE_SECONDS = 3.0
_last_old_site_call = 0.0


def _old_site_throttle():
    """Block until at least OLD_SITE_THROTTLE_SECONDS have passed since
    the last call to the old site. Keeps us polite and well below any
    rate-limit threshold."""
    global _last_old_site_call
    now = time.time()
    wait = OLD_SITE_THROTTLE_SECONDS - (now - _last_old_site_call)
    if wait > 0:
        time.sleep(wait)
    _last_old_site_call = time.time()


def download_complete_list_pdf(date_str):
    """
    Download the Complete List PDF for a date (YYYY-MM-DD) from
    highcourtchd.gov.in. Returns the path to a temp PDF file, or None
    if the PDF isn't published yet / network fails.
    """
    try:
        d = datetime.date.fromisoformat(date_str)
    except ValueError:
        return None
    ddmmyyyy = d.strftime("%d/%m/%Y")

    # --- Step 1: POST to view_causeList.php to get obfuscated filename ---
    try:
        _old_site_throttle()
        r = requests.post(
            OLD_SITE_FORM_URL,
            headers={**OLD_SITE_HEADERS, "X-Requested-With": "XMLHttpRequest"},
            data={
                "csrf_token": OLD_SITE_CSRF,
                "t_f_date": ddmmyyyy,
                "urg_ord": "B",  # B = Complete List
                "action": "show_causeList",
            },
            timeout=30,
        )
    except Exception as e:
        print(f"[COMPLETE] {date_str} form POST failed: {e}")
        return None
    if r.status_code != 200:
        print(f"[COMPLETE] {date_str} form POST HTTP {r.status_code}")
        return None

    m = re.search(r"filename=([A-Za-z0-9]+)", r.text)
    if not m:
        # Either no Complete List published yet for this date, or the
        # page layout changed.
        return None
    obfuscated = m.group(1)

    # --- Step 2: GET the PDF ---
    try:
        _old_site_throttle()
        r = requests.get(
            OLD_SITE_PDF_URL,
            params={"filename": obfuscated},
            headers=OLD_SITE_HEADERS,
            timeout=120,  # PDFs are ~4MB
        )
    except Exception as e:
        print(f"[COMPLETE] {date_str} PDF GET failed: {e}")
        return None
    if r.status_code != 200:
        print(f"[COMPLETE] {date_str} PDF GET HTTP {r.status_code}")
        return None
    if "application/pdf" not in r.headers.get("Content-Type", ""):
        print(f"[COMPLETE] {date_str} PDF GET got non-PDF content")
        return None

    # Save to temp file
    fd, path = tempfile.mkstemp(prefix=f"cl_{date_str}_", suffix=".pdf")
    with os.fdopen(fd, "wb") as f:
        f.write(r.content)
    print(f"[COMPLETE] {date_str} downloaded PDF ({len(r.content) // 1024} KB)")
    return path


# Match a cause-list entry line like:
#   "  101 I    KAPURTHALA    CRM-18484-2023  ..."
# Captures: item, column_marker, case_type, case_no, case_year.
_ENTRY_RE = re.compile(
    r"^\s*(?P<item>\d{1,4})\s+"
    r"(?P<col>[IVX\*]+)\s+"
    r"(?P<rest>.{0,60}?)\s+"
    r"(?P<ct>[A-Z]+(?:-[A-Z]+)*)-(?P<no>\d+)-(?P<yr>\d{4})\b"
)
# Detect the court-room number from section headers like:
#   "... CR NO 31" or zoom link "vcphhc31"
_CR_NO_RE = re.compile(r"\bCR\s+NO\s+(\d+)\b")
_VC_LINK_RE = re.compile(r"vcphhc(\d{2,3})")


def parse_complete_list_pdf(pdf_path, date_str):
    """
    Parse the Complete List PDF using `pdftotext -layout`. Returns a
    list of CauseListEntry dicts (same shape as the livedb path).
    """
    try:
        txt = subprocess.run(
            ["pdftotext", "-layout", pdf_path, "-"],
            check=True,
            capture_output=True,
            timeout=120,
        ).stdout.decode("utf-8", errors="replace")
    except FileNotFoundError:
        print("[COMPLETE] pdftotext not installed — skipping parse")
        return []
    except Exception as e:
        print(f"[COMPLETE] pdftotext failed: {e}")
        return []

    entries = []
    seen_keys = set()  # (case_number, court_no, item) dedup within the PDF
    current_court = None
    now_ist = datetime.datetime.now(IST).isoformat(timespec='seconds')

    for line in txt.splitlines():
        # Track current court room from section headers
        m_cr = _CR_NO_RE.search(line)
        if m_cr:
            try:
                current_court = int(m_cr.group(1))
            except ValueError:
                pass
            continue
        m_vc = _VC_LINK_RE.search(line)
        if m_vc:
            try:
                current_court = int(m_vc.group(1))
            except ValueError:
                pass
            # Don't `continue` — vc link might appear on a line that
            # doesn't also have an entry, but doesn't hurt to try.

        m = _ENTRY_RE.match(line)
        if not m or current_court is None:
            continue

        item_raw = m.group("item")
        case_type = m.group("ct")
        case_no_raw = m.group("no")
        case_year = m.group("yr")
        case_number = f"{case_type}-{case_no_raw}-{case_year}"

        # Dedup — the same case can appear multiple times in the PDF
        # (e.g. linked with sub-applications). We keep the first seen.
        key = (case_number, current_court, item_raw)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        try:
            case_no_int = int(case_no_raw)
        except ValueError:
            case_no_int = None

        if case_no_int is None:
            # Base44 schema requires a numeric case_no; skip rows whose
            # case number can't be parsed as int (e.g. "12345A").
            continue

        entries.append({
            "case_number": case_number,
            "case_type": case_type,
            "case_no": case_no_int,
            "case_year": case_year,
            "court_number": current_court,
            "item_number": item_raw,
            "list_date": date_str,
            "list_type": "COMPLETE",
            "bench_type": "",
            "district": "",
            "parties": "",
            "downloaded_at": now_ist,
            "last_updated": now_ist,
        })

    print(f"[COMPLETE] {date_str} parsed {len(entries)} entries from PDF")
    return entries


def scrape_complete_lists():
    """
    For each upcoming date (today..today+3), attempt to download and
    parse the Complete List PDF from the old site. Skips dates that
    already have a stored Complete list in Base44 (>= MIN_COMPLETE_LIST_RECORDS).
    Rate-limited: at most one poll per (date) per
    COMPLETE_LIST_POLL_INTERVAL_SECONDS.
    """
    global _last_complete_poll
    now_ist = datetime.datetime.now(IST)
    dates_to_check = [
        (now_ist.date() + timedelta(days=d)).isoformat()
        for d in range(4)
    ]
    now_epoch = time.time()

    for cl_date in dates_to_check:
        # Already have a complete enough Complete List?
        if _cause_list_counts.get((cl_date, "COMPLETE"), 0) >= MIN_COMPLETE_LIST_RECORDS:
            continue
        # Polled too recently?
        last = _last_complete_poll.get(cl_date)
        if last is not None and (now_epoch - last) < COMPLETE_LIST_POLL_INTERVAL_SECONDS:
            continue
        _last_complete_poll[cl_date] = now_epoch

        print(f"[COMPLETE] {cl_date}: checking old site for Complete List...")
        pdf_path = download_complete_list_pdf(cl_date)
        if not pdf_path:
            print(f"[COMPLETE] {cl_date}: Complete List not yet published (or fetch failed)")
            continue

        try:
            entries = parse_complete_list_pdf(pdf_path, cl_date)
        finally:
            try:
                os.remove(pdf_path)
            except OSError:
                pass

        if not entries:
            print(f"[COMPLETE] {cl_date}: no parseable entries")
            continue

        store_cause_list_entries(entries)


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

            # --- SCRAPE COMPLETE LIST from old PHHC site (rate-limited, ~15min per date) ---
            try:
                scrape_complete_lists()
            except Exception as e:
                print(f"[COMPLETE] Unexpected error: {e}")

            # --- REFRESH TRACKED CASE DETAILS FROM PHHC WEBSITE (rate-limited, ~6h) ---
            # Runs BEFORE cause-list sync so that if both fire in the same
            # cycle, the cause-list sync gets the last word (cause list trumps
            # website).
            try:
                maybe_refresh_tracked_cases_from_website()
            except Exception as e:
                print(f"[WEBREFRESH] Unexpected error: {e}")

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
