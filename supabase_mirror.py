"""
Mirror the scraper's writes into Supabase, alongside Base44.

This is the shadow half of the Base44 migration: Base44 stays the only source
of truth and every alert decision still reads from it. Supabase receives a
copy so the two can be compared for a few weeks before anything switches over.

THREE RULES, IN THIS ORDER OF IMPORTANCE:

  1. IT MUST NOT DELAY AN ALERT. Callers only ever put an item on a queue and
     return; a background worker does the network. If the queue is full the
     item is dropped and counted -- a lost shadow copy is nothing, a stalled
     30-second loop is a missed hearing.

  2. IT MUST NOT BREAK AN ALERT. Nothing in here raises to its caller. Every
     failure is swallowed and counted, including a Supabase outage.

  3. IT CHANGES NO DECISION. Nothing is READ from Supabase. This module is
     write-only, by construction.

It is inert until SUPABASE_URL and SUPABASE_SERVICE_KEY are both set, so the
code can ship dark and be switched on -- or off -- by changing an environment
variable rather than deploying.
"""
import json
import os
import queue
import threading
import time
import urllib.error
import urllib.request

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)

# Bounded on purpose. If Supabase is unreachable the queue fills, and from then
# on we drop rather than grow memory without limit inside a process whose real
# job is to watch the display board.
_q = queue.Queue(maxsize=20000)
_stats = {"queued": 0, "sent": 0, "failed": 0, "dropped": 0}
_started = False
_lock = threading.Lock()

CONFLICT = {
    "cause_list_entries": "list_date,list_type,court_number,item_number,"
                          "case_type,case_no,case_year",
    "court_status": "court_number",
    "staging_notification_logs": "legacy_id",
    "tracked_cases": "legacy_id",
}

# Fingerprint of the last version of each case we sent. The alert loop re-reads
# every TrackedCase each cycle; without this we would re-POST ~633 unchanged
# rows every 30s (21 writes/second doing nothing). Load should scale with what
# CHANGES, not with how many cases exist.
_tc_seen = {}


def _i(v):
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _s(v):
    return None if v is None or v == "" else str(v)


def _post(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={CONFLICT[table]}"
    req = urllib.request.Request(
        url, data=json.dumps(rows).encode(), method="POST",
        headers={"apikey": SUPABASE_KEY,
                 "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates,return=minimal"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status


def _worker():
    """Drain the queue forever. Batches whatever has piled up, per table."""
    last_report = time.time()
    while True:
        try:
            table, row = _q.get()
            batch = {table: [row]}
            # Opportunistically sweep up anything else already waiting.
            for _ in range(499):
                try:
                    t2, r2 = _q.get_nowait()
                except queue.Empty:
                    break
                batch.setdefault(t2, []).append(r2)

            for tbl, rows in batch.items():
                try:
                    _post(tbl, rows)
                    _stats["sent"] += len(rows)
                except Exception as e:
                    _stats["failed"] += len(rows)
                    if _stats["failed"] <= 5 or _stats["failed"] % 500 == 0:
                        detail = ""
                        if isinstance(e, urllib.error.HTTPError):
                            try:
                                detail = e.read()[:200].decode(errors="replace")
                            except Exception:
                                pass
                        print(f"[MIRROR] {tbl}: {len(rows)} rows failed "
                              f"({type(e).__name__} {e}) {detail}")

            if time.time() - last_report > 600:
                print(f"[MIRROR] queued={_stats['queued']} sent={_stats['sent']} "
                      f"failed={_stats['failed']} dropped={_stats['dropped']} "
                      f"backlog={_q.qsize()}")
                last_report = time.time()
        except Exception as e:                      # the worker never dies
            print(f"[MIRROR] worker error: {e}")
            time.sleep(5)


def start():
    """Start the background worker once. Safe to call repeatedly."""
    global _started
    if not ENABLED:
        print("[MIRROR] disabled (SUPABASE_URL / SUPABASE_SERVICE_KEY not set)")
        return
    with _lock:
        if _started:
            return
        threading.Thread(target=_worker, daemon=True, name="supabase-mirror").start()
        _started = True
        print(f"[MIRROR] shadow writes ON -> {SUPABASE_URL}")


def _put(table, row):
    if not ENABLED:
        return
    try:
        _q.put_nowait((table, row))
        _stats["queued"] += 1
    except queue.Full:
        _stats["dropped"] += 1
        if _stats["dropped"] in (1, 100) or _stats["dropped"] % 5000 == 0:
            print(f"[MIRROR] queue full, dropped {_stats['dropped']} rows "
                  f"(shadow copy only; alerts unaffected)")
    except Exception:
        pass                                         # never raise at a call site


# ── public API ─────────────────────────────────────────────────────────────
def mirror_court_status(court_number, data, today, now):
    if not ENABLED:
        return
    _put("court_status", {
        "court_number": _i(court_number),
        "current_item": _i(data.get("current_item")),
        "last_regular_item": _i(data.get("last_regular_item")),
        "last_queue_item": _i(data.get("last_queue_item")),
        "is_passover": bool(data.get("is_passover")),
        "passover_current": _i(data.get("passover_current")),
        "passover_total": _i(data.get("passover_total")),
        "court_date": today, "last_updated": now, "is_active": True})


def mirror_cause_list(entry):
    if not ENABLED:
        return
    row = {"case_number": _s(entry.get("case_number")),
           "case_type": _s(entry.get("case_type")),
           "case_no": _i(entry.get("case_no")),
           "case_year": _i(entry.get("case_year")),
           "court_number": _i(entry.get("court_number")),
           "item_number": _i(entry.get("item_number")),
           "list_date": _s(entry.get("list_date")),
           "list_type": _s(entry.get("list_type")),
           "bench_type": _s(entry.get("bench_type")),
           "district": _s(entry.get("district")),
           "parties": _s(entry.get("parties")),
           "downloaded_at": _s(entry.get("downloaded_at"))}
    # The destination declares these NOT NULL; a row missing one would fail the
    # whole batch, so drop it here rather than poison 500 good rows.
    for f in ("case_number", "case_type", "case_no", "case_year",
              "court_number", "item_number", "list_date", "list_type"):
        if row[f] is None:
            return
    _put("cause_list_entries", row)


def mirror_notification(user_id, case_id, notification_type, message, sent_at):
    """Alerts go to the HOLDING table: the real one points at user accounts,
    and those do not exist until the sign-in migration."""
    if not ENABLED:
        return
    _put("staging_notification_logs", {
        "legacy_id": f"{case_id}:{notification_type}:{sent_at}",
        "legacy_user_id": _s(user_id), "legacy_case_id": _s(case_id),
        "notification_type": _s(notification_type),
        "sent_at": _s(sent_at), "message": _s(message),
        "raw": {"user_id": user_id, "case_id": case_id,
                "notification_type": notification_type,
                "message": message, "sent_at": sent_at, "source": "mirror"}})


def mirror_tracked_case(case):
    """Mirror one TrackedCase, UNCLAIMED.

    user_id is deliberately absent from the payload. PostgREST's upsert only
    assigns the columns it is given, so leaving it out means a row already
    claimed by a signed-in advocate keeps its owner — including it as null
    would hand their cases back to nobody on the next cycle.

    ⚠️ TURN THIS OFF AT CUT-OVER. Once the app writes cases to Supabase, Base44
    becomes the stale copy and this would overwrite live edits with old data.
    Clearing SUPABASE_URL/SUPABASE_SERVICE_KEY stops it without a deploy.
    """
    if not ENABLED:
        return
    legacy_id = _s(case.get("id"))
    if legacy_id is None:
        return
    status = _s(case.get("status"))
    thresholds = [x for x in (_i(v) for v in (case.get("notified_thresholds") or []))
                  if x is not None]
    fetched = case.get("fetched_data_json")
    if isinstance(fetched, str):
        try:
            fetched = json.loads(fetched) if fetched.strip() else None
        except ValueError:
            fetched = None                    # malformed on Base44; do not guess

    row = {
        "legacy_id": legacy_id,
        "legacy_user_id": _s(case.get("user_id")),
        "case_type": _s(case.get("case_type")),
        "case_number": _s(case.get("case_number")),
        "case_year": _i(case.get("case_year")),
        "case_title": _s(case.get("case_title")),
        "case_status": _s(case.get("case_status")),
        "petitioner_name": _s(case.get("petitioner_name")),
        "respondent_name": _s(case.get("respondent_name")),
        "petitioner_advocate": _s(case.get("petitioner_advocate")),
        "respondent_advocate": _s(case.get("respondent_advocate")),
        "next_hearing_date": _s(case.get("next_hearing_date")),
        "registration_date": _s(case.get("registration_date")),
        "listing_date": _s(case.get("listing_date")),
        "disposal_date": _s(case.get("disposal_date")),
        "final_order_date": _s(case.get("final_order_date")),
        "court_number": _i(case.get("court_number")),
        "item_number": _i(case.get("item_number")),
        "case_date": _s(case.get("case_date")),
        "cnr_no": _s(case.get("cnr_no")), "bench": _s(case.get("bench")),
        "district": _s(case.get("district")), "category": _s(case.get("category")),
        "order_link": _s(case.get("order_link")),
        "diary_number": _s(case.get("diary_number")),
        "is_disposed": bool(case.get("is_disposed")),
        "fetched_data_json": fetched,
        "notify_at_15": bool(case.get("notify_at_15", True)),
        "notify_at_10": bool(case.get("notify_at_10", True)),
        "notify_at_5": bool(case.get("notify_at_5", True)),
        "notified_thresholds": thresholds,
        "notifications_enabled": bool(case.get("notifications_enabled", True)),
        "notify_passover_always": bool(case.get("notify_passover_always")),
        # the destination constrains status; anything else becomes pending
        "status": status if status in ("pending", "called", "not_listed", "passover")
                  else "pending",
        "notes": _s(case.get("notes")),
    }

    fingerprint = json.dumps(row, sort_keys=True, default=str)
    if _tc_seen.get(legacy_id) == fingerprint:
        return
    _tc_seen[legacy_id] = fingerprint
    _put("tracked_cases", row)


def stats():
    return dict(_stats, backlog=_q.qsize(), enabled=ENABLED,
                tracked_cached=len(_tc_seen))
