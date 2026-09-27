"""Tests for the sitting-hours deferral.

The dangerous failure here is not "heavy work ran when it should have waited".
It is "the safety valve failed and a court day ran with no cause list", which
drops items-away onto naive arithmetic -- the false "your case is up" path.
So most of these test the valve.
"""
import datetime, os, sys
os.environ.setdefault("BASE44_APP_ID", "x"); os.environ.setdefault("BASE44_API_KEY", "x")
import scraper as S

ok = fail = 0
def check(label, cond, detail=""):
    global ok, fail
    if cond: ok += 1;  print(f"  ✅ {label}")
    else:    fail += 1; print(f"  ❌ {label}   {detail}")

def at(y, m, d, hh, mm):
    return datetime.datetime(y, m, d, hh, mm, tzinfo=S.IST)

print("1. Is it sitting hours?  (Mon 2026-09-28 … Sat 2026-10-03)")
cases = [
    ("Mon 09:44 — before courts",     at(2026,9,28,9,44),  False),
    ("Mon 09:45 — window opens",      at(2026,9,28,9,45),  True),
    ("Mon 11:30 — mid morning",       at(2026,9,28,11,30), True),
    ("Mon 13:30 — lunch, still sitting day", at(2026,9,28,13,30), True),
    ("Mon 16:29 — last minute",       at(2026,9,28,16,29), True),
    ("Mon 16:30 — window closes",     at(2026,9,28,16,30), False),
    ("Mon 23:00 — night",             at(2026,9,28,23,0),  False),
    ("Mon 01:00 — small hours",       at(2026,9,28,1,0),   False),
    ("Sat 11:30 — weekend",           at(2026,10,3,11,30), False),
    ("Sun 11:30 — weekend",           at(2026,10,4,11,30), False),
]
for label, when, expected in cases:
    check(label, S._in_sitting_hours(when) is expected,
          f"got {S._in_sitting_hours(when)}, wanted {expected}")

print("\n2. THE SAFETY VALVE — a missing list must override the pause")
today = datetime.date.today().isoformat()
real_keys = S._cause_list_keys

S._cause_list_keys = set()                       # nothing held for today
S._deferred_logged = None
import unittest.mock as mock
with mock.patch.object(S, "_in_sitting_hours", lambda *a: True):
    check("no list for today -> fetches anyway", S._defer_heavy_work("test") is False)

S._cause_list_keys = {(today, "COMPLETE", "CWP-1-2026", 7, 101)}
S._deferred_logged = None
with mock.patch.object(S, "_in_sitting_hours", lambda *a: True):
    check("list held for today -> defers", S._defer_heavy_work("test") is True)

S._cause_list_keys = {("2026-01-01", "COMPLETE", "CWP-1-2026", 7, 101)}
S._deferred_logged = None
with mock.patch.object(S, "_in_sitting_hours", lambda *a: True):
    check("only OLD dates held -> fetches anyway", S._defer_heavy_work("test") is False)

print("\n3. Outside sitting hours nothing is ever deferred")
S._cause_list_keys = {(today, "COMPLETE", "CWP-1-2026", 7, 101)}
S._deferred_logged = None
with mock.patch.object(S, "_in_sitting_hours", lambda *a: False):
    check("night: runs", S._defer_heavy_work("test") is False)
    check("weekend: runs", S._defer_heavy_work("test") is False)

print("\n4. The three heavy jobs actually return early when deferred")
S._cause_list_keys = {(today, "COMPLETE", "CWP-1-2026", 7, 101)}
for fn_name in ("scrape_cause_lists", "scrape_complete_lists",
                "maybe_refresh_tracked_cases_from_website"):
    S._deferred_logged = None
    called = {"http": False}
    with mock.patch.object(S, "_in_sitting_hours", lambda *a: True), \
         mock.patch.object(S.requests, "get",
                           side_effect=lambda *a, **k: called.__setitem__("http", True)):
        getattr(S, fn_name)()
    check(f"{fn_name} made no network call", called["http"] is False)

print("\n5. The alert path is NOT guarded — it must always run")
src = open("scraper.py").read()
for fn in ("def check_notifications", "def scrape_display_board",
           "def update_court_status", "def maybe_sync_tracked_cases",
           "def maybe_fill_case_positions"):
    i = src.index(fn)
    body = src[i:i+1400]
    check(f"{fn.replace('def ','')} is unguarded", "_defer_heavy_work" not in body)

S._cause_list_keys = real_keys
print(f"\n{ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
