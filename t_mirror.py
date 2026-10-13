"""Tests for the shadow-write module. The point of every one of these is the
same: prove it cannot hurt the alert loop."""
import importlib, os, sys, time

def fresh(url="", key=""):
    os.environ["SUPABASE_URL"] = url
    os.environ["SUPABASE_SERVICE_KEY"] = key
    if "supabase_mirror" in sys.modules:
        del sys.modules["supabase_mirror"]
    import supabase_mirror
    return importlib.reload(supabase_mirror)

ok = fail = 0
def check(label, cond, detail=""):
    global ok, fail
    if cond: ok += 1;  print(f"  ✅ {label}")
    else:    fail += 1; print(f"  ❌ {label}  {detail}")

print("1. INERT when the environment variables are absent")
m = fresh()
check("reports disabled", m.ENABLED is False)
t0 = time.time()
for _ in range(5000):
    m.mirror_court_status(7, {"current_item": 101}, "2026-09-28", "2026-09-28T00:00:00Z")
    m.mirror_cause_list({"case_number": "X-1-2026", "case_type": "X", "case_no": 1,
                         "case_year": 2026, "court_number": 7, "item_number": 1,
                         "list_date": "2026-09-28", "list_type": "COMPLETE"})
    m.mirror_notification("u", "c", "5_away", "msg", "2026-09-28T00:00:00Z")
el = time.time() - t0
check("15,000 calls are a no-op", m.stats()["queued"] == 0)
check(f"and cost nothing ({el*1000:.0f} ms total)", el < 0.5, f"{el:.3f}s")
m.start()
check("start() is safe when disabled", True)

print("\n2. UNREACHABLE Supabase must not raise or block")
m = fresh("https://127.0.0.1:9", "fake-key")
m.start()
t0 = time.time()
for _ in range(2000):
    m.mirror_court_status(7, {"current_item": 101}, "2026-09-28", "2026-09-28T00:00:00Z")
el = time.time() - t0
check(f"2,000 calls return immediately ({el*1000:.0f} ms)", el < 1.0, f"{el:.3f}s")
time.sleep(3)
s = m.stats()
check("failures are counted, not raised", s["failed"] > 0 or s["sent"] == 0)
check("the caller never saw an exception", True)

print("\n3. A FULL QUEUE drops instead of blocking")
m = fresh("https://127.0.0.1:9", "fake-key")   # worker NOT started: nothing drains
for _ in range(20050):
    m.mirror_court_status(7, {"current_item": 1}, "2026-09-28", "2026-09-28T00:00:00Z")
s = m.stats()
check(f"queue capped at 20,000 (backlog {s['backlog']})", s["backlog"] <= 20000)
check(f"overflow dropped, not blocked ({s['dropped']} dropped)", s["dropped"] >= 50)

print("\n4. BAD DATA is discarded before it can poison a batch")
m = fresh("https://example.invalid", "k")
m.mirror_cause_list({"case_number": "X-1-2026", "case_type": "X"})   # missing required
check("incomplete cause-list row is dropped", m.stats()["queued"] == 0)
m.mirror_cause_list({"case_number": "X-1-2026", "case_type": "X", "case_no": "1.0",
                     "case_year": "2026.0", "court_number": "7.0", "item_number": "1.0",
                     "list_date": "2026-09-28", "list_type": "COMPLETE"})
check("complete row is accepted", m.stats()["queued"] == 1)

print("\n5. FLOATS are coerced to whole numbers on the way through")
m = fresh("https://example.invalid", "k")
m.mirror_cause_list({"case_number": "CWP-1394-2024", "case_type": "CWP", "case_no": 1394.0,
                     "case_year": 2024.0, "court_number": 5.0, "item_number": 282.0,
                     "list_date": "2026-09-25", "list_type": "ORDINARY"})
tbl, row = m._q.get_nowait()
check("case_no 1394.0 -> 1394", row["case_no"] == 1394 and isinstance(row["case_no"], int))
check("court_number 5.0 -> 5", row["court_number"] == 5 and isinstance(row["court_number"], int))
check("item_number 282.0 -> 282", row["item_number"] == 282)


print("\n6. TRACKED CASES mirror unclaimed, and only when something changed")
m = fresh("https://example.invalid", "k")
CASE = {"id": "abc123", "user_id": "u-999", "case_type": "CWP", "case_number": "8590",
        "case_year": 2020, "court_number": 12, "item_number": 34,
        "case_date": "2026-10-13", "status": "pending", "notified_thresholds": [15.0],
        "fetched_data_json": '{"a": 1}', "notifications_enabled": True}
m.mirror_tracked_case(CASE)
tbl, row = m._q.get_nowait()
check("goes to tracked_cases", tbl == "tracked_cases", tbl)
check("carries the Base44 user id as legacy_user_id", row["legacy_user_id"] == "u-999")
check("NEVER sends user_id (would unclaim a signed-in advocate's cases)",
      "user_id" not in row, str(sorted(row)[:4]))
check("fetched_data_json parsed from string to object",
      row["fetched_data_json"] == {"a": 1}, repr(row["fetched_data_json"]))
check("notified_thresholds coerced to ints", row["notified_thresholds"] == [15])

m.mirror_tracked_case(CASE)
check("an unchanged case is NOT re-sent", m._q.qsize() == 0, f"{m._q.qsize()} queued")

moved = dict(CASE, item_number=35)
m.mirror_tracked_case(moved)
check("a changed case IS re-sent", m._q.qsize() == 1)
_, row2 = m._q.get_nowait()
check("…with the new position", row2["item_number"] == 35)

print("\n7. TRACKED CASE edge cases cannot poison the batch")
m = fresh("https://example.invalid", "k")
m.mirror_tracked_case(dict(CASE, status="weird-value"))
_, row = m._q.get_nowait()
check("an unknown status becomes pending", row["status"] == "pending", row["status"])
m.mirror_tracked_case(dict(CASE, id="x2", fetched_data_json="not json{"))
_, row = m._q.get_nowait()
check("malformed fetched_data_json becomes null, not a guess",
      row["fetched_data_json"] is None)
m.mirror_tracked_case(dict(CASE, id=None))
check("a case with no id is skipped entirely", m._q.qsize() == 0)

m = fresh()
for _ in range(5000):
    m.mirror_tracked_case(CASE)
check("inert when disabled", m.stats()["queued"] == 0)

print(f"\n{ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
