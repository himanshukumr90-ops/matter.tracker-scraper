"""Tests for the paginated TrackedCase read.

The failure this guards against is SILENT: cases past the row cap simply
never get evaluated, with no error anywhere. So the tests that matter most
are (a) every row is returned however many pages it takes, and (b) a failure
mid-way is reported as a failure and NEVER as "that was all of them".
"""
import os, sys, unittest.mock as mock
os.environ.setdefault("BASE44_APP_ID","x"); os.environ.setdefault("BASE44_API_KEY","x")
import scraper as S

ok=fail=0
def check(l,c,d=""):
    global ok,fail
    if c: ok+=1; print(f"  ✅ {l}")
    else: fail+=1; print(f"  ❌ {l}  {d}")

def pages(total, fail_at_skip=None, bad_status=429):
    """A fake Base44 holding `total` rows, optionally failing at one page."""
    def go(url, params=None, **kw):
        skip = (params or {}).get("skip", 0)
        limit = (params or {}).get("limit", 500)
        class R:
            status_code = 200
            @staticmethod
            def json(): return [{"id": f"c{i}"} for i in range(skip, min(skip+limit, total))]
        if fail_at_skip is not None and skip == fail_at_skip:
            class Bad:
                status_code = bad_status
                text = "Rate limit exceeded"
                @staticmethod
                def json(): return []
            return Bad()
        return R()
    return go

print("1. Every row comes back, however many pages")
for total, want_pages in [(0,1), (1,1), (499,1), (500,2), (501,2), (1200,3), (5200,11)]:
    with mock.patch.object(S.requests,"get",side_effect=pages(total)), \
         mock.patch.object(S.time,"sleep",lambda *a: None):
        rows = S._fetch_tracked_cases()
    check(f"{total:>5} rows -> got {len(rows) if rows is not None else 'None'}",
          rows is not None and len(rows)==total, f"{rows and len(rows)}")

print("\n2. THE SILENT KILLER: a mid-pagination failure must NOT look like the end")
with mock.patch.object(S.requests,"get",side_effect=pages(3000, fail_at_skip=1000)), \
     mock.patch.object(S.time,"sleep",lambda *a: None):
    rows = S._fetch_tracked_cases()
check("429 on page 3 of 6 -> None, not a partial list", rows is None,
      f"got {rows if rows is None else len(rows)} rows")

with mock.patch.object(S.requests,"get",side_effect=pages(3000, fail_at_skip=0)), \
     mock.patch.object(S.time,"sleep",lambda *a: None):
    rows = S._fetch_tracked_cases()
check("429 on the very first page -> None", rows is None)

def boom(*a, **k): raise S.requests.RequestException("connection reset")
with mock.patch.object(S.requests,"get",side_effect=boom):
    check("a network exception -> None", S._fetch_tracked_cases() is None)

print("\n3. Each caller handles None WITHOUT doing damage")
with mock.patch.object(S,"_fetch_tracked_cases",lambda *a,**k: None):
    S._all_tracked_cases = [{"id":"keepme"}]
    check("alert path returns no cases for this cycle", S.get_tracked_cases() == [])
    check("  and does NOT clobber the good list it already had",
          S._all_tracked_cases == [{"id":"keepme"}])

    with mock.patch.object(S.requests,"put") as put:
        S.reset_daily_flags()
        check("daily reset writes nothing on a failed read", put.call_count == 0)

    with mock.patch.object(S.requests,"put") as put:
        S.sync_tracked_cases_from_cause_list()
        check("the 10-minute sync writes nothing", put.call_count == 0)

    with mock.patch.object(S.requests,"put") as put, \
         mock.patch.object(S.requests,"get") as get:
        S.refresh_tracked_cases_from_website()
        check("the website refresh writes nothing", put.call_count == 0)

print("\n4. A healthy read still behaves exactly as before")
import datetime
today = datetime.date.today().isoformat()
good = [{"id":"a","case_date":today,"notifications_enabled":True,"court_number":7,"item_number":101},
        {"id":"b","case_date":today,"notifications_enabled":False,"court_number":7,"item_number":102},
        {"id":"c","case_date":"2020-01-01","notifications_enabled":True,"court_number":7,"item_number":103},
        {"id":"d","case_date":today,"notifications_enabled":True,"court_number":None,"item_number":None}]
with mock.patch.object(S,"_fetch_tracked_cases",lambda *a,**k: good):
    act = S.get_tracked_cases()
check(f"only today's alertable, placed cases are returned (got {[c['id'] for c in act]})",
      [c["id"] for c in act] == ["a"])
check("but _all_tracked_cases keeps ALL of them for the fill step",
      len(S._all_tracked_cases) == 4)

print(f"\n{ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
