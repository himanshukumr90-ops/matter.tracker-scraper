"""Tests for the board-fetch spacing.

The dangerous failure is not "we spaced too little". It is "we spaced so much
that the cycle grew", because a longer cycle means every court is read less
often -- a slower alert for everyone. So most of these test the budget.
"""
import os, sys, time, unittest.mock as mock
os.environ.setdefault("BASE44_APP_ID","x"); os.environ.setdefault("BASE44_API_KEY","x")
import scraper as S

ok=fail=0
def check(l,c,d=""):
    global ok,fail
    if c: ok+=1; print(f"  ✅ {l}")
    else: fail+=1; print(f"  ❌ {l}  {d}")

print("1. Planning the gap")
S._last_cycle_work = 5.0            # a healthy cycle after fix 4
for n, lo, hi in [(1,0,0), (2,1.4,1.5), (10,0.9,1.1), (18,0.5,0.6), (56,0.15,0.2)]:
    S._spacing_warned = False
    S._plan_board_spacing(n)
    g = S._board_spacing
    check(f"{n:>2} courts -> {g:.2f}s between fetches", lo <= g <= hi, f"got {g}")

print("\n2. The gap NEVER lengthens the cycle beyond the target")
for work in (5.0, 10.0, 14.0, 20.0, 40.0):
    S._spacing_warned = False
    S._plan_board_spacing(10)
    total = S._board_spacing * 10
    period = work + total + S.SCRAPE_INTERVAL
    S._last_cycle_work = work
    S._plan_board_spacing(10)
    total2 = S._board_spacing * 10
    check(f"work={work:>4.0f}s -> spends {total2:.1f}s on spacing",
          total2 <= max(3.0, S.BOARD_TARGET_CYCLE - S.SCRAPE_INTERVAL - work) + 0.01,
          f"spent {total2}")

print("\n3. An already-slow cycle collapses the spend to the floor, not more")
S._last_cycle_work = 60.0           # cycle is badly overrunning
S._spacing_warned = False
S._plan_board_spacing(10)
check(f"no headroom -> only {S._board_spacing*10:.1f}s total (the 3s minimum)",
      S._board_spacing*10 <= 3.01, f"{S._board_spacing*10}")

print("\n4. Too many courts is REPORTED, not silently useless")
S._last_cycle_work = 40.0
S._spacing_warned = False
with mock.patch("builtins.print") as p:
    S._plan_board_spacing(60)
    msgs = " ".join(str(c) for c in p.call_args_list)
check("warns when the gap is too tight to break a burst", "outgrown" in msgs)

print("\n5. The pacing actually waits — and never longer than the cap")
S._board_spacing = 0.25
S._last_board_fetch = 0.0
S._pace_board_fetch()                       # first call: no wait (stale marker)
t0=time.time(); S._pace_board_fetch(); first=time.time()-t0
t0=time.time(); S._pace_board_fetch(); second=time.time()-t0
check(f"consecutive fetches are ~0.25s apart (got {second:.2f}s)", 0.2 <= second <= 0.4)
S._board_spacing = 0.0
t0=time.time(); S._pace_board_fetch(); off=time.time()-t0
check(f"spacing 0 -> no wait at all ({off*1000:.0f} ms)", off < 0.05)

print("\n6. A long stall does not cause a catch-up sleep")
S._board_spacing = 1.0
S._last_board_fetch = time.time() - 600     # last fetch ten minutes ago
t0=time.time(); S._pace_board_fetch(); w=time.time()-t0
check(f"overdue fetch goes immediately ({w*1000:.0f} ms)", w < 0.05)

print("\n7. Each court is still read ONCE per cycle — spacing is a phase shift")
src=open("scraper.py").read()
i=src.index("def _board_status_for_case")
check("the per-cycle board cache is still consulted first",
      "if court_number not in board_cache:" in src[i:i+600])
check("SCRAPE_INTERVAL is untouched", S.SCRAPE_INTERVAL == 30)

print(f"\n{ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
