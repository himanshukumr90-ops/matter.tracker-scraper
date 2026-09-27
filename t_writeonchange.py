"""Tests for write-on-change.

The dangerous failure is not "we wrote too often" -- it is "we stopped writing
and the app told users the board was stale". The heartbeat tests below are the
ones that matter.
"""
import datetime, os, sys, unittest.mock as mock
os.environ.setdefault("BASE44_APP_ID","x"); os.environ.setdefault("BASE44_API_KEY","x")
import scraper as S

ok=fail=0
def check(l,c,d=""):
    global ok,fail
    if c: ok+=1; print(f"  ✅ {l}")
    else: fail+=1; print(f"  ❌ {l}  {d}")

print("1. The change check itself")
S._court_last_write.clear()
check("never seen before -> write", S._court_write_needed(7, ("a",), now_epoch=1000))
S._court_write_done(7, ("a",), now_epoch=1000)
check("same values, 1s later -> skip", not S._court_write_needed(7, ("a",), now_epoch=1001))
check("CHANGED values -> write immediately", S._court_write_needed(7, ("b",), now_epoch=1001))
check("same values, 119s -> still skip", not S._court_write_needed(7, ("a",), now_epoch=1119))
check("same values, 120s -> HEARTBEAT writes", S._court_write_needed(7, ("a",), now_epoch=1120))
check("another court is tracked separately", S._court_write_needed(8, ("a",), now_epoch=1001))

print("\n2. A court moving item by item is never skipped")
S._court_last_write.clear()
t=2000; wrote=0
for item in range(101, 121):          # court advances every cycle
    if S._court_write_needed(5, (item,), now_epoch=t):
        wrote+=1; S._court_write_done(5, (item,), now_epoch=t)
    t+=30
check(f"20 item changes -> 20 writes (got {wrote})", wrote==20)

print("\n3. A STATIONARY court still beats the 5-minute staleness warning")
S._court_last_write.clear()
t=3000; writes=[]
for _ in range(40):                   # 40 cycles x 30s = 20 minutes, no change
    if S._court_write_needed(5, ("same",), now_epoch=t):
        writes.append(t); S._court_write_done(5, ("same",), now_epoch=t)
    t+=30
gaps=[writes[i]-writes[i-1] for i in range(1,len(writes))]
check(f"{len(writes)} writes in 20 min instead of 40", len(writes)<=11)
check(f"longest silence {max(gaps)}s — under the app's 300s stale threshold",
      max(gaps) < 300, f"gaps={gaps}")

print("\n4. End to end through update_court_status")
S._court_last_write.clear()
court_data={7:{"current_item":118,"is_passover":False,"passover_current":None,
               "passover_total":None,"last_regular_item":118,"last_queue_item":118}}
existing={7:"rec7"}
calls=[]
class R:  status_code=200; text=""
with mock.patch.object(S.requests,"put",side_effect=lambda *a,**k:(calls.append(a),R())[1]), \
     mock.patch.object(S.requests,"post",side_effect=lambda *a,**k:(calls.append(a),R())[1]), \
     mock.patch.object(S.supabase_mirror,"mirror_court_status",lambda *a,**k:None):
    S.update_court_status(court_data, existing);  first=len(calls)
    S.update_court_status(court_data, existing);  second=len(calls)-first
    court_data[7]["current_item"]=119
    S.update_court_status(court_data, existing);  third=len(calls)-first-second
check(f"first cycle writes (got {first})", first==1)
check(f"identical second cycle writes nothing (got {second})", second==0)
check(f"item moves 118->119, writes again (got {third})", third==1)

print("\n5. A failed write must NOT be recorded as done")
S._court_last_write.clear()
class Bad: status_code=429; text="Rate limit exceeded"
with mock.patch.object(S.requests,"put",return_value=Bad()), \
     mock.patch.object(S.supabase_mirror,"mirror_court_status",lambda *a,**k:None):
    S.update_court_status(court_data, existing)
check("a refused write is retried next cycle, not skipped",
      S._court_write_needed(7, (119,False,None,None,118,118,
                                datetime.date.today().isoformat(),True)))

print("\n6. The safety rail on a failed read is untouched")
with mock.patch.object(S.requests,"put") as put:
    S.update_court_status(court_data, None)
check("existing_records=None still writes nothing", put.call_count==0)

print(f"\n{ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
